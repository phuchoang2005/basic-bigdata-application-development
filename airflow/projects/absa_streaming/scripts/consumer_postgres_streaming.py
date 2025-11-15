# SE363 – Phát triển ứng dụng trên nền tảng dữ liệu lớn
# Khoa Công nghệ Phần mềm – Trường Đại học Công nghệ Thông tin, ĐHQG-HCM
# HopDT – Faculty of Software Engineering, University of Information Technology (FSE-UIT)
#
# ======================================
# consumer_postgres_streaming.py (Phiên bản CNN siêu nhẹ)
# ĐÃ TÁI CẤU TRÚC (REFACTORED) THÀNH CÁC HÀM
# ======================================

import json
import os
import re
import sys
import time
import traceback  # Thêm thư viện để in lỗi chi tiết

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as tF
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.streaming import StreamingQuery

# =============================================================================
# === 1. CẤU HÌNH TOÀN CỤC (CONSTANTS) ===
# =============================================================================
ASPECTS = [
    "Price",
    "Shipping",
    "Outlook",
    "Quality",
    "Size",
    "Shop_Service",
    "General",
    "Others",
]
SENTIMENTS = ["POS", "NEU", "NEG"]

# Đường dẫn (phải khớp với volumes trong DockerOperator)
CHECKPOINT_PATH = "/opt/airflow/checkpoints/absa_streaming_checkpoint"
VOCAB_PATH = "/opt/airflow/models/vocab.json"
MODEL_PATH = "/opt/airflow/models/cnn_best.pth"

# Cấu hình model
MAX_LEN = 64
EMBED_DIM = 100
DEVICE = "cpu"

# Biến global cho model (dùng trong UDF)
_model, _vocab = None, None


# =============================================================================
# === 2. ĐỊNH NGHĨA MODEL VÀ HÀM HỖ TRỢ ===
# (Phải ở top-level để Spark có thể "thấy" và "serialize")
# =============================================================================


class CNN_ABSAModel(nn.Module):
    """Định nghĩa kiến trúc mô hình CNN cho ABSA."""

    def __init__(
        self,
        vocab_size,
        embed_dim,
        num_aspects,
        num_sentiments=3,
        num_filters=50,
        kernel_sizes=[3, 4, 5],
    ):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim, padding_idx=0)
        self.convs = nn.ModuleList(
            [
                nn.Conv1d(
                    in_channels=embed_dim, out_channels=num_filters, kernel_size=k
                )
                for k in kernel_sizes
            ]
        )
        total_filters = num_filters * len(kernel_sizes)
        self.dropout = nn.Dropout(0.1)
        self.head_s = nn.Linear(total_filters, num_aspects * num_sentiments)

    def forward(self, input_ids):
        embedded = self.embedding(input_ids).permute(0, 2, 1)
        conved = [F.relu(conv(embedded)) for conv in self.convs]
        pooled = [F.max_pool1d(conv, conv.shape[2]).squeeze(2) for conv in conved]
        cat = self.dropout(torch.cat(pooled, dim=1))
        logits_s = self.head_s(cat).view(-1, len(ASPECTS), 3)
        return logits_s


def text_to_indices(text: str, vocab: dict, max_len: int) -> list:
    """Helper: Chuyển text thô sang list các ID từ vựng."""
    tokens = re.findall(r"\w+", text.lower())
    indices = [vocab.get(token, vocab.get("<unk>", 1)) for token in tokens]
    if len(indices) < max_len:
        indices += [vocab.get("<pad>", 0)] * (max_len - len(indices))
    else:
        indices = indices[:max_len]
    return indices


@pandas_udf(T.MapType(T.StringType(), T.StringType()))
def absa_cnn_infer_and_decode_udf(texts: pd.Series) -> pd.Series:
    """
    Pandas UDF: Tải model (một lần) và thực hiện dự đoán (inference)
    cho từng batch dữ liệu.
    """
    global _model, _vocab
    if _model is None:
        try:
            # 1. Tải vocab
            print("UDF: Đang tải vocab...")
            with open(VOCAB_PATH, "r", encoding="utf-8") as f:
                _vocab = json.load(f)
            print(f"UDF: Tải vocab thành công ({len(_vocab)} từ).")

            # 2. Tải model CNN
            print("UDF: Đang tải model CNN...")
            _model = CNN_ABSAModel(
                vocab_size=len(_vocab), embed_dim=EMBED_DIM, num_aspects=len(ASPECTS)
            )
            _model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
            _model.to(DEVICE).eval()
            print("UDF: Tải model CNN thành công.")

        except Exception as e:
            print(
                f"LỖI NGHIÊM TRỌNG TRONG UDF: Không thể tải model/vocab. Lỗi: {e}",
                file=sys.stderr,
            )
            return pd.Series([{} for _ in range(len(texts))])

    # 3. Tokenize toàn bộ batch
    all_indices = [text_to_indices(text, _vocab, MAX_LEN) for text in texts]
    input_tensor = torch.tensor(all_indices, dtype=torch.long).to(DEVICE)

    # 4. Infer toàn bộ batch
    with torch.no_grad():
        logits_s = _model(input_tensor)
        sent_indices = torch.argmax(logits_s, dim=-1).cpu().numpy()

    # 5. Decode kết quả
    results = []
    sent_map = np.array(SENTIMENTS)
    for batch_indices in sent_indices:
        sent_labels = sent_map[batch_indices]
        res_map = {asp: sent for asp, sent in zip(ASPECTS, sent_labels)}
        results.append(res_map)

    return pd.Series(results)


# =============================================================================
# === 3. ĐỊNH NGHĨA HÀM GHI (SINK) ===
# (Hàm này được gọi bởi foreachBatch)
# =============================================================================


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    """
    Ghi một micro-batch DataFrame vào bảng PostgreSQL.
    """
    sys.stdout.reconfigure(encoding="utf-8")

    # Tối ưu: cache batch này để tránh tính toán lại
    batch_df.persist()
    total_rows = 0

    try:
        total_rows = batch_df.count()
        if total_rows == 0:
            print(f"[Batch {batch_id}] ⚠️ Không có dữ liệu mới.")
            return

        # Log preview
        preview_pd = batch_df.select("review", *ASPECTS).limit(5).toPandas()
        preview_dict = preview_pd.to_dict(orient="records")
        print(
            f"\n[Batch {batch_id}] Nhận {total_rows} dòng, hiển thị {len(preview_dict)} dòng đầu (CNN):"
        )
        print(json.dumps(preview_dict, ensure_ascii=False, indent=2))

        # Ghi vào DB
        (
            batch_df.select(F.col("review").alias("ReviewText"), *ASPECTS)
            .write.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/airflow")
            .option("dbtable", "absa_results")
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .option("charset", "utf8")
            .mode("append")
            .save()
        )
        print(f"[Batch {batch_id}] ✅ Ghi PostgreSQL thành công ({total_rows} dòng).")

    except Exception as e:
        print(
            f"[Batch {batch_id}] ⚠️ Không thể ghi vào PostgreSQL. Lỗi: {str(e)}",
            file=sys.stderr,
        )
        traceback.print_exc()  # In chi tiết lỗi
    finally:
        # Giải phóng cache
        batch_df.unpersist()


# =============================================================================
# === 4. CÁC HÀM XỬ LÝ CHÍNH (PIPELINE) ===
# =============================================================================


def create_spark_session() -> SparkSession:
    """Khởi tạo và trả về một SparkSession."""
    print("Pipeline: Đang tạo Spark session...")
    spark = (
        SparkSession.builder.appName("Kafka_ABSA_Postgres_CNN")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2",
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.2",
                    "org.apache.kafka:kafka-clients:3.5.1",
                    "org.apache.commons:commons-pool2:2.12.0",
                    "org.postgresql:postgresql:42.6.0",
                ]
            ),
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Pipeline: Tạo Spark session thành công.")
    return spark


def define_kafka_source(spark: SparkSession) -> DataFrame:
    """Định nghĩa nguồn streaming từ Kafka và trích xuất text."""
    print("Pipeline: Đang định nghĩa nguồn Kafka...")
    df_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "absa-reviews")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

    review_schema = T.StructType(
        [T.StructField("id", T.StringType()), T.StructField("review", T.StringType())]
    )
    df_json = df_stream.select(
        from_json(col("value").cast("string"), review_schema).alias("data")
    )
    df_text = df_json.select(F.col("data.review").alias("review"))
    print("Pipeline: Định nghĩa nguồn Kafka thành công.")
    return df_text


def process_stream(df_in: DataFrame) -> DataFrame:
    """Áp dụng UDF và chuyển đổi DataFrame."""
    print("Pipeline: Đang áp dụng logic xử lý (UDF)...")
    df_pred = df_in.withColumn(
        "aspect_sentiments", absa_cnn_infer_and_decode_udf(F.col("review"))
    )

    df_final = df_pred.select("review", "aspect_sentiments")
    for asp in ASPECTS:
        df_final = df_final.withColumn(asp, F.col("aspect_sentiments").getItem(asp))

    print("Pipeline: Áp dụng logic xử lý thành công.")
    return df_final


def start_stream_sink(df_final: DataFrame) -> StreamingQuery:
    """Bắt đầu query streaming và ghi ra sink (Postgres)."""
    print("Pipeline: Đang bắt đầu streaming query...")
    query = (
        df_final.writeStream.foreachBatch(write_to_postgres)
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    print(
        "🚀 Streaming job (CNN Siêu Nhẹ) started — đang lắng nghe dữ liệu từ Kafka..."
    )
    return query


# =============================================================================
# === 5. THỰC THI CHƯƠNG TRÌNH ===
# =============================================================================


def main():
    """Hàm main điều phối toàn bộ pipeline."""
    try:
        spark = create_spark_session()
        df_raw = define_kafka_source(spark)
        df_final = process_stream(df_raw)
        query = start_stream_sink(df_final)
        query.awaitTermination()
    except Exception as e:
        print(f"FATAL ERROR: Pipeline đã bị dừng đột ngột. Lỗi: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
