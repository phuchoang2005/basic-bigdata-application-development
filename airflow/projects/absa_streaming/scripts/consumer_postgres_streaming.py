# SE363 – Phát triển ứng dụng trên nền tảng dữ liệu lớn
# Khoa Công nghệ Phần mềm – Trường Đại học Công nghệ Thông tin, ĐHQG-HCM
# HopDT – Faculty of Software Engineering, University of Information Technology (FSE-UIT)

# consumer_postgres_streaming.py
# ======================================
# Consumer đọc dữ liệu từ Kafka topic "absa-reviews"
# → chạy inference mô hình ABSA (.pt)
# → ghi kết quả vào PostgreSQL
# → Airflow sẽ giám sát và khởi động lại khi job bị dừng.
# SE363 – Phát triển ứng dụng trên nền tảng dữ liệu lớn
# Khoa Công nghệ Phần mềm – Trường Đại học Công nghệ Thông tin, ĐHQG-HCM
# HopDT – Faculty of Software Engineering, University of Information Technology (FSE-UIT)

# consumer_postgres_streaming.py (ĐÃ TỐI ƯU)
# ======================================
# Consumer đọc dữ liệu từ Kafka topic "absa-reviews"
# → chạy inference mô hình ABSA (vectorized)
# → ghi kết quả vào PostgreSQL (sử dụng cache)
# consumer_postgres_streaming.py (Phiên bản CNN siêu nhẹ)
# ======================================
# Bỏ hoàn toàn 'transformers', chỉ dùng 'torch' và 'pyspark'.
# Nhanh hơn, nhẹ hơn, phù hợp cho máy yếu.

import json
import os
import re
import sys
import time

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as tF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, from_json, pandas_udf

# === 1. Spark session (Giữ nguyên) ===
spark = (
    SparkSession.builder.appName("Kafka_ABSA_Postgres_CNN")
    .config(
        "spark.sql.streaming.checkpointLocation",
        "/opt/airflow/checkpoints/absa_streaming_checkpoint",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# === 2. Đọc dữ liệu streaming từ Kafka (Giữ nguyên) ===
df_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "absa-reviews")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100)  # Xử lý 100 dòng mỗi 5s
    .load()
)

review_schema = T.StructType(
    [T.StructField("id", T.StringType()), T.StructField("review", T.StringType())]
)
df_json = df_stream.select(
    from_json(col("value").cast("string"), review_schema).alias("data")
)
df_text = df_json.select(F.col("data.review").alias("review"))  # Đặt alias là "review"

# === 3. Định nghĩa mô hình CNN (THAY THẾ HOÀN TOÀN) ===
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

# Đường dẫn đến model CNN và vocab của bạn
VOCAB_PATH = "/opt/models/vocab.json"
MODEL_PATH = "/opt/models/cnn_best.pth"

MAX_LEN = 64  # Max length khi huấn luyện CNN
EMBED_DIM = 100  # Ví dụ
DEVICE = "cpu"  # Ép chạy CPU cho nhẹ

_model, _vocab = None, None


# Định nghĩa một mô hình CNN (Kiểu "Kim CNN" 2014)
class CNN_ABSAModel(nn.Module):
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
        # input_ids shape: [batch_size, max_len]
        embedded = self.embedding(input_ids)
        # embedded shape: [batch_size, max_len, embed_dim]

        embedded = embedded.permute(0, 2, 1)
        # embedded shape: [batch_size, embed_dim, max_len] (cho Conv1d)

        conved = [F.relu(conv(embedded)) for conv in self.convs]
        # conved[i] shape: [batch_size, num_filters, max_len - k + 1]

        pooled = [F.max_pool1d(conv, conv.shape[2]).squeeze(2) for conv in conved]
        # pooled[i] shape: [batch_size, num_filters] (Global Max Pooling)

        cat = self.dropout(torch.cat(pooled, dim=1))
        # cat shape: [batch_size, num_filters * len(kernel_sizes)]

        logits_s = self.head_s(cat).view(-1, len(ASPECTS), 3)
        return logits_s


# Helper: Hàm tokenizer đơn giản cho CNN (thay thế AutoTokenizer)
def text_to_indices(text, vocab, max_len):
    tokens = re.findall(r"\w+", text.lower())  # Tách từ đơn giản
    indices = [vocab.get(token, vocab.get("<unk>", 1)) for token in tokens]
    # Padding
    if len(indices) < max_len:
        indices += [vocab.get("<pad>", 0)] * (max_len - len(indices))
    else:
        indices = indices[:max_len]
    return indices


# UDF mới: Dùng CNN, đã vector hóa và gộp logic
@pandas_udf(T.MapType(T.StringType(), T.StringType()))
def absa_cnn_infer_and_decode_udf(texts: pd.Series) -> pd.Series:
    global _model, _vocab
    if _model is None:
        # 1. Tải vocab (file JSON)
        try:
            with open(VOCAB_PATH, "r", encoding="utf-8") as f:
                _vocab = json.load(f)
        except Exception as e:
            # Ghi lỗi nghiêm trọng nếu không tải được vocab
            print(f"LỖI NGHIÊM TRỌNG: Không thể tải vocab từ {VOCAB_PATH}. Lỗi: {e}")
            # Trả về kết quả rỗng cho batch này
            return pd.Series([{} for _ in range(len(texts))])

        # 2. Tải model CNN
        # Phải khớp với tham số khi huấn luyện
        _model = CNN_ABSAModel(
            vocab_size=len(_vocab), embed_dim=EMBED_DIM, num_aspects=len(ASPECTS)
        )
        _model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
        _model.to(DEVICE).eval()

    # 3. Tokenize toàn bộ batch (vectorized)
    # (Việc tokenize không thể vector hóa hoàn toàn như transformers,
    # nhưng list comprehension vẫn nhanh)
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


# === 4. Áp dụng UDF và tạo cột (Giữ nguyên) ===
df_pred = df_text.withColumn(
    "aspect_sentiments", absa_cnn_infer_and_decode_udf(F.col("review"))
)

df_final = df_pred.select("review", "aspect_sentiments")
for asp in ASPECTS:
    df_final = df_final.withColumn(asp, F.col("aspect_sentiments").getItem(asp))


# === 5. Ghi kết quả vào PostgreSQL (Giữ nguyên) ===
def write_to_postgres(batch_df, batch_id):
    sys.stdout.reconfigure(encoding="utf-8")
    batch_df.persist()  # Giữ nguyên tối ưu cache

    total_rows = 0
    try:
        total_rows = batch_df.count()
        if total_rows == 0:
            print(f"[Batch {batch_id}] ⚠️ Không có dữ liệu mới.")
            return

        # Log preview
        preview = (
            batch_df.select("review", *ASPECTS)
            .limit(5)
            .toPandas()
            .to_dict(orient="records")
        )
        print(
            f"\n[Batch {batch_id}] Nhận {total_rows} dòng, hiển thị 5 dòng đầu (CNN):"
        )
        print(json.dumps(preview, ensure_ascii=False, indent=2))

        # Ghi vào DB
        (
            batch_df.select(
                F.col("review").alias("ReviewText"), *ASPECTS
            )  # Đổi tên cột
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
        print(f"[Batch {batch_id}] ⚠️ Không thể ghi vào PostgreSQL. Lỗi: {str(e)}")
    finally:
        batch_df.unpersist()  # Giữ nguyên tối ưu cache


# === 6. Bắt đầu stream (Giữ nguyên) ===
query = (
    df_final.writeStream.foreachBatch(write_to_postgres)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print("🚀 Streaming job (CNN Siêu Nhẹ) started — đang lắng nghe dữ liệu từ Kafka...")
query.awaitTermination()
