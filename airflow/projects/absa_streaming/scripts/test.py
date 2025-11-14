import sys

from pyspark.sql import SparkSession

# Tên class Java chính xác đang bị báo lỗi
CLASS_TO_FIND = "org.apache.spark.kafka010.KafkaConfigUpdater"

print("====================================================")
print("===   BÀI KIỂM TRA TẢI FILE JAR KAFKA CỦA SPARK   ===")
print("====================================================")

try:
    print("\n[BƯỚC 1] Khởi tạo SparkSession...")
    spark = SparkSession.builder.appName("CheckJarAvailability").getOrCreate()
    print("[BƯỚC 1] Khởi tạo SparkSession thành công.")

    print(f"\n[BƯỚC 2] Lấy kết nối tới máy ảo Java (JVM)...")
    jvm = spark.sparkContext._jvm
    print(f"[BƯỚC 2] Lấy kết nối JVM thành công.")

    print(f"\n[BƯỚC 3] Thử tải class: {CLASS_TO_FIND}")
    jvm.java.lang.Class.forName(CLASS_TO_FIND)

    # Nếu dòng trên chạy thành công, nó sẽ không ném ra lỗi
    print("\n====================================================")
    print(f"✅ THÀNH CÔNG! Class đã được tìm thấy và tải thành công.")
    print("Điều này có nghĩa là cờ --jars của bạn đã hoạt động.")
    print("====================================================")

except Exception as e:
    # Nếu class không tìm thấy, nó sẽ ném ra lỗi (thường là Py4JError)
    print("\n====================================================")
    print(f"❌ THẤT BẠI! Class không được tìm thấy.")
    print("Điều này 100% xác nhận cờ --jars của bạn BỊ SAI hoặc BỊ THIẾU.")
    print("\nChi tiết lỗi từ Java:")
    print(e)
    print("====================================================")

    # Thoát với mã lỗi để Airflow biết tác vụ thất bại
    sys.exit(1)

finally:
    if "spark" in locals():
        spark.stop()
    print("Bài kiểm tra kết thúc.")
