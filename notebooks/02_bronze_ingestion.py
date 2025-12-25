# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 02 - Bronze Layer Ingestion
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Ingest raw CSV data into Bronze Delta tables using Auto Loader
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Bronze Layer Characteristics:
# MAGIC - Raw data with minimal transformation
# MAGIC - Schema inference and evolution support
# MAGIC - Metadata columns (ingestion timestamp, source file, batch ID)
# MAGIC - Incremental loading with Auto Loader
# MAGIC - Full audit trail

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, expr, col
from pyspark.sql.types import *
import uuid
from datetime import datetime

# Khởi tạo Spark
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ĐỊNH NGHĨA BIẾN Ở ĐẦU NOTEBOOK
CATALOG_NAME = "brazilian_ecommerce"
BRONZE_SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/source_data"

# Cell này phải được chạy đầu tiên
class BronzeConfig:
    RAW_PATH = f"{VOLUME_PATH}/raw"
    CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/bronze"
    SCHEMA_PATH = f"{VOLUME_PATH}/_schemas/bronze"
    
    # Mapping tên file CSV sang tên bảng đích
    SOURCE_TABLES = {
        "customers": "olist_customers_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "category_translation": "product_category_name_translation.csv"
    }

print(f"✅ Đang cấu hình nạp dữ liệu cho Catalog: {CATALOG_NAME}")

# COMMAND ----------

# DBTITLE 1,Utility Functions
# 2. Utility Functions
def add_bronze_metadata(df):
    """Thêm các cột metadata chuyên nghiệp cho lớp Bronze."""
    return df \
        .withColumn("_bronze_ingestion_ts", current_timestamp()) \
        .withColumn("_bronze_batch_id", lit(str(uuid.uuid4()))) \
        .withColumn("_bronze_source_file", col("_metadata.file_path")) \
        .withColumn("_bronze_row_id", expr("uuid()"))

def log_to_audit(table_name, status, records=0, error=None):
    """Ghi log vào bảng audit đã tạo ở Notebook 02."""
    try:
        audit_data = [(str(uuid.uuid4()), table_name, "INGESTION", status, records, datetime.now())]
        audit_df = spark.createDataFrame(audit_data, ["event_id", "table_name", "operation", "status", "records_processed", "timestamp"])
        audit_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG_NAME}.bronze._pipeline_audit")
    except:
        print(f"  ⚠️ Cảnh báo: Không thể ghi log cho {table_name}")

# COMMAND ----------

# DBTITLE 1,Auto Loader Ingestion Engine
def ingest_to_bronze(table_name, file_name):
    """Sử dụng Auto Loader để nạp dữ liệu từ Volume vào Delta Table."""
    print(f"🚀 Processing: {table_name}...")
    
    # ĐƯỜNG DẪN THƯ MỤC GỐC (Directory)
    source_directory = BronzeConfig.RAW_PATH 
    
    target_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"
    checkpoint_dir = f"{BronzeConfig.CHECKPOINT_PATH}/{table_name}"
    schema_dir = f"{BronzeConfig.SCHEMA_PATH}/{table_name}"
    
    try:
        # Sử dụng readStream với cloudFiles (Auto Loader)
        df_stream = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_dir)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            # SỬ DỤNG pathGlobFilter ĐỂ CHỌN FILE CỤ THỂ TRONG THƯ MỤC
            .option("pathGlobFilter", file_name) 
            .load(source_directory)) # Truyền THƯ MỤC vào đây
        
        # Thêm metadata và ghi xuống Delta Table
        query = (add_bronze_metadata(df_stream).writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target_table))
        
        query.awaitTermination()
        
        # Đếm số lượng record (Sử dụng spark.table để lấy số liệu mới nhất)
        count = spark.table(target_table).count()
        log_to_audit(table_name, "SUCCESS", count)
        print(f"  ✅ SUCCESS: {count:,} records nạp vào {target_table}")
        
    except Exception as e:
        print(f"  ❌ FAILED: {str(e)}")
        log_to_audit(table_name, "FAILED", error=str(e))

# COMMAND ----------

# DBTITLE 1,Thực thi nạp toàn bộ các bảng
# 4. Thực thi nạp toàn bộ các bảng
print("🏁 Bắt đầu tiến trình nạp dữ liệu lớp Bronze...")

for target_table, csv_file in BronzeConfig.SOURCE_TABLES.items():
    ingest_to_bronze(target_table, csv_file)

# COMMAND ----------

# 5. Tổng kết & Kiểm tra
print(f"\n{'='*60}")
print("📊 TỔNG KẾT LỚP BRONZE")
print(f"{'='*60}")

for table in BronzeConfig.SOURCE_TABLES.keys():
    full_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table}"
    try:
        cnt = spark.table(full_name).count()
        print(f"  ✅ {full_name:<40} | Records: {cnt:,}")
    except:
        print(f"  ❌ {full_name:<40} | Lỗi: Không tìm thấy bảng")

print(f"\n⏭️ Bước tiếp theo: Chạy notebook 04_silver_transformation.py")