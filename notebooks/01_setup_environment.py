# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ 01 - Setup Databricks Environment
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Configure Databricks environment for the data pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Setup Tasks:
# MAGIC 1. Create Unity Catalog (if available) or Hive Metastore schemas
# MAGIC 2. Setup Bronze, Silver, Gold, Business databases/schemas
# MAGIC 3. Configure Delta Lake settings
# MAGIC 4. Set up checkpoint directories
# MAGIC 5. Create utility functions
# MAGIC 6. Validate environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
from datetime import datetime
import json

# 1. Khởi tạo Spark (Bỏ sparkContext để tương thích Serverless)
spark = SparkSession.builder.getOrCreate()

print("✅ Spark Session initialized")
print(f"   Spark Version: {spark.version}")

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# 2. # Cấu hình định danh dự án
CATALOG_NAME = "brazilian_ecommerce"
SCHEMAS = ["bronze", "silver", "gold", "business", "ml_models"]

# Cấu hình lưu trữ trong Unity Catalog Volume
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/bronze/source_data"

class Config:
    RAW_PATH = f"{VOLUME_PATH}/raw"
    CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"
    SCHEMA_PATH = f"{VOLUME_PATH}/_schemas"
    
    @staticmethod
    def table(schema, table_name):
        return f"{CATALOG_NAME}.{schema}.{table_name}"

print(f"✅ Đang khởi tạo môi trường cho Catalog: {CATALOG_NAME}")

# COMMAND ----------

# 3. Khởi tạo Cấu trúc Unity Catalog
# Tạo Catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

# Tạo các Schemas (Bronze, Silver, Gold, Business, ML)
for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")
    print(f"  ✅ Đã tạo Schema: {CATALOG_NAME}.{schema}")

# Tạo Volume để lưu trữ dữ liệu vật lý (File CSV, Checkpoints)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.bronze.source_data")
print(f"  ✅ Đã tạo Volume tại: {VOLUME_PATH}")

# COMMAND ----------

# 4. Khởi tạo Cấu trúc Thư mục (Volumes)
print("📁 Đang cấu trúc thư mục trong Volume...")

paths_to_create = [
    Config.RAW_PATH, 
    Config.CHECKPOINT_PATH, 
    Config.SCHEMA_PATH,
    f"{Config.CHECKPOINT_PATH}/bronze",
    f"{Config.CHECKPOINT_PATH}/silver"
]

for path in paths_to_create:
    try:
        dbutils.fs.mkdirs(path)
        print(f"  ✅ Đã tạo thư mục: {path}")
    except Exception as e:
        print(f"  ⚠️ Cảnh báo khi tạo {path}: {str(e)}")

# COMMAND ----------

# 5. Cấu hình Spark (Fail-safe cho Serverless)
# Các tham số cấu hình Spark (Sử dụng try-except để bỏ qua nếu bị Serverless khóa)
spark_conf = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "auto",
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.sql.streaming.schemaInference": "true"
}

print("⚡ Đang áp dụng cấu hình Spark...")
for key, value in spark_conf.items():
    try:
        spark.conf.set(key, value)
        print(f"  ✅ {key} = {value}")
    except Exception as e:
        print(f"  ⚠️ Bỏ qua {key}: Hệ thống Serverless tự quản lý (Lỗi 42K0I)")

# COMMAND ----------

# 6. Khởi tạo Hệ thống Logging (Audit Table)
print("📊 Đang khởi tạo bảng quản lý Pipeline...")

audit_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("status", StringType(), False),
    StructField("records_processed", LongType(), True),
    StructField("timestamp", TimestampType(), False)
])

# Tạo bảng Delta rỗng trong schema Bronze để lưu log chạy pipeline
spark.createDataFrame([], audit_schema).write \
    .format("delta") \
    .mode("ignore") \
    .saveAsTable(Config.table("bronze", "_pipeline_audit"))

print(f"  ✅ Bảng Audit sẵn sàng: {CATALOG_NAME}.bronze._pipeline_audit")

# COMMAND ----------

# 7. Xác nhận Trạng thái Cuối cùng
print(f"""
╔══════════════════════════════════════════════════════════╗
║ ✨ SETUP HOÀN TẤT - HỆ THỐNG SẴN SÀNG!                   ║
╠══════════════════════════════════════════════════════════╣
║ Catalog:    {CATALOG_NAME:<44} ║
║ Volume Root: {VOLUME_PATH:<44} ║
║ Raw Data:    {Config.RAW_PATH:<44} ║
║ Status:     SUCCESS                                      ║
╚══════════════════════════════════════════════════════════╝

⏭️ Bước tiếp theo: Chạy notebook 01_download_data.py để nạp dữ liệu.
""")

# COMMAND ----------

# %sql
# -- Lệnh CASCADE cực kỳ quan trọng vì nó sẽ xóa toàn bộ 
# -- Schema (bronze, silver, gold...) và dữ liệu bên trong.
# DROP CATALOG IF EXISTS brazilian_ecommerce CASCADE;

# COMMAND ----------

# # Kiểm tra danh sách file trong Volume
# files = dbutils.fs.ls("/Volumes/brazilian_ecommerce/bronze/source_data/raw")
# for f in files:
#     print(f"✅ Đã tìm thấy: {f.name} ({f.size / 1024 / 1024:.2f} MB)")