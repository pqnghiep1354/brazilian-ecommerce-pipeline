# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 03 - Silver Layer Transformation
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Transform Bronze data into cleansed, validated Silver tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Silver Layer Characteristics:
# MAGIC - Data type casting and standardization
# MAGIC - Null handling and default values
# MAGIC - Deduplication and data cleansing
# MAGIC - Business rules validation
# MAGIC - Referential integrity checks
# MAGIC - Data quality metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration & Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import uuid

spark = SparkSession.builder.getOrCreate()

print("✅ Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Configuration
# Cấu hình định danh (Đồng bộ với Notebook 02 & 03)
CATALOG_NAME = "brazilian_ecommerce"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/source_data"

class SilverConfig:
    # Danh sách đầy đủ 9 bảng từ bước Bronze Ingestion
    TABLE_MAP = {
        "customers": ["customer_id"],
        "orders": ["order_id"],
        "order_items": ["order_id", "order_item_id"],
        "products": ["product_id"],
        "sellers": ["seller_id"],
        "payments": ["order_id", "payment_sequential"],
        "reviews": ["review_id"],
        "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
        "category_translation": ["product_category_name"]
    }

print(f"✅ Đang thực hiện chuyển đổi cho Catalog: {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Utility Functions

# COMMAND ----------

# DBTITLE 1,Utility Functions
# 2. Utility Functions
def add_silver_metadata(df):
    """Thêm cột metadata chuẩn cho lớp Silver."""
    return df \
        .withColumn("_silver_timestamp", current_timestamp()) \
        .withColumn("_silver_batch_id", lit(str(uuid.uuid4()))) \
        .withColumn("_is_valid", lit(True))

def deduplicate_data(df, primary_keys):
    """
    Loại bỏ trùng lặp bằng Window Function.
    Giữ lại bản ghi có thời gian nạp Bronze mới nhất.
    """
    if not primary_keys: return df
    
    # Sắp xếp theo thời gian nạp ở lớp Bronze (đã tạo ở Notebook 03)
    window = Window.partitionBy(primary_keys).orderBy(col("_bronze_ingestion_ts").desc())
    
    return df.withColumn("row_num", row_number().over(window)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")

def log_audit(table_name, status, count=0):
    """Ghi log vào bảng audit tập trung tại lớp Bronze."""
    try:
        audit_data = [(str(uuid.uuid4()), table_name, "SILVER_TRANSFORM", status, count, datetime.now())]
        spark.createDataFrame(audit_data, ["event_id", "table_name", "operation", "status", "records_processed", "timestamp"]) \
             .write.format("delta").mode("append").saveAsTable(f"{CATALOG_NAME}.bronze._pipeline_audit")
    except:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Transformation Functions

# COMMAND ----------

# DBTITLE 1,Main Transformation Engine
# 3. Main Transformation Engine
def transform_to_silver(table_name, pk_cols):
    print(f"🔄 Chuyển đổi bảng: {table_name}")
    
    source_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{table_name}"
    target_table = f"{CATALOG_NAME}.{SILVER_SCHEMA}.{table_name}"
    
    df = spark.table(source_table)
    
    # 1. Làm sạch chung: Trim khoảng trắng
    for c in df.columns:
        if isinstance(df.schema[c].dataType, StringType):
            df = df.withColumn(c, trim(col(c)))

    # 2. Định nghĩa danh sách cột Timestamp cụ thể cho từng bảng để tránh ép kiểu nhầm cột 'status'
    timestamp_mapping = {
        "orders": [
            "order_purchase_timestamp", "order_approved_at", 
            "order_delivered_carrier_date", "order_delivered_customer_date", 
            "order_estimated_delivery_date"
        ],
        "order_items": ["shipping_limit_date"],
        "reviews": ["review_creation_date", "review_answer_timestamp"]
    }

    # Thực hiện ép kiểu TIMESTAMP an toàn cho các cột đã định nghĩa
    if table_name in timestamp_mapping:
        for c in timestamp_mapping[table_name]:
            if c in df.columns:
                # to_timestamp mặc định sẽ trả về NULL nếu format không khớp, không gây crash
                df = df.withColumn(c, to_timestamp(col(c)))

    # 3. Chuẩn hóa riêng cho các cột địa lý và tiền tệ
    if table_name in ["customers", "sellers", "geolocation"]:
        zip_col = "geolocation_zip_code_prefix" if table_name == "geolocation" else f"{table_name[:-1]}_zip_code_prefix"
        df = df.withColumn(zip_col, lpad(col(zip_col).cast("string"), 5, "0"))
        
    if table_name in ["order_items", "payments", "products"]:
        decimal_cols = ["price", "freight_value", "payment_value", "product_weight_g", 
                        "product_length_cm", "product_height_cm", "product_width_cm"]
        for c in decimal_cols:
            if c in df.columns:
                df = df.withColumn(c, col(c).cast("decimal(10,2)"))

    # 4. Khử trùng dữ liệu dựa trên PK và thời gian nạp Bronze mới nhất
    df = deduplicate_data(df, pk_cols)
    
    # 5. Thêm metadata Silver
    df = add_silver_metadata(df)
    
    # 6. Ghi dữ liệu xuống lớp Silver
    df.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(target_table)
    
    final_count = df.count()
    log_audit(table_name, "SUCCESS", final_count)
    print(f"  ✅ Hoàn tất: {final_count:,} dòng đã ghi vào {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Silver Transformation

# COMMAND ----------

# DBTITLE 1,Run All Silver Transformations
print("🏁 Bắt đầu tiến trình Silver Transformation...")

for table, pks in SilverConfig.TABLE_MAP.items():
    try:
        transform_to_silver(table, pks)
    except Exception as e:
        print(f"  ❌ Lỗi tại bảng {table}: {str(e)}")
        log_audit(table, "FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verify Silver Tables

# COMMAND ----------

# DBTITLE 1,Verify Silver Tables
# %sql
# -- 5. Verify Results
# -- Kiểm tra trạng thái nạp dữ liệu từ bảng Audit
# SELECT table_name, operation, status, records_processed, timestamp 
# FROM brazilian_ecommerce.bronze._pipeline_audit 
# WHERE operation = 'SILVER_TRANSFORM'
# ORDER BY timestamp DESC;

# COMMAND ----------

# DBTITLE 1,PySpark Automation Check
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# def check_pipeline_status(table_name=None):
#     """
#     Kiểm tra trạng thái nạp dữ liệu từ bảng audit.
#     Nếu trạng thái là 'FAIL', dừng pipeline bằng cách raise Exception.
#     """
#     print("Checking pipeline audit status...")
    
#     # 1. Truy vấn bản ghi mới nhất của tiến trình SILVER_TRANSFORM
#     query = """
#         SELECT table_name, operation, status, records_processed, timestamp 
#         FROM brazilian_ecommerce.bronze._pipeline_audit 
#         WHERE operation = 'SILVER_TRANSFORM'
#         ORDER BY timestamp DESC 
#         LIMIT 1
#     """
    
#     audit_df = spark.sql(query)
    
#     # 2. Kiểm tra xem có dữ liệu audit hay không
#     if audit_df.count() == 0:
#         raise Exception("CRITICAL: Không tìm thấy lịch sử vận hành trong bảng audit!")
    
#     # Lấy thông tin bản ghi mới nhất
#     latest_run = audit_df.first()
#     status = latest_run['status']
#     target_table = latest_run['table_name']
#     records = latest_run['records_processed']
    
#     print(f"Table: {target_table} | Status: {status} | Records: {records}")

#     # 3. Logic kiểm tra và dừng Pipeline
#     if status.upper() == 'FAIL':
#         error_msg = f"PIPELINE STOPPED: Tiến trình nạp bảng {target_table} thất bại. Vui lòng kiểm tra log!"
#         raise Exception(error_msg)
    
#     elif status.upper() == 'SUCCESS' and records == 0:
#         # Trường hợp thành công nhưng không có dữ liệu (có thể là cảnh báo)
#         print("WARNING: Tiến trình thành công nhưng không có bản ghi nào được xử lý.")
    
#     else:
#         print(f"SUCCESS: Tiến trình cho bảng {target_table} đã hoàn thành tốt.")

# # Thực thi hàm kiểm tra
# try:
#     check_pipeline_status()
# except Exception as e:
#     # Nếu chạy trong Databricks Workflow, raise Exception sẽ làm Task này bị Mark là Failed
#     raise e

# COMMAND ----------

# DBTITLE 1,Data Quality - DQ
# def quality_gate_silver_orders():
#     # Kiểm tra không được có ID trùng lặp ở tầng Silver
#     duplicate_count = spark.sql("SELECT order_id FROM brazilian_ecommerce.silver.orders GROUP BY order_id HAVING COUNT(*) > 1").count()
    
#     if duplicate_count > 0:
#         raise Exception(f"DATA QUALITY FAILED: Phát hiện {duplicate_count} dòng trùng lặp trong bảng Silver Orders!")
    
#     print("Data Quality Check Passed: No duplicates found.")

# # Gọi hàm sau khi kiểm tra Audit Status
# quality_gate_silver_orders()

# COMMAND ----------

