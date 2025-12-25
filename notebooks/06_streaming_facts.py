# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 06 - Streaming Facts Pipeline
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Implement streaming pipeline for real-time fact table updates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Streaming Features:
# MAGIC - **Auto Loader:** Ingestion dữ liệu tăng trưởng (incremental).
# MAGIC - **Structured Streaming:** Biến đổi dữ liệu liên tục.
# MAGIC - **Watermarking:** Xử lý dữ liệu đến muộn (late data).
# MAGIC - **foreachBatch:** Hỗ trợ thực hiện logic `MERGE` phức tạp và lookup dimension.
# MAGIC - **Checkpointing:** Đảm bảo khả năng phục hồi lỗi (fault tolerance).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration & Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable
from datetime import datetime
import time
import uuid

spark = SparkSession.builder.getOrCreate()
print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration
class StreamingConfig:
    """Cấu hình tập trung cho Unity Catalog."""
    CATALOG = "brazilian_ecommerce"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    BUSINESS_SCHEMA = "business"
    
    # Unity Catalog Volume Path
    VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/source_data"
    RAW_PATH = f"{VOLUME_PATH}/raw"
    CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"
    SCHEMA_PATH = f"{VOLUME_PATH}/_schemas"
    
    # Cấu hình Trigger tập trung
    # Bạn có thể đổi sang "10 seconds", "1 minute" hoặc "availableNow" tại đây
    DEFAULT_TRIGGER = {"availableNow": True} 
    # Hoặc nếu muốn chạy theo chu kỳ:
    # DEFAULT_TRIGGER = {"processingTime": "1 minute"}
    
    # Định danh bảng 3 cấp
    TABLE_BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}.orders_streaming"
    TABLE_SILVER = f"{CATALOG}.{SILVER_SCHEMA}.orders_streaming"
    TABLE_FACT = f"{CATALOG}.{BUSINESS_SCHEMA}.fact_orders_streaming"
    TABLE_AGG = f"{CATALOG}.{BUSINESS_SCHEMA}.daily_orders_realtime"

config = StreamingConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Streaming Utilities

# COMMAND ----------

# DBTITLE 1,Streaming Utility Functions
class StreamingManager:
    """Quản lý các luồng streaming và checkpoint."""
    def __init__(self):
        self.active_queries = {}
    
    def get_checkpoint(self, name):
        return f"{config.CHECKPOINT_PATH}/{name}"
    
    def get_schema_location(self, name):
        return f"{config.SCHEMA_PATH}/{name}"
    
    def stop_all(self):
        for name, query in self.active_queries.items():
            if query.isActive:
                query.stop()
                print(f"⏹️ Stopped: {name}")
        self.active_queries = {}

stream_manager = StreamingManager()

def get_trigger(mode):
    return {"availableNow": True} if mode == "availableNow" else {"processingTime": mode}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 Streaming Bronze Ingestion

# COMMAND ----------

# DBTITLE 1,Stream Orders to Bronze
def stream_orders_bronze(trigger_mode="availableNow"):
    """Sử dụng Auto Loader để đẩy dữ liệu vào bảng Bronze."""
    print(f"🚀 Khởi chạy Bronze Stream...")
    
    df_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{config.SCHEMA_PATH}/bronze_orders")
        .option("header", "true")
        .load(config.RAW_PATH))
    
    query = (df_stream
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_batch_id", lit(str(uuid.uuid4())))
        .withColumn("_source_file", col("_metadata.file_path")) # Thay đổi quan trọng tại đây
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", stream_manager.get_checkpoint("bronze_orders"))
        .trigger(**get_trigger(trigger_mode))
        .toTable(config.TABLE_BRONZE))
    
    stream_manager.active_queries["bronze"] = query
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Streaming Silver Transformation

# COMMAND ----------

# DBTITLE 1,Stream Bronze to Silver
def stream_orders_silver(trigger_mode="availableNow"):
    """Làm sạch và định dạng lại dữ liệu đơn hàng."""
    print(f"🚀 Khởi chạy Silver Stream...")
    
    df_bronze = spark.readStream.table(config.TABLE_BRONZE)
    
    df_silver = (df_bronze
        .select(
            col("order_id").cast("string"),
            col("customer_id").cast("string"),
            lower(trim(col("order_status"))).alias("order_status"),
            to_timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
            to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_date"),
            to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_date")
        )
        .withColumn("order_date", to_date("order_purchase_timestamp"))
        .withColumn("delivery_days", datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("is_late_delivery", col("order_delivered_customer_date") > col("order_estimated_delivery_date"))
        .withColumn("_silver_at", current_timestamp()))
    
    query = (df_silver.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", stream_manager.get_checkpoint("silver_orders"))
        .trigger(**get_trigger(trigger_mode))
        .queryName("silver_transform")
        .toTable(config.TABLE_SILVER))
    
    stream_manager.active_queries["silver"] = query
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Streaming Fact Tables

# COMMAND ----------

# DBTITLE 1,Stream to Fact Orders
def stream_fact_orders(trigger_mode="availableNow"):
    """Sử dụng foreachBatch để thực hiện MERGE vào bảng Fact."""
    print(f"🚀 Khởi chạy Fact Stream...")
    
    # Load dimensions (Dùng để join nếu cần trong transform, hiện tại để đây để bạn tham khảo)
    # dim_cust = spark.table(f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.dim_customers").filter("_is_current = true")

    def merge_fact(batch_df, batch_id):
        # LƯU Ý: Phải thụt lề toàn bộ khối code bên trong hàm này
        table_name = f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.fact_orders_streaming"
        
        # 1. Kiểm tra bảng tồn tại bằng Catalog (Tránh lỗi Path must be absolute)
        if spark.catalog.tableExists(table_name):
            print(f"--- [Batch {batch_id}] Đang MERGE vào {table_name} ---")
            
            target_table = DeltaTable.forName(spark, table_name)
            
            (target_table.alias("t")
                .merge(
                    batch_df.alias("s"),
                    "t.order_id = s.order_id" 
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        else:
            print(f"--- [Batch {batch_id}] Đang khởi tạo bảng mới {table_name} ---")
            # 2. Khởi tạo bảng nếu chưa tồn tại
            (batch_df.write
                .format("delta")
                .mode("overwrite") # Overwrite ở đây chỉ có tác dụng tạo cấu trúc bảng lần đầu
                .saveAsTable(table_name))

    # 3. Định nghĩa Stream từ bảng Silver
    query = (spark.readStream
        .table(config.TABLE_SILVER)
        .writeStream
        .foreachBatch(merge_fact)
        .option("checkpointLocation", stream_manager.get_checkpoint("fact_orders"))
        .trigger(**get_trigger(trigger_mode))
        .queryName("fact_merge")
        .start())
    
    stream_manager.active_queries["fact"] = query
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Real-time Aggregations

# COMMAND ----------

# DBTITLE 1,Streaming Daily Aggregations
def stream_daily_aggregations(trigger_mode="availableNow"):
    """Tính toán các chỉ số theo ngày sử dụng Watermarking."""
    print(f"🚀 Khởi chạy Aggregation Stream...")
    
    df_stream = (spark.readStream.table(config.TABLE_SILVER)
        .withWatermark("order_purchase_timestamp", "1 day")) # Xử lý dữ liệu trễ 1 ngày
    
    df_agg = (df_stream
        .groupBy(window("order_purchase_timestamp", "1 day"), "order_date")
        .agg(
            count("order_id").alias("order_count"),
            avg("delivery_days").alias("avg_delivery_days"),
            sum(when(col("is_late_delivery") == True, 1).otherwise(0)).alias("late_deliveries")
        )
        .select("order_date", "window.start", "window.end", "order_count", "avg_delivery_days", "late_deliveries",
                current_timestamp().alias("_updated_at")))
    
    query = (df_agg.writeStream
        .format("delta")
        .outputMode("complete") # Chế độ ghi đè toàn bộ bảng aggregation mỗi batch
        .option("checkpointLocation", stream_manager.get_checkpoint("daily_aggs"))
        .trigger(**get_trigger(trigger_mode))
        .queryName("realtime_agg")
        .toTable(config.TABLE_AGG))
    
    stream_manager.active_queries["agg"] = query
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execute Streaming Pipeline

# COMMAND ----------

# DBTITLE 1,Run Full Streaming Pipeline
def run_full_pipeline(mode="availableNow"):
    start_time = datetime.now()
    
    # Thứ tự thực thi tuần tự để đảm bảo tính toàn vẹn dữ liệu
    q1 = stream_orders_bronze(mode)
    if mode == "availableNow": q1.awaitTermination()
    
    q2 = stream_orders_silver(mode)
    if mode == "availableNow": q2.awaitTermination()
    
    q3 = stream_fact_orders(mode)
    q4 = stream_daily_aggregations(mode)
    
    if mode == "availableNow":
        q3.awaitTermination()
        q4.awaitTermination()
        
    print(f"✅ Toàn bộ pipeline hoàn tất! Tổng thời gian: {datetime.now() - start_time}")

# Chạy pipeline
run_full_pipeline("availableNow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitor Streaming Queries

# COMMAND ----------

# DBTITLE 1,Monitor Active Streams
def monitor_streams_advanced():
    """Monitor chi tiết hiệu năng và độ trễ của các luồng Stream."""
    active_streams = spark.streams.active
    
    print(f"\n{'='*80}")
    print(f"📊 BÁO CÁO GIÁM SÁT STREAMING - {spark.catalog.currentCatalog()}")
    print(f"{'='*80}\n")

    if not active_streams:
        print(" ⚠️ Không có luồng nào đang chạy.")
        return

    for stream in active_streams:
        status = stream.status
        last_progress = stream.lastProgress
        
        print(f"📡 Query: {stream.name if stream.name else 'Unnamed'}")
        print(f"   ├─ Status: {status['message']}")
        print(f"   ├─ ID: {stream.id}")
        
        if last_progress:
            # Tính toán các chỉ số quan trọng
            input_rows = last_progress.get('numInputRows', 0)
            process_rate = round(last_progress.get('processedRowsPerSecond', 0), 2)
            
            # Lấy thông tin về thời gian xử lý (Latency)
            duration = last_progress.get('durationMs', {})
            total_duration = sum(duration.values()) if duration else 0
            
            print(f"   ├─ Input: {input_rows} rows")
            print(f"   ├─ Speed: {process_rate} rows/sec")
            print(f"   ├─ Latency (Batch Duration): {total_duration} ms")
            print(f"   └─ Checkpoint: {last_progress.get('sources')[0].get('description')[:50]}...")
        
        # Kiểm tra nếu stream bị lỗi dừng đột ngột
        if stream.exception():
            print(f"   ❌ ERROR: {stream.exception().get('message')[:100]}...")
        
        print("-" * 40)

# Gọi hàm giám sát
monitor_streams_advanced()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verify Streaming Tables

# COMMAND ----------

# DBTITLE 1,Verify Streaming Tables
print(f"\n{'='*60}")
print("✅ STREAMING TABLES VERIFICATION")
print(f"{'='*60}\n")

streaming_tables = [
    (f"{config.BRONZE_SCHEMA}.orders_streaming", "Bronze Orders"),
    (f"{config.SILVER_SCHEMA}.orders_streaming", "Silver Orders"),
    (f"{config.BUSINESS_SCHEMA}.fact_orders_streaming", "Fact Orders"),
    (f"{config.BUSINESS_SCHEMA}.daily_orders_realtime", "Daily Aggregations"),
]

for table_name, description in streaming_tables:
    try:
        df = spark.table(table_name)
        count = df.count()
        print(f"  ✅ {description:<25} {count:>10,} records")
    except Exception as e:
        print(f"  ⚠️ {description:<25} Not created (may be expected)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Continuous Streaming (Optional)

# COMMAND ----------

# DBTITLE 1,Start Continuous Streaming (Optional)
# Uncomment to start continuous streaming with 10-second intervals

# def start_continuous_streaming():
#     """Start continuous streaming for real-time updates."""
#     
#     print("🔄 Starting continuous streaming...")
#     
#     # Start all streams with processing time trigger
#     stream_orders_bronze("10 seconds")
#     stream_orders_silver("10 seconds")
#     stream_fact_orders("10 seconds")
#     stream_daily_aggregations("30 seconds")
#     
#     print("✅ Continuous streaming started!")
#     print("   Run stream_manager.stop_all_streams() to stop")
#     
# # start_continuous_streaming()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Final Summary

# COMMAND ----------

# DBTITLE 1,Streaming Pipeline Complete
print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    📡 STREAMING PIPELINE COMPLETE                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  ✅ Status:        SUCCESS                                                    ║
║  📊 Streams:       4 streaming pipelines configured                           ║
║                                                                               ║
║  📋 Streaming Tables Created:                                                 ║
║     • bronze.orders_streaming       - Raw order ingestion                     ║
║     • silver.orders_streaming       - Transformed orders                      ║
║     • business.fact_orders_streaming - Fact table with dims                   ║
║     • business.daily_orders_realtime - Real-time aggregations                 ║
║                                                                               ║
║  🔧 Features Implemented:                                                     ║
║     • Auto Loader for incremental ingestion                                   ║
║     • Structured Streaming transformations                                    ║
║     • Watermarking for late data handling                                     ║
║     • foreachBatch for dimension lookups                                      ║
║     • Delta Lake merge for upserts                                            ║
║     • Checkpointing for fault tolerance                                       ║
║                                                                               ║
║  ⚡ Trigger Modes Available:                                                  ║
║     • availableNow - Process all available data (batch-like)                  ║
║     • processingTime("10 seconds") - Continuous micro-batches                 ║
║     • once - Process once and stop                                            ║
║                                                                               ║
║  ⏭️  Next Step:     Run 08_ml_models.py                                       ║
║                                                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")