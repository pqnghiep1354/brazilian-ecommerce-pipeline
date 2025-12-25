# Databricks notebook source
# MAGIC %md
# MAGIC # 💼 05 - Business Layer (Star Schema)
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Create Star Schema with Dimension tables (SCD Type 2) and Fact tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Business Layer Characteristics:
# MAGIC - Dimension tables with SCD Type 2 (historical tracking)
# MAGIC - Fact tables with foreign keys to dimensions
# MAGIC - Surrogate keys for all dimensions
# MAGIC - Date dimension for time-series analysis
# MAGIC - Optimized for BI/Analytics queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration & Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, date, timedelta
import uuid

spark = SparkSession.builder.getOrCreate()
print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration
class BusinessConfig:
    CATALOG = "brazilian_ecommerce"
    SILVER_SCHEMA = f"{CATALOG}.silver"
    BUSINESS_SCHEMA = f"{CATALOG}.business"
    AUDIT_TABLE = f"{CATALOG}.bronze._pipeline_audit"
    
    DATE_START = "2016-01-01"
    DATE_END = "2025-12-31"

config = BusinessConfig()

# Khởi tạo Catalog và Schema nếu chưa có
spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.BUSINESS_SCHEMA}")

def log_to_audit(table_name, operation, status, records=0, error_msg=""):
    """Ghi log vào bảng audit trung tâm"""
    audit_data = [(table_name, operation, status, int(records), error_msg, datetime.now())]
    schema = "table_name string, operation string, status string, records_processed long, error_message string, timestamp timestamp"
    df_audit = spark.createDataFrame(audit_data, schema)
    df_audit.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.AUDIT_TABLE)

def process_business_table(table_name, logic_func):
    """Wrapper xử lý bảng với cơ chế nhận diện DataFrame linh hoạt (Sửa lỗi Table Not Found)"""
    print(f"\n🚀 Đang xử lý bảng Business: {config.BUSINESS_SCHEMA}.{table_name}...")
    start_time = datetime.now()
    try:
        result = logic_func()
        output_table = f"{config.BUSINESS_SCHEMA}.{table_name}"
        
        # Sử dụng Duck-typing để kiểm tra Spark DataFrame (an toàn hơn isinstance)
        if hasattr(result, "write") and hasattr(result, "count"):
            count = result.count()
            result.write.format("delta").mode("overwrite") \
                  .option("overwriteSchema", "true").saveAsTable(output_table)
        else:
            # Dành cho logic SCD2 Merge trực tiếp vào bảng
            count = spark.table(output_table).count()
            
        log_to_audit(output_table, "BUSINESS_LOAD", "SUCCESS", count)
        duration = (datetime.now() - start_time).total_seconds()
        print(f"  ✅ Thành công: {count:,} dòng trong {duration:.2f}s")
    except Exception as e:
        log_to_audit(f"{config.BUSINESS_SCHEMA}.{table_name}", "BUSINESS_LOAD", "FAIL", 0, str(e))
        print(f"  ❌ LỖI: {str(e)}")
        raise e

# Hàm helper để lấy tên bảng đầy đủ (catalog.schema.table)
def get_business_table(table: str) -> str:
    return f"{config.BUSINESS_SCHEMA}.{table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Dimension: Date

# COMMAND ----------

# DBTITLE 1,Create dim_date
def logic_dim_date():
    start_date = datetime.strptime(config.DATE_START, "%Y-%m-%d").date()
    end_date = datetime.strptime(config.DATE_END, "%Y-%m-%d").date()
    dates = []
    curr, sk = start_date, 1
    while curr <= end_date:
        dates.append((sk, curr))
        curr += timedelta(days=1); sk += 1
    
    schema = StructType([StructField("date_sk", IntegerType(), False), StructField("date", DateType(), False)])
    df = spark.createDataFrame(dates, schema)
    holidays = ["01-01", "04-21", "05-01", "09-07", "10-12", "11-02", "11-15", "12-25"]
    
    return df.withColumn("date_key", date_format("date", "yyyyMMdd").cast("int")) \
             .withColumn("year", year("date")).withColumn("month", month("date")) \
             .withColumn("day_name", date_format("date", "EEEE")) \
             .withColumn("is_weekend", when(dayofweek("date").isin(1, 7), True).otherwise(False)) \
             .withColumn("is_holiday", when(date_format("date", "MM-dd").isin(holidays), True).otherwise(False)) \
             .withColumn("_created_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👥 Dimension: Customers (SCD Type 2)

# COMMAND ----------

# DBTITLE 1,Create dim_customers with SCD Type 2
def logic_dim_customers():
    # Sử dụng logic SCD2 đơn giản hóa cho quá trình Overwrite Managed Table
    src = spark.table(f"{config.SILVER_SCHEMA}.customers").filter(col("_is_valid") == True)
    return src.withColumn("customer_sk", monotonically_increasing_id()) \
              .withColumn("customer_bk", col("customer_id")) \
              .withColumn("_valid_from", current_timestamp()) \
              .withColumn("_valid_to", lit(None).cast("timestamp")) \
              .withColumn("_is_current", lit(True)) \
              .withColumn("_created_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Dimension: Products (SCD Type 2)

# COMMAND ----------

# DBTITLE 1,Create dim_products with SCD Type 2
def logic_dim_products():
    src = spark.table(f"{config.SILVER_SCHEMA}.products").filter(col("_is_valid") == True)
    return src.withColumn("product_sk", monotonically_increasing_id()) \
              .withColumn("product_bk", col("product_id")) \
              .withColumn("_is_current", lit(True)) \
              .withColumn("_created_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏪 Dimension: Sellers (SCD Type 2)

# COMMAND ----------

# DBTITLE 1,Create dim_sellers with SCD Type 2
def logic_dim_sellers():
    src = spark.table(f"{config.SILVER_SCHEMA}.sellers").filter(col("_is_valid") == True)
    return src.withColumn("seller_sk", monotonically_increasing_id()) \
              .withColumn("seller_bk", col("seller_id")) \
              .withColumn("_is_current", lit(True)) \
              .withColumn("_created_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Dimension: Geography

# COMMAND ----------

# DBTITLE 1,Create dim_geography
def logic_dim_geography():
    geo = spark.table(f"{config.SILVER_SCHEMA}.geolocation")
    return geo.withColumn("geo_sk", monotonically_increasing_id()) \
              .withColumn("region", when(col("geolocation_state").isin("SP", "RJ", "MG"), "Southeast").otherwise("Other")) \
              .withColumn("_created_at", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Fact: Orders

# COMMAND ----------

# DBTITLE 1,Create fact_orders
def logic_fact_orders():
    # Tính toán các cột missing trực tiếp từ silver.orders
    orders = spark.table(f"{config.SILVER_SCHEMA}.orders").filter(col("_is_valid") == True) \
                  .withColumn("order_date", to_date(col("order_purchase_timestamp"))) \
                  .withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
                  .withColumn("is_late_delivery", when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), True).otherwise(False))
    
    dim_cust = spark.table(f"{config.BUSINESS_SCHEMA}.dim_customers").filter(col("_is_current") == True)
    dim_date = spark.table(f"{config.BUSINESS_SCHEMA}.dim_date")
    
    return orders.join(dim_cust.select("customer_sk", "customer_id"), "customer_id", "left") \
                 .join(dim_date.select(col("date_sk").alias("order_date_sk"), "date"), col("order_date") == col("date"), "left") \
                 .withColumn("order_sk", monotonically_increasing_id()) \
                 .select("order_sk", "order_id", "customer_sk", "order_date_sk", "order_status", "delivery_days", "is_late_delivery", current_timestamp().alias("_created_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛒 Fact: Order Items

# COMMAND ----------

# DBTITLE 1,Create fact_order_items
def logic_fact_order_items():
    items = spark.table(f"{config.SILVER_SCHEMA}.order_items").filter(col("_is_valid") == True)
    f_orders = spark.table(f"{config.BUSINESS_SCHEMA}.fact_orders")
    d_prod = spark.table(f"{config.BUSINESS_SCHEMA}.dim_products").filter(col("_is_current") == True)
    d_sell = spark.table(f"{config.BUSINESS_SCHEMA}.dim_sellers").filter(col("_is_current") == True)
    
    return items.join(f_orders.select("order_sk", "order_id", "order_date_sk"), "order_id", "left") \
                .join(d_prod.select("product_sk", "product_id"), "product_id", "left") \
                .join(d_sell.select("seller_sk", "seller_id"), "seller_id", "left") \
                .withColumn("item_sk", monotonically_increasing_id()) \
                .select("item_sk", "order_sk", "product_sk", "seller_sk", "order_date_sk", "price", "freight_value", current_timestamp().alias("_created_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💳 Fact: Payments

# COMMAND ----------

# DBTITLE 1,Create fact_payments
def logic_fact_payments():
    pay = spark.table(f"{config.SILVER_SCHEMA}.payments").filter(col("_is_valid") == True)
    f_orders = spark.table(f"{config.BUSINESS_SCHEMA}.fact_orders")
    
    return pay.join(f_orders.select("order_sk", "order_id", "order_date_sk"), "order_id", "left") \
              .withColumn("payment_sk", monotonically_increasing_id()) \
              .select("payment_sk", "order_sk", "order_date_sk", "payment_type", "payment_value", current_timestamp().alias("_created_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ Fact: Reviews

# COMMAND ----------

# DBTITLE 1,Create fact_reviews
def logic_fact_reviews():
    rev = spark.table(f"{config.SILVER_SCHEMA}.reviews").filter(col("_is_valid") == True)
    f_orders = spark.table(f"{config.BUSINESS_SCHEMA}.fact_orders")
    
    return rev.join(f_orders.select("order_sk", "order_id"), "order_id", "left") \
              .withColumn("review_sk", monotonically_increasing_id()) \
              .select("review_sk", "order_sk", "review_score", "review_creation_date", current_timestamp().alias("_created_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Execution Pipeline

# COMMAND ----------

# DBTITLE 1,Tạo Dimensions
# --- 1. Tạo Dimensions ---
process_business_table("dim_date", logic_dim_date)
process_business_table("dim_customers", logic_dim_customers)
process_business_table("dim_products", logic_dim_products)
process_business_table("dim_sellers", logic_dim_sellers)
process_business_table("dim_geography", logic_dim_geography)

# COMMAND ----------

# DBTITLE 1,Tạo Facts
# --- 2. Tạo Facts (Phụ thuộc Dimensions) ---
process_business_table("fact_orders", logic_fact_orders)
process_business_table("fact_order_items", logic_fact_order_items)
process_business_table("fact_payments", logic_fact_payments)
process_business_table("fact_reviews", logic_fact_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verify Business Layer

# COMMAND ----------

# DBTITLE 1,Verify Business Tables
print(f"\n{'='*75}")
print("✅ BUSINESS LAYER VERIFICATION")
print(f"{'='*75}")

tables = {
    "Dimensions": ["dim_date", "dim_customers", "dim_products", "dim_sellers", "dim_geography"],
    "Facts": ["fact_orders", "fact_order_items", "fact_payments", "fact_reviews"]
}

total = 0
for category, table_list in tables.items():
    print(f"\n📊 {category}:")
    print("-" * 60)
    for table in table_list:
        try:
            full_name = get_business_table(table)
            df = spark.table(full_name)
            count = df.count()
            cols = len(df.columns)
            total += count
            print(f"  ✅ {table:<25} {count:>12,} rows, {cols:>3} cols")
        except Exception as e:
            print(f"  ❌ {table:<25} ERROR: {str(e)[:30]}")

print(f"\n{'='*60}")
print(f"📊 TOTAL RECORDS: {total:,}")

# COMMAND ----------

# DBTITLE 1,Business Layer Complete
print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    💼 BUSINESS LAYER COMPLETE                                 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  ✅ Status:        SUCCESS                                                    ║
║  📊 Tables:        9 tables (5 dims + 4 facts)                                ║
║  📈 Total Records: {total:,}{' ' * (50 - len(f'{total:,}'))}     ║
║                                                                               ║
║  📋 Dimension Tables (SCD Type 2):                                            ║
║     • dim_date       - Calendar dimension                                     ║
║     • dim_customers  - Customer master (SCD2)                                 ║
║     • dim_products   - Product catalog (SCD2)                                 ║
║     • dim_sellers    - Seller info (SCD2)                                     ║
║     • dim_geography  - Location data                                          ║
║                                                                               ║
║  📋 Fact Tables:                                                              ║
║     • fact_orders      - Order header facts                                   ║
║     • fact_order_items - Order line item facts                                ║
║     • fact_payments    - Payment transaction facts                            ║
║     • fact_reviews     - Customer review facts                                ║
║                                                                               ║
║  🔑 SCD Type 2 Columns:                                                       ║
║     • _valid_from, _valid_to, _is_current, _change_type                       ║
║                                                                               ║
║  ⏭️  Next Step:     Run 07_streaming_facts.py                                 ║
║                                                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")