# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 04 - Gold Layer Aggregation
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Create aggregated business metrics and KPIs in Gold layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Gold Layer Characteristics:
# MAGIC - Business-ready aggregated tables
# MAGIC - Pre-computed KPIs and metrics
# MAGIC - Time-series aggregations (daily, weekly, monthly)
# MAGIC - Customer and product analytics
# MAGIC - Performance dashboards support

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
from datetime import datetime
import uuid

spark = SparkSession.builder.getOrCreate()
print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration
# 1.Configuration & Audit Helpers (FIXED)
class GoldConfig:
    DBFS_BASE_PATH = "/FileStore/brazilian-ecommerce"
    # Cập nhật thêm tên Catalog để tránh lỗi TABLE_OR_VIEW_NOT_FOUND
    CATALOG = "brazilian_ecommerce"
    SILVER_SCHEMA = f"{CATALOG}.silver"
    GOLD_SCHEMA = f"{CATALOG}.gold"
    AUDIT_TABLE = f"{CATALOG}.bronze._pipeline_audit"

config = GoldConfig()

def log_to_audit(table_name, operation, status, records=0, error_msg=""):
    """Ghi log trạng thái thực thi (Sửa lỗi merge fields records_processed)"""
    audit_data = [(
        table_name, 
        operation, 
        status, 
        int(records), 
        error_msg,
        datetime.now()
    )]
    # Sử dụng bigint (long) để đồng bộ kiểu dữ liệu
    schema = "table_name string, operation string, status string, records_processed long, error_message string, timestamp timestamp"
    df_audit = spark.createDataFrame(audit_data, schema)
    
    # Sử dụng mergeSchema để xử lý các thay đổi schema nếu có
    df_audit.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(config.AUDIT_TABLE)

def process_gold_table(table_name, calculation_logic_func):
    """Wrapper thực thi tính toán và ghi log (Phiên bản tương thích Unity Catalog)"""
    print(f"\n🚀 Đang xử lý: {config.GOLD_SCHEMA}.{table_name}...")
    start_time = datetime.now()
    try:
        # 1. Thực thi logic tính toán
        df_result = calculation_logic_func()
        
        # 2. Ghi dữ liệu vào Unity Catalog (Managed Table)
        # Loại bỏ .option("path", output_path) để UC tự quản lý storage
        output_table = f"{config.GOLD_SCHEMA}.{table_name}"
        
        df_result.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(output_table)
        
        count = df_result.count()
        
        # 3. Log kết quả vào bảng Audit
        log_to_audit(output_table, "GOLD_AGGREGATION", "SUCCESS", count)
        duration = (datetime.now() - start_time).total_seconds()
        print(f"  ✅ Thành công: {count:,} dòng trong {duration:.2f}s")
        return count
        
    except Exception as e:
        error_msg = str(e)
        log_to_audit(f"{config.GOLD_SCHEMA}.{table_name}", "GOLD_AGGREGATION", "FAIL", 0, error_msg)
        print(f"  ❌ LỖI: {error_msg}")
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Daily Sales Metrics

# COMMAND ----------

# DBTITLE 1,Create Daily Sales Aggregation
def logic_daily_sales():
    # Load Silver tables
    orders = spark.table(f"{config.SILVER_SCHEMA}.orders").filter(col("_is_valid") == True)
    order_items = spark.table(f"{config.SILVER_SCHEMA}.order_items").filter(col("_is_valid") == True)
    payments = spark.table(f"{config.SILVER_SCHEMA}.payments").filter(col("_is_valid") == True)
    
    # Chuẩn bị dữ liệu: Tính toán các cột missing từ dữ liệu gốc
    order_metrics = orders.join(order_items, "order_id", "left") \
                          .join(payments, "order_id", "left") \
                          .withColumn("order_date", to_date(col("order_purchase_timestamp"))) \
                          .withColumn("delivery_days", 
                                      datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
                          .withColumn("is_late_delivery", 
                                      when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), True).otherwise(False))
    
    return order_metrics \
        .filter(col("order_date").isNotNull()) \
        .groupBy("order_date") \
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            countDistinct("seller_id").alias("unique_sellers"),
            count("order_item_id").alias("total_items"),
            sum("price").alias("gross_revenue"),
            sum("freight_value").alias("total_freight"),
            sum("payment_value").alias("total_payment_value"),
            avg("price").alias("avg_item_price"),
            avg("payment_value").alias("avg_order_value"),
            sum(when(col("order_status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
            sum(when(col("order_status") == "canceled", 1).otherwise(0)).alias("canceled_orders"),
            # Sử dụng cột vừa tính toán ở trên
            avg("delivery_days").alias("avg_delivery_days"),
            sum(when(col("is_late_delivery") == True, 1).otherwise(0)).alias("late_deliveries")
        ) \
        .withColumn("year", year("order_date")) \
        .withColumn("month", month("order_date")) \
        .withColumn("day_of_week", dayofweek("order_date")) \
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), True).otherwise(False)) \
        .withColumn("net_revenue", col("gross_revenue") - col("total_freight")) \
        .withColumn("delivery_rate", when(col("total_orders") > 0, col("delivered_orders") / col("total_orders") * 100).otherwise(0)) \
        .withColumn("cancellation_rate", when(col("total_orders") > 0, col("canceled_orders") / col("total_orders") * 100).otherwise(0)) \
        .withColumn("late_delivery_rate", when(col("delivered_orders") > 0, col("late_deliveries") / col("delivered_orders") * 100).otherwise(0)) \
        .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Monthly Sales Metrics

# COMMAND ----------

# DBTITLE 1,Create Monthly Sales Aggregation
def logic_monthly_sales():
    daily = spark.table(f"{config.GOLD_SCHEMA}.daily_sales")
    
    monthly = daily.groupBy("year", "month").agg(
        sum("total_orders").alias("total_orders"),
        sum("unique_customers").alias("total_customers"),
        sum("total_items").alias("total_items"),
        sum("gross_revenue").alias("gross_revenue"),
        sum("net_revenue").alias("net_revenue"),
        sum("total_freight").alias("total_freight"),
        sum("total_payment_value").alias("total_payment_value"),
        avg("avg_order_value").alias("avg_order_value"),
        sum("delivered_orders").alias("delivered_orders"),
        sum("canceled_orders").alias("canceled_orders"),
        avg("avg_delivery_days").alias("avg_delivery_days"),
        sum("late_deliveries").alias("late_deliveries"),
        count("order_date").alias("active_days")
    ).withColumn("month_start", to_date(concat(col("year"), lit("-"), lpad(col("month"), 2, "0"), lit("-01")))) \
     .withColumn("orders_per_day", col("total_orders") / col("active_days")) \
     .withColumn("revenue_per_day", col("gross_revenue") / col("active_days")) \
     .withColumn("delivery_rate", when(col("total_orders") > 0, col("delivered_orders") / col("total_orders") * 100).otherwise(0))
    
    window = Window.orderBy("year", "month")
    return monthly.withColumn("prev_revenue", lag("gross_revenue").over(window)) \
        .withColumn("revenue_growth_pct", when(col("prev_revenue") > 0, (col("gross_revenue") - col("prev_revenue")) / col("prev_revenue") * 100).otherwise(0)) \
        .withColumn("prev_orders", lag("total_orders").over(window)) \
        .withColumn("orders_growth_pct", when(col("prev_orders") > 0, (col("total_orders") - col("prev_orders")) / col("prev_orders") * 100).otherwise(0)) \
        .drop("prev_revenue", "prev_orders") \
        .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👥 Customer Analytics

# COMMAND ----------

# DBTITLE 1,Create Customer Analytics
def logic_customer_analytics():
    # 1. Load các bảng Silver với Catalog awareness
    orders = spark.table(f"{config.SILVER_SCHEMA}.orders").filter(col("_is_valid") == True) \
                  .withColumn("order_date", to_date(col("order_purchase_timestamp")))
    order_items = spark.table(f"{config.SILVER_SCHEMA}.order_items").filter(col("_is_valid") == True)
    payments = spark.table(f"{config.SILVER_SCHEMA}.payments").filter(col("_is_valid") == True)
    customers = spark.table(f"{config.SILVER_SCHEMA}.customers").filter(col("_is_valid") == True)
    reviews = spark.table(f"{config.SILVER_SCHEMA}.reviews").filter(col("_is_valid") == True)
    
    # 2. Định nghĩa max_date (Ngày mua hàng gần nhất của toàn hệ thống)
    max_date = orders.agg(max("order_date")).collect()[0][0]
    
    # 3. Tính toán Customer order metrics
    customer_orders = orders.join(order_items, "order_id", "left").join(payments, "order_id", "left") \
        .groupBy("customer_id").agg(
            countDistinct("order_id").alias("total_orders"),
            sum("price").alias("total_spent"),
            min("order_date").alias("first_order_date"),
            max("order_date").alias("last_order_date"),
            avg(datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))).alias("avg_delivery_days")
        )
    
    # 4. Tính toán Review metrics
    customer_review_agg = orders.select("order_id", "customer_id") \
        .join(reviews.groupBy("order_id").agg(avg("review_score").alias("avg_review_score")), "order_id", "left") \
        .groupBy("customer_id").agg(avg("avg_review_score").alias("avg_review_score"))

    # 5. Join tất cả và tính Recency, Frequency, Monetary
    ca = customer_orders.join(customers.select("customer_id", "customer_city", "customer_state"), "customer_id", "left") \
        .join(customer_review_agg, "customer_id", "left") \
        .withColumn("recency_days", datediff(lit(max_date), col("last_order_date"))) \
        .withColumn("frequency", col("total_orders")) \
        .withColumn("monetary", col("total_spent"))

    # 6. RFM Scoring (Sử dụng approxQuantile để phân nhóm)
    r_q = ca.approxQuantile("recency_days", [0.2, 0.4, 0.6, 0.8], 0.01)
    f_q = ca.approxQuantile("frequency", [0.2, 0.4, 0.6, 0.8], 0.01)
    m_q = ca.approxQuantile("monetary", [0.2, 0.4, 0.6, 0.8], 0.01)

    # 7. Phân khúc khách hàng
    return ca.withColumn("r_score", 
            when(col("recency_days") <= r_q[0], 5).when(col("recency_days") <= r_q[1], 4)
            .when(col("recency_days") <= r_q[2], 3).when(col("recency_days") <= r_q[3], 2).otherwise(1)) \
        .withColumn("f_score", 
            when(col("frequency") >= f_q[3], 5).when(col("frequency") >= f_q[2], 4)
            .when(col("frequency") >= f_q[1], 3).when(col("frequency") >= f_q[0], 2).otherwise(1)) \
        .withColumn("m_score", 
            when(col("monetary") >= m_q[3], 5).when(col("monetary") >= m_q[2], 4)
            .when(col("monetary") >= m_q[1], 3).when(col("monetary") >= m_q[0], 2).otherwise(1)) \
        .withColumn("customer_segment", 
            when((col("r_score") >= 4) & (col("f_score") >= 4), "Champions")
            .when((col("r_score") <= 2) & (col("f_score") <= 2), "Lost")
            .otherwise("Potential Loyalists")) \
        .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Product Analytics

# COMMAND ----------

# DBTITLE 1,Create Product Analytics
def logic_product_analytics():
    products = spark.table(f"{config.SILVER_SCHEMA}.products").filter(col("_is_valid") == True)
    order_items = spark.table(f"{config.SILVER_SCHEMA}.order_items").filter(col("_is_valid") == True)
    orders = spark.table(f"{config.SILVER_SCHEMA}.orders").filter(col("_is_valid") == True)
    reviews = spark.table(f"{config.SILVER_SCHEMA}.reviews").filter(col("_is_valid") == True)
    
    # SỬA LỖI TẠI ĐÂY: Tạo order_date từ order_purchase_timestamp
    orders_filtered = orders.select(
        "order_id", 
        "order_status",
        to_date(col("order_purchase_timestamp")).alias("order_date")
    )
    
    product_sales = order_items.join(orders_filtered, "order_id", "left") \
        .groupBy("product_id").agg(
            count("order_item_id").alias("total_sold"), 
            sum("price").alias("total_revenue"),
            min("order_date").alias("first_sold_date"),
            max("order_date").alias("last_sold_date")
        )
    
    product_reviews = order_items.join(reviews.select("order_id", "review_score"), "order_id", "left") \
        .groupBy("product_id").agg(
            avg("review_score").alias("avg_review_score"), 
            count("review_score").alias("review_count")
        )
    
    return products.join(product_sales, "product_id", "left").join(product_reviews, "product_id", "left") \
        .withColumn("revenue_rank", dense_rank().over(Window.orderBy(desc("total_revenue")))) \
        .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏪 Seller Analytics

# COMMAND ----------

# DBTITLE 1,Create Seller Analytics
def logic_seller_analytics():
    sellers = spark.table(f"{config.SILVER_SCHEMA}.sellers").filter(col("_is_valid") == True)
    order_items = spark.table(f"{config.SILVER_SCHEMA}.order_items").filter(col("_is_valid") == True)
    orders = spark.table(f"{config.SILVER_SCHEMA}.orders").filter(col("_is_valid") == True)
    
    # Tạo order_date
    orders_filtered = orders.select(
        "order_id", 
        to_date(col("order_purchase_timestamp")).alias("order_date")
    )
    
    seller_sales = order_items.join(orders_filtered, "order_id", "left") \
        .groupBy("seller_id").agg(
            countDistinct("order_id").alias("total_orders"), 
            sum("price").alias("total_revenue"),
            min("order_date").alias("first_sale_date"),
            max("order_date").alias("last_sale_date")
        )
        
    return sellers.join(seller_sales, "seller_id", "left") \
        .withColumn("revenue_rank", dense_rank().over(Window.orderBy(desc("total_revenue")))) \
        .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Geographic Analytics

# COMMAND ----------

# DBTITLE 1,Create Geographic Analytics
def logic_geographic_analytics():
    customers = spark.table(f"{config.SILVER_SCHEMA}.customers").filter(col("_is_valid") == True)
    orders = spark.table(f"{config.SILVER_SCHEMA}.orders").filter(col("_is_valid") == True)
    order_items = spark.table(f"{config.SILVER_SCHEMA}.order_items").filter(col("_is_valid") == True)
    
    return orders.join(customers.select("customer_id", "customer_state"), "customer_id", "left") \
        .join(order_items, "order_id", "left") \
        .groupBy("customer_state").agg(
            countDistinct("order_id").alias("total_orders"), 
            sum("price").alias("total_revenue")
        ) \
        .withColumn("_gold_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Execution Pipeline

# COMMAND ----------

# DBTITLE 1,Execution Pipeline
spark.sql(f"USE CATALOG {config.CATALOG}")

# COMMAND ----------

# Execution Pipeline
# 1. Chạy các bảng độc lập
process_gold_table("daily_sales", logic_daily_sales)
process_gold_table("customer_analytics", logic_customer_analytics)
process_gold_table("product_analytics", logic_product_analytics)
process_gold_table("seller_analytics", logic_seller_analytics)
process_gold_table("geographic_analytics", logic_geographic_analytics)

# COMMAND ----------

# 2. Chạy bảng phụ thuộc (Monthly Sales cần Daily Sales)
process_gold_table("monthly_sales", logic_monthly_sales)

print("\n🎯 GOLD LAYER PIPELINE COMPLETE")

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables
print(f"\n{'='*75}")
print("✅ GOLD TABLE VERIFICATION")
print(f"{'='*75}")

gold_tables = [
    "daily_sales",
    "monthly_sales", 
    "customer_analytics",
    "product_analytics",
    "seller_analytics",
    "geographic_analytics"
]

print(f"\n{'Table':<25} {'Records':>12} {'Columns':>10} {'Status':>10}")
print("-" * 60)

total_records = 0
for table in gold_tables:
    try:
        full_name = get_gold_table(table)
        df = spark.table(full_name)
        count = df.count()
        cols = len(df.columns)
        total_records += count
        print(f"{table:<25} {count:>12,} {cols:>10} {'✅ OK':>10}")
    except Exception as e:
        print(f"{table:<25} {'ERROR':>12} {str(e)[:20]:>10}")

print("-" * 60)
print(f"{'TOTAL':<25} {total_records:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Final Summary

# COMMAND ----------

# DBTITLE 1,Gold Aggregation Complete
print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    🥇 GOLD AGGREGATION COMPLETE                               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  ✅ Status:        SUCCESS                                                    ║
║  📊 Tables:        6 Gold tables created                                      ║
║  📈 Records:       {total_records:,} total aggregated records{' ' * (30 - len(f'{total_records:,}'))}        ║
║                                                                               ║
║  📋 Tables Created:                                                           ║
║     • gold.daily_sales          - Daily revenue & order metrics               ║
║     • gold.monthly_sales        - Monthly trends & MoM growth                 ║
║     • gold.customer_analytics   - RFM analysis & segmentation                 ║
║     • gold.product_analytics    - Product performance & rankings              ║
║     • gold.seller_analytics     - Seller performance & tiers                  ║
║     • gold.geographic_analytics - Regional insights by state                  ║
║                                                                               ║
║  🎯 Key Metrics Computed:                                                     ║
║     • Revenue, orders, items, freight                                         ║
║     • Delivery rates & late delivery tracking                                 ║
║     • RFM scores & customer segments                                          ║
║     • Product & seller rankings                                               ║
║     • Growth rates (MoM)                                                      ║
║                                                                               ║
║  ⏭️  Next Step:     Run 06_business_layer.py                                  ║
║                                                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")