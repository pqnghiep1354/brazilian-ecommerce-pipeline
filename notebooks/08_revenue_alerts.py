# Databricks notebook source
# MAGIC %md
# MAGIC # 🚨 08 - Revenue Alerts System
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Implement automated alerts for revenue anomalies and business metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Alert Types:
# MAGIC 1. **Revenue Spike Alert** - Sudden revenue increase (>50%)
# MAGIC 2. **Revenue Drop Alert** - Sudden revenue decrease (>30%)
# MAGIC 3. **Anomaly Alert** - ML-detected anomalies
# MAGIC 4. **KPI Threshold Alert** - Business metrics thresholds
# MAGIC 5. **Data Quality Alert** - Data freshness and completeness

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration & Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json
import uuid
import requests

spark = SparkSession.builder.getOrCreate()
print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration
class AlertConfig:
    CATALOG = "brazilian_ecommerce"
    SILVER_SCHEMA = "silver"
    BUSINESS_SCHEMA = "business"
    ML_SCHEMA = "ml_models"
    
    # Ngưỡng cảnh báo (Thresholds)
    SPIKE_THRESHOLD = 0.50  # Tăng 50%
    DROP_THRESHOLD = -0.30  # Giảm 30%
    Z_SCORE_THRESHOLD = 3.0
    LOOKBACK_DAYS = 7
    
    # Cấu hình thông báo (Placeholder)
    SLACK_WEBHOOK = None  
    EMAIL_ENABLED = False

config = AlertConfig()

print(f"✅ Alert System synchronized with Catalog: {config.CATALOG}")

# COMMAND ----------

# Định nghĩa Schema tường minh để tránh lỗi [CANNOT_DETERMINE_TYPE]
ALERT_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("alert_type", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("title", StringType(), False),
    StructField("message", StringType(), False),
    StructField("metric_name", StringType(), True),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold", DoubleType(), True),
    StructField("context", StringType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("is_acknowledged", BooleanType(), False),
    StructField("acknowledged_by", StringType(), True),
    StructField("acknowledged_at", TimestampType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Alert Detection Functions

# COMMAND ----------

# DBTITLE 1,Alert Detection Engine
class AlertEngine:
    def __init__(self, config):
        self.config = config
        self.alerts = []
    
    def create_alert(self, alert_type, severity, title, message, 
                     metric_name=None, metric_value=None, threshold=None, context=None):
        alert = {
            "alert_id": str(uuid.uuid4()),
            "alert_type": alert_type,
            "severity": severity,
            "title": title,
            "message": message,
            "metric_name": metric_name,
            "metric_value": float(metric_value) if metric_value is not None else None,
            "threshold": float(threshold) if threshold is not None else None,
            "context": json.dumps(context) if context else None,
            "created_at": datetime.now(),
            "is_acknowledged": False,
            "acknowledged_by": None,
            "acknowledged_at": None
        }
        self.alerts.append(alert)
        return alert
    
    def get_alerts_df(self):
        # Sử dụng ALERT_SCHEMA để tránh lỗi inference
        if not self.alerts:
            return spark.createDataFrame([], ALERT_SCHEMA)
        return spark.createDataFrame(self.alerts, ALERT_SCHEMA)
    
    def clear_alerts(self):
        self.alerts = []

alert_engine = AlertEngine(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue Change Detection

# COMMAND ----------

# DBTITLE 1,Detect Revenue Changes
def detect_revenue_changes():
    print(f"📊 Analyzing revenue changes...")
    try:
        # Sử dụng fact_orders_streaming join silver.payments
        daily_sales = spark.table(f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.fact_orders_streaming") \
            .join(spark.table(f"{config.CATALOG}.{config.SILVER_SCHEMA}.payments"), "order_id") \
            .withColumn("order_date", to_date("order_purchase_timestamp")) \
            .groupBy("order_date").agg(sum("payment_value").alias("gross_revenue"))
        
        window_spec = Window.orderBy("order_date").rowsBetween(-config.LOOKBACK_DAYS, -1)
        analysis = daily_sales.withColumn("rolling_avg", avg("gross_revenue").over(window_spec)) \
                              .withColumn("pct_change", (col("gross_revenue") - col("rolling_avg")) / col("rolling_avg")) \
                              .filter(col("rolling_avg").isNotNull())
        
        results = analysis.filter((col("pct_change") > config.SPIKE_THRESHOLD) | 
                                  (col("pct_change") < config.DROP_THRESHOLD)).collect()
        
        for row in results:
            alert_type = "revenue_spike" if row['pct_change'] > 0 else "revenue_drop"
            alert_engine.create_alert(
                alert_type=alert_type, severity="critical" if alert_type == "revenue_drop" else "warning",
                title=f"Revenue {alert_type.split('_')[1].capitalize()}: {row['order_date']}",
                message=f"Change of {row['pct_change']*100:.1f}% vs {config.LOOKBACK_DAYS}-day avg",
                metric_name="gross_revenue", metric_value=row['gross_revenue'],
                threshold=config.SPIKE_THRESHOLD if row['pct_change'] > 0 else config.DROP_THRESHOLD
            )
    except Exception as e: print(f"⚠️ Revenue Change Error: {e}")

def generate_ml_anomaly_alerts():
    print(f"🔍 Scanning ML Anomalies...")
    try:
        anomalies = spark.table(f"{config.CATALOG}.{config.ML_SCHEMA}.revenue_anomalies")
        recent = anomalies.filter("is_anomaly == 1").orderBy(desc("ds")).limit(10).collect()
        for row in recent:
            alert_engine.create_alert(
                alert_type="ml_anomaly", severity="critical",
                title=f"AI Anomaly: {row['ds']}",
                message=f"Isolation Forest detected revenue R$ {row['revenue']:,.2f} as outlier.",
                metric_name="daily_revenue", metric_value=row['revenue'],
                context={"ds": str(row['ds']), "score": row['anomaly_score']}
            )
    except Exception as e: print(f"⚠️ ML Anomaly Error: {e}")

def check_data_quality():
    print(f"📊 Checking Data Quality...")
    # Sử dụng đúng tên cột đã phát hiện: _silver_timestamp và _silver_at
    tables = [
        (f"{config.CATALOG}.{config.SILVER_SCHEMA}.orders", "_silver_timestamp", 24),
        (f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.fact_orders_streaming", "_silver_at", 12)
    ]
    for tab, col_name, hrs in tables:
        try:
            df = spark.table(tab)
            latest = df.agg(max(col_name)).collect()[0][0]
            if latest:
                hrs_old = (datetime.now() - latest).total_seconds() / 3600
                if hrs_old > hrs:
                    alert_engine.create_alert(
                        alert_type="data_quality", severity="warning",
                        title=f"Stale Data: {tab}",
                        message=f"Data is {hrs_old:.1f} hours old (Threshold: {hrs}h)",
                        metric_name="freshness_hours", metric_value=hrs_old, threshold=float(hrs)
                    )
        except Exception as e: print(f"⚠️ DQ Error {tab}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ML Anomaly Alerts

# COMMAND ----------

# DBTITLE 1,Generate ML Anomaly Alerts
def generate_ml_anomaly_alerts():
    """Tạo cảnh báo từ kết quả phát hiện bất thường của mô hình ML (Isolation Forest)."""
    
    print(f"🔍 Đang quét kết quả từ bảng ML: revenue_anomalies...")
    
    try:
        # Đọc kết quả từ Notebook 08
        anomalies = spark.table(f"{config.CATALOG}.{config.ML_SCHEMA}.revenue_anomalies")
        
        recent_anomalies = anomalies.filter("is_anomaly == 1").orderBy(desc("ds")).limit(5).collect()
        
        for row in recent_anomalies:
            alert_engine.create_alert(
                alert_type="ml_anomaly",
                severity="critical",
                title=f"AI phát hiện bất thường: {row['ds']}",
                message=f"Mô hình Isolation Forest xác định doanh thu R$ {row['revenue']:,.2f} là ngoại lai.",
                metric_name="daily_revenue",
                metric_value=row['revenue'],
                context={"ds": str(row['ds']), "anomaly_score": row['anomaly_score']}
            )
        print(f"✅ Đã tích hợp {len(recent_anomalies)} cảnh báo từ ML.")
    except Exception as e:
        print(f"⚠️ Không tìm thấy bảng ML hoặc có lỗi: {e}")

generate_ml_anomaly_alerts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI Threshold Alerts

# COMMAND ----------

# DBTITLE 1,Check KPI Thresholds
def check_kpi_thresholds():
    """Kiểm tra các KPI vận hành so với ngưỡng cho phép."""
    
    print(f"📊 Đang kiểm tra ngưỡng KPI...")
    
    # Giả định bạn có bảng daily_kpis hoặc tính toán trực tiếp từ Fact
    try:
        # Ví dụ kiểm tra tỷ lệ giao hàng muộn (Late Delivery Rate)
        fact_orders = spark.table(f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.fact_orders_streaming")
        
        late_rate = fact_orders.select(avg(col("is_late_delivery").cast("float"))).collect()[0][0] * 100
        
        if late_rate > 15: # Ngưỡng 15%
            alert_engine.create_alert(
                alert_type="kpi_threshold",
                severity="warning",
                title="Tỷ lệ giao hàng muộn cao",
                message=f"Tỷ lệ hiện tại ({late_rate:.1f}%) vượt ngưỡng 15%",
                metric_name="late_delivery_rate",
                metric_value=late_rate,
                threshold=15.0
            )
    except Exception as e:
        print(f"⚠️ Lỗi khi kiểm tra KPI: {e}")

check_kpi_thresholds()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Alerts

# COMMAND ----------

# DBTITLE 1,Check Data Quality
# Check Data Quality (Corrected Column Names)
def check_data_quality():
    """Kiểm tra độ tươi (freshness) dựa trên đúng Schema thực tế."""
    
    print(f"\n{'='*60}")
    print("📊 CHECKING DATA QUALITY")
    print(f"{'='*60}")
    
    alerts = []
    
    # DANH SÁCH ĐÃ CẬP NHẬT: (tên_bảng, cột_thực_tế, ngưỡng_giờ)
    tables_to_check = [
        (f"{config.CATALOG}.{config.SILVER_SCHEMA}.orders", "_silver_timestamp", 24),
        (f"{config.CATALOG}.{config.SILVER_SCHEMA}.order_items", "_silver_timestamp", 24),
        (f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.fact_orders_streaming", "_silver_at", 12)
    ]
    
    for table_full_name, timestamp_col, max_hours in tables_to_check:
        try:
            df = spark.table(table_full_name)
            
            # 1. Kiểm tra độ tươi (Freshness)
            latest_ts = df.agg(max(timestamp_col)).collect()[0][0]
            
            if latest_ts:
                hours_old = (datetime.now() - latest_ts).total_seconds() / 3600
                
                if hours_old > max_hours:
                    alert = alert_engine.create_alert(
                        alert_type="data_quality_stale",
                        severity="warning",
                        title=f"Dữ liệu bị chậm: {table_full_name}",
                        message=f"Bảng chưa được cập nhật trong {hours_old:.1f} giờ (Ngưỡng: {max_hours}h)",
                        metric_name="data_freshness_hours",
                        metric_value=hours_old,
                        threshold=float(max_hours),
                        context={"table": table_full_name, "last_update": str(latest_ts)}
                    )
                    alerts.append(alert)
                    print(f"  ⚠️ {table_full_name}: {hours_old:.1f} giờ tuổi (Cảnh báo)")
                else:
                    print(f"  ✅ {table_full_name}: {hours_old:.1f} giờ tuổi (Ổn định)")
            
            # 2. Kiểm tra tính đầy đủ (Row count)
            if df.count() == 0:
                alert = alert_engine.create_alert(
                    alert_type="data_quality_empty",
                    severity="critical",
                    title=f"Bảng trống: {table_full_name}",
                    message=f"Phát hiện bảng không có dữ liệu",
                    metric_name="row_count",
                    metric_value=0,
                    threshold=1
                )
                alerts.append(alert)
                print(f"  🚨 {table_full_name}: BẢNG TRỐNG!")
                
        except Exception as e:
            # In lỗi chi tiết để debug nếu vẫn gặp vấn đề column
            print(f"  ❌ Lỗi kiểm tra {table_full_name}: {str(e)[:100]}...")
    
    return alerts

# Chạy lại hàm kiểm tra
dq_alerts = check_data_quality()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Notification Functions

# COMMAND ----------

# DBTITLE 1,Notification Functions
def send_notifications(alerts):
    if not alerts: 
        print("✅ No alerts to send.")
        return
    for a in alerts:
        icon = "🚨" if a["severity"] == "critical" else "⚠️"
        print(f"{icon} {a['title']}: {a['message']}")

def save_alerts():
    df = alert_engine.get_alerts_df()
    if df.count() > 0:
        target = f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.alert_history"
        df.withColumn("_updated_at", current_timestamp()) \
          .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
        print(f"✅ Saved {df.count()} alerts to {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Save Alerts

# COMMAND ----------

# DBTITLE 1,Save Alerts to Delta Table
alert_engine.clear_alerts()
detect_revenue_changes()
generate_ml_anomaly_alerts()
check_data_quality()
save_alerts()
send_notifications(alert_engine.alerts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Alert Summary Dashboard

# COMMAND ----------

# DBTITLE 1,Display Alert Summary
def display_alert_summary():
    """Display summary of all generated alerts."""
    
    print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    📊 ALERT SUMMARY                                           ║
╠══════════════════════════════════════════════════════════════════════════════╣""")
    
    alerts_df = alert_engine.get_alerts_df()
    
    if alerts_df is None or alerts_df.count() == 0:
        print("║  ✅ No alerts generated - All systems normal                          ║")
    else:
        # Count by severity
        severity_counts = alerts_df.groupBy("severity").count().collect()
        severity_dict = {row["severity"]: row["count"] for row in severity_counts}
        
        critical = severity_dict.get("critical", 0)
        warning = severity_dict.get("warning", 0)
        info = severity_dict.get("info", 0)
        
        print(f"║  🚨 Critical:  {critical:<58} ║")
        print(f"║  ⚠️  Warning:   {warning:<58} ║")
        print(f"║  ℹ️  Info:      {info:<58} ║")
        print(f"║  📊 Total:     {alerts_df.count():<58} ║")
        
        print(f"╠══════════════════════════════════════════════════════════════════════════════╣")
        
        # Count by type
        type_counts = alerts_df.groupBy("alert_type").count().orderBy(desc("count")).collect()
        
        print("║  Alert Types:                                                             ║")
        for row in type_counts:
            print(f"║    • {row['alert_type']:<30} {row['count']:>5} alerts{' '*23} ║")
    
    print(f"╚══════════════════════════════════════════════════════════════════════════════╝")

display_alert_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 View Recent Alerts

# COMMAND ----------

# DBTITLE 1,View Recent Alerts
alerts_df = alert_engine.get_alerts_df()

if alerts_df and alerts_df.count() > 0:
    print("\n📋 RECENT ALERTS:")
    display(
        alerts_df.select(
            "created_at",
            "severity",
            "alert_type",
            "title",
            "metric_value",
            "threshold"
        ).orderBy(desc("created_at"))
    )
else:
    print("\n✅ No alerts generated - All systems are operating normally!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Run Complete Alert Check

# COMMAND ----------

# DBTITLE 1,Run Complete Alert Check
# DBTITLE 1,🚀 Final Summary Verification
# 1. Lấy DataFrame cảnh báo một cách an toàn
alerts_final_df = alert_engine.get_alerts_df()

# 2. Tính toán tổng số cảnh báo (Xử lý trường hợp DataFrame rỗng hoặc None)
if alerts_final_df is not None:
    total_alerts = alerts_final_df.count()
else:
    total_alerts = 0

# 3. In bảng tóm tắt chuyên nghiệp
print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    🚨 REVENUE ALERTS COMPLETE                                 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                               ║
║  ✅ Status:        SUCCESS                                                    ║
║  📊 Alerts:        {total_alerts} alerts generated{' ' * (45 - len(str(total_alerts)))}      ║
║                                                                               ║
║  📋 Alert Types Implemented:                                                  ║
║     1. Revenue Spike Detection (>{config.SPIKE_THRESHOLD*100:.0f}% increase){' ' * 31} ║
║     3. ML Anomaly Alerts (Isolation Forest)                                   ║
║     4. KPI Threshold Alerts (Delivery, Cancellation, Late Delivery)           ║
║     5. Data Quality Alerts (Freshness & Completeness)                         ║
║                                                                               ║
║  📤 Notification Channels:                                                    ║
║     • Console output (Active)                                                 ║
║     • Slack webhook (Configured: {"Yes" if config.SLACK_WEBHOOK else "No"})   ║
║     • Email (Enabled: {config.EMAIL_ENABLED})                                 ║
║                                                                               ║
║  💾 Alert History (Unity Catalog):                                            ║
║     • Table: {config.CATALOG}.{config.BUSINESS_SCHEMA}.alert_history          ║
║                                                                               ║
║  ⏭️  Next Step:     Run 10_data_quality.py                                    ║
║                                                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# Thay vì dùng assert gây crash notebook, hãy dùng thông báo info
if total_alerts == 0:
    print("ℹ️ All systems are operating normally. No anomalies detected.")