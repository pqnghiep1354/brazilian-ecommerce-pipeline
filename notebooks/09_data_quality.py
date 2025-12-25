# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 09 - Data Quality Framework
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Implement comprehensive data quality checks
# MAGIC
# MAGIC ### 🎯 Data Quality Dimensions:
# MAGIC 1. **Completeness** - No unexpected nulls
# MAGIC 2. **Uniqueness** - No duplicate keys  
# MAGIC 3. **Validity** - Values within expected ranges
# MAGIC 4. **Consistency** - Cross-table integrity
# MAGIC 5. **Timeliness** - Data freshness

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
from datetime import datetime
import uuid

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,DQConfig
class DQConfig:
    CATALOG = "brazilian_ecommerce"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    BUSINESS_SCHEMA = "business"
    
    # Các ngưỡng cho phép
    NULL_THRESHOLD = 0.05       # Tối đa 5% Null
    DUPLICATE_THRESHOLD = 0.01  # Tối đa 1% Trùng

config = DQConfig()

# COMMAND ----------

class DataQualityEngine:
    def __init__(self):
        self.results = []
        self.run_id = str(uuid.uuid4())
        
    def log_result(self, table_name, check_name, check_type, column=None, 
                   passed=True, expected=None, actual=None, records_checked=0, records_failed=0):
        result = {
            "run_id": self.run_id, "table_name": table_name, "column_name": column,
            "check_name": check_name, "check_type": check_type, "passed": passed,
            "expected": expected, "actual": actual, "records_checked": int(records_checked),
            "records_failed": int(records_failed), "created_at": datetime.now()
        }
        self.results.append(result)
        status = "✅" if passed else "❌"
        print(f"  {status} {check_name:<20} | Thực tế: {actual:<12} (Kỳ vọng: {expected})")
        
    def get_summary(self):
        total = len(self.results)
        passed_list = [r for r in self.results if r["passed"]]
        passed = len(passed_list)
        return {
            "total": total, 
            "passed": passed, 
            "failed": total - passed,
            "pass_rate": (passed/total*100) if total > 0 else 0
        }

dq = DataQualityEngine()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completeness Checks

# COMMAND ----------

# DBTITLE 1,Completeness Check (Kiểm tra Null)
def check_completeness(schema_table, columns):
    full_path = f"{config.CATALOG}.{schema_table}"
    print(f"\n📊 COMPLETENESS: {full_path}")
    try:
        df = spark.table(full_path)
        total = df.count()
        for c in columns:
            if c in df.columns:
                nulls = df.filter(F.col(c).isNull()).count()
                pct = nulls/total if total > 0 else 0
                dq.log_result(full_path, f"null_{c}", "completeness", c, 
                             pct <= config.NULL_THRESHOLD, f"<={config.NULL_THRESHOLD*100}%", f"{pct*100:.2f}%", total, nulls)
    except Exception as e: print(f"  ⚠️ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uniqueness Checks

# COMMAND ----------

# DBTITLE 1,Uniqueness Check (Kiểm tra Trùng lặp)
def check_uniqueness(schema_table, keys):
    full_path = f"{config.CATALOG}.{schema_table}"
    print(f"\n🔑 UNIQUENESS: {full_path}")
    try:
        df = spark.table(full_path)
        total = df.count()
        distinct_count = df.select(*keys).distinct().count()
        dups = total - distinct_count
        pct = dups/total if total > 0 else 0
        dq.log_result(full_path, "unique_key", "uniqueness", ",".join(keys),
                     pct <= config.DUPLICATE_THRESHOLD, f"<={config.DUPLICATE_THRESHOLD*100}%", f"{pct*100:.2f}%", total, dups)
    except Exception as e: print(f"  ⚠️ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validity Checks

# COMMAND ----------

# DBTITLE 1,Validity Check (Kiểm tra Tính hợp lệ)
def check_validity(schema_table, column, valid_values=None, min_val=None, max_val=None):
    full_table = f"{config.CATALOG}.{schema_table}"
    print(f"\n✅ VALIDITY: {full_table}.{column}")
    try:
        df = spark.table(full_table)
        total = df.count()
        if valid_values:
            invalid = df.filter(~col(column).isin(valid_values) & col(column).isNotNull()).count()
            expected = f"trong {valid_values}"
        elif min_val is not None and max_val is not None:
            invalid = df.filter(~col(column).between(min_val, max_val) & col(column).isNotNull()).count()
            expected = f"[{min_val}, {max_val}]"
        else:
            invalid = df.filter(col(column) < 0).count()
            expected = ">= 0"
        dq.log_result(full_table, f"valid_{column}", "validity", column, invalid/total <= 0.01, expected, f"{invalid} lỗi", total, invalid)
    except Exception as e:
        print(f"  ⚠️ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity

# COMMAND ----------

# DBTITLE 1,Referential Integrity (Khóa ngoại)
def check_fk(child_table, child_col, parent_table, parent_col):
    c_path, p_path = f"{config.CATALOG}.{child_table}", f"{config.CATALOG}.{parent_table}"
    print(f"\n🔗 FK Integrity: {child_table} -> {parent_table}")
    try:
        child_df = spark.table(c_path).select(child_col).distinct().filter(F.col(child_col).isNotNull())
        parent_df = spark.table(p_path).select(parent_col).distinct()
        orphans = child_df.join(parent_df, child_df[child_col] == parent_df[parent_col], "left_anti").count()
        total = child_df.count()
        pct = orphans/total if total > 0 else 0
        dq.log_result(c_path, f"fk_{child_col}", "consistency", child_col, pct <= 0.01, "0% orphans", f"{pct*100:.2f}%", total, orphans)
    except Exception as e: print(f"  ⚠️ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Run Execution (UC Synchronized)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Chạy kiểm tra tính đầy đủ
check_completeness(f"{config.SILVER_SCHEMA}.orders", ["order_id", "customer_id"])
check_completeness(f"{config.SILVER_SCHEMA}.order_items", ["order_id", "product_id", "price"])

# COMMAND ----------

# DBTITLE 1,Chạy kiểm tra tính duy nhất
check_uniqueness(f"{config.SILVER_SCHEMA}.orders", ["order_id"])
check_uniqueness(f"{config.SILVER_SCHEMA}.customers", ["customer_id"])

# COMMAND ----------

# DBTITLE 1,Chạy kiểm tra tính hợp lệ
# 3. Chạy kiểm tra tính hợp lệ
check_validity(f"{config.SILVER_SCHEMA}.orders", "order_status", 
               valid_values=["delivered","shipped","processing","canceled","unavailable","invoiced","created","approved"])
check_validity(f"{config.SILVER_SCHEMA}.order_items", "price")

# COMMAND ----------

# DBTITLE 1,Chạy kiểm tra khóa ngoại
check_fk(f"{config.SILVER_SCHEMA}.orders", "customer_id", f"{config.SILVER_SCHEMA}.customers", "customer_id")
check_fk(f"{config.SILVER_SCHEMA}.order_items", "order_id", f"{config.SILVER_SCHEMA}.orders", "order_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

# DBTITLE 1,Save DQ Results to Unity Catalog
def save_dq_results():
    if dq.results:
        # Định nghĩa Schema tường minh cho bảng giám sát
        schema = StructType([
            StructField("run_id", StringType(), False), StructField("table_name", StringType(), False),
            StructField("column_name", StringType(), True), StructField("check_name", StringType(), False),
            StructField("check_type", StringType(), False), StructField("passed", BooleanType(), False),
            StructField("expected", StringType(), True), StructField("actual", StringType(), True),
            StructField("records_checked", LongType(), True), StructField("records_failed", LongType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        
        results_df = spark.createDataFrame(dq.results, schema) \
                          .withColumn("_updated_at", F.current_timestamp())
        
        target_table = f"{config.CATALOG}.{config.BUSINESS_SCHEMA}.dq_monitoring"
        results_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        print(f"\n✅ DQ Results saved to: {target_table}")

save_dq_results()

# COMMAND ----------

# DBTITLE 1,Print Summary Report
# Print Summary Report
summary = dq.get_summary()
print(f"""
╔══════════════════════════════════════════════════════════════╗
║                 📊 DATA QUALITY REPORT                       ║
╠══════════════════════════════════════════════════════════════╣
║  Tổng số kiểm tra:  {summary['total']:<36}      ║
║  ✅ Đạt:            {summary['passed']:<36}     ║
║  ❌ Lỗi:            {summary['failed']:<36}     ║
║  Tỷ lệ đạt:         {summary['pass_rate']:.1f}%{' '*33}   ║
╚══════════════════════════════════════════════════════════════╝
""")

failed = [r for r in dq.results if not r["passed"]]
if failed:
    print("\n❌ CÁC KIỂM TRA KHÔNG ĐẠT:")
    for r in failed:
        print(f"  • {r['table_name']}.{r['column_name']}: {r['check_name']} (Thực tế: {r['actual']})")