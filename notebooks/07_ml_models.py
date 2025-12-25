# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 07 - Machine Learning Models
# MAGIC
# MAGIC ## Brazilian E-Commerce Pipeline
# MAGIC
# MAGIC **Objective:** Build ML models for customer segmentation, revenue forecasting, and anomaly detection
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 ML Models:
# MAGIC 1. **Customer Segmentation** - RFM clustering with K-Means
# MAGIC 2. **Revenue Forecasting** - Time series prediction
# MAGIC 3. **Anomaly Detection** - Revenue anomaly detection
# MAGIC 4. **Product Recommendation** - Association rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Configuration & Setup

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ML Libraries
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# MLflow
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

# Python ML
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler as SklearnScaler
from sklearn.cluster import KMeans as SklearnKMeans
from mlflow.models.signature import infer_signature
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

spark = SparkSession.builder.getOrCreate()
print("✅ Libraries imported")

# COMMAND ----------

# DBTITLE 1,Configuration
class MLConfig:
    """Configuration for ML models."""
    
    CATALOG = "brazilian_ecommerce"
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    BUSINESS_SCHEMA = "business"
    SCHEMA_ML = "ml_models"

    # Unity Catalog Volume Path
    VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/source_data"
    RAW_PATH = f"{VOLUME_PATH}/raw"
    CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints"
    SCHEMA_PATH = f"{VOLUME_PATH}/_schemas"
    
    
    
    # Lấy dữ liệu từ bảng Fact orders mà bạn đã xây dựng ở các bước trước
    TABLE_FACT_ORDERS = f"{CATALOG}.{BUSINESS_SCHEMA}.fact_orders_streaming"
    TABLE_SILVER_ITEMS = f"{CATALOG}.{SILVER_SCHEMA}.order_items"

    # MLflow settings
    CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
    EXPERIMENT_PATH = f"/Users/{CURRENT_USER}/brazilian_ecommerce_ml_v1"
    
    # Model parameters
    CUSTOMER_CLUSTERS = 5
    FORECAST_HORIZON  = 30  # Dự báo doanh thu 30 ngày tới
    ANOMALY_CONTAMINATION = 0.05 # Tỷ lệ ngoại lệ dự kiến (5%)

config = MLConfig()

# --- HÀM BỔ TRỢ (HELPERS) ---
def get_table_path(table_name, schema=config.SCHEMA_ML):
    """Trả về full path: catalog.schema.table"""
    return f"{config.CATALOG}.{schema}.{table_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup MLflow

# COMMAND ----------

# DBTITLE 1,Setup MLflow Experiment
# Thiết lập MLflow sử dụng Unity Catalog làm Model Registry
mlflow.set_registry_uri("databricks-uc") 

# Thiết lập Tracking URI (bắt buộc cho Databricks Connect)
mlflow.set_tracking_uri("databricks")

try:
    # Đảm bảo đường dẫn experiment là tuyệt đối
    mlflow.set_experiment(config.EXPERIMENT_PATH)
    print(f"✅ MLflow đã sẵn sàng với Unity Catalog.")
    print(f"🧪 Experiment Path: {config.EXPERIMENT_PATH}")
except Exception as e:
    # Nếu lỗi do không có quyền tạo folder, thử tạo experiment thủ công
    print(f"⚠️ Đang khởi tạo Experiment mới...")
    mlflow.create_experiment(config.EXPERIMENT_PATH)
    mlflow.set_experiment(config.EXPERIMENT_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 👥 Model 1: Customer Segmentation (RFM Clustering)

# COMMAND ----------

# DBTITLE 1,Prepare Customer RFM Data
def prepare_rfm_data():
    print(f"📊 Đang chuẩn bị dữ liệu RFM...")
    
    # 1. Đọc bảng Fact (Thông tin đơn hàng & Khách hàng)
    # Nguồn: brazilian_ecommerce.business.fact_orders_streaming
    fact_df = spark.table(config.TABLE_FACT_ORDERS)
    
    # 2. Đọc bảng Payments từ lớp Silver
    # Sử dụng config.SILVER_SCHEMA
    payments_table = f"{config.CATALOG}.{config.SILVER_SCHEMA}.payments"
    print(f"🔗 Kết hợp với dữ liệu thanh toán từ: {payments_table}")
    
    payments_df = spark.table(payments_table)
    
    # 3. Tổng hợp giá trị thanh toán theo từng order_id
    # Tránh tình trạng duplicate dòng khi 1 đơn hàng có nhiều phương thức thanh toán
    order_values = payments_df.groupBy("order_id").agg(
        sum("payment_value").alias("total_order_value")
    )
    
    # 4. Join Fact với Payments
    joined_df = fact_df.join(order_values, "order_id", "inner")
    
    # 5. Xác định mốc thời gian gần nhất để tính Recency
    max_date = joined_df.agg(max("order_purchase_timestamp")).collect()[0][0]
    
    # 6. Tính toán 3 chỉ số RFM
    rfm_df = joined_df.groupBy("customer_id").agg(
        datediff(lit(max_date), max("order_purchase_timestamp")).alias("recency"),
        count("order_id").alias("frequency"),
        sum("total_order_value").alias("monetary")
    ).filter("monetary > 0").dropna()
    
    print(f"✅ Chuẩn bị xong {rfm_df.count():,} khách hàng.")
    return rfm_df

# Thực thi chuẩn bị dữ liệu
rfm_raw_data = prepare_rfm_data()

# COMMAND ----------

# DBTITLE 1,Train Customer Segmentation Model
def run_segmentation_pipeline_sklearn(rfm_spark_df):
    """
    Huấn luyện mô hình phân cụm khách hàng bằng Scikit-Learn,
    tích hợp MLflow Signature và lưu kết quả vào Unity Catalog.
    """
    print(f"\n{'='*60}")
    print(f"🤖 ĐANG HUẤN LUYỆN PHÂN CỤM (K={config.CUSTOMER_CLUSTERS})")
    print(f"{'='*60}")
    
    # 1. Chuyển đổi sang Pandas để xử lý trên Driver (Tránh lỗi Whitelist của Spark ML)
    print("⏳ Đang chuyển đổi dữ liệu sang Pandas...")
    rfm_pdf = rfm_spark_df.toPandas()
    
    # Xác định các cột đặc trưng (Features)
    feature_cols = ["recency", "frequency", "monetary"]
    X = rfm_pdf[feature_cols]
    
    # 2. Khởi chạy MLflow Tracking
    with mlflow.start_run(run_name="customer_segmentation_rfm_sklearn") as run:
        print("⏳ Đang chuẩn hóa dữ liệu và huấn luyện mô hình...")
        
        # 3. Chuẩn hóa dữ liệu (Scaling)
        scaler = SklearnScaler()
        X_scaled = scaler.fit_transform(X)
        
        # 4. Huấn luyện mô hình KMeans
        kmeans = SklearnKMeans(
            n_clusters=config.CUSTOMER_CLUSTERS, 
            random_state=42, 
            n_init=10
        )
        rfm_pdf['segment'] = kmeans.fit_predict(X_scaled)
        
        # 5. TẠO MLFLOW SIGNATURE (Chuyên nghiệp hóa MLOps)
        # Signature giúp định nghĩa rõ Input (recency, freq, mon) và Output (segment)
        signature = infer_signature(X, rfm_pdf[['segment']])
        
        # 6. Log tham số và mô hình vào MLflow
        mlflow.log_param("n_clusters", config.CUSTOMER_CLUSTERS)
        mlflow.log_param("algorithm", "KMeans-Sklearn")
        
        mlflow.sklearn.log_model(
            sk_model=kmeans,
            artifact_path="customer_segmentation_model",
            signature=signature  # Gắn signature vào model
        )
        
        # 7. Gán nhãn phân khúc khách hàng
        print("⏳ Đang gán nhãn phân khúc và chuẩn bị lưu dữ liệu...")
        segment_map = {
            0: "Champions",
            1: "Loyal Customers",
            2: "Potential Loyalists",
            3: "At Risk",
            4: "Lost Customers"
        }
        rfm_pdf["segment_name"] = rfm_pdf["segment"].map(segment_map).fillna("Others")
        
        # 8. Chuyển ngược lại Spark DataFrame và lưu vào Unity Catalog
        final_spark_df = spark.createDataFrame(rfm_pdf) \
                              .withColumn("_updated_at", current_timestamp())
        
        output_table = f"{config.CATALOG}.{config.SCHEMA_ML}.customer_segments"
        
        final_spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(output_table)
            
        print(f"✅ THÀNH CÔNG!")
        print(f"📊 MLflow Run ID: {run.info.run_id}")
        print(f"💾 Kết quả đã lưu tại: {output_table}")
        
        return final_spark_df

# --- THỰC THI ---
if rfm_raw_data is not None:
    customer_segments = run_segmentation_pipeline_sklearn(rfm_raw_data)
    
    # Hiển thị thống kê nhanh
    display(customer_segments.groupBy("segment_name").count().orderBy(desc("count")))

# COMMAND ----------

# DBTITLE 1,Save Customer Segments
def save_customer_segments(predictions):
    """Lưu phân khúc khách hàng vào Unity Catalog."""
    
    print(f"\n{'='*60}")
    print("💾 ĐANG LƯU KẾT QUẢ PHÂN KHÚC KHÁCH HÀNG")
    print(f"{'='*60}")
    
    # 1. Tạo cột segment_name (Nếu trong hàm train chưa làm)
    # Nếu hàm run_segmentation_pipeline_sklearn đã tạo rồi thì bước này có thể bỏ qua
    if "segment_name" not in predictions.columns:
        predictions = predictions.withColumn("segment_name",
            when(col("segment") == 0, "Champions")
            .when(col("segment") == 1, "Loyal Customers")
            .when(col("segment") == 2, "Potential Loyalists")
            .when(col("segment") == 3, "At Risk")
            .otherwise("Lost Customers"))
    
    # 2. Thêm timestamp và chọn các cột cần thiết
    final_df = predictions.select(
    "customer_id", "recency", "frequency", 
    "monetary", "segment", "segment_name"
).withColumn("_updated_at", current_timestamp())
    
    # 3. Định nghĩa tên bảng theo Unity Catalog (catalog.schema.table)
    # Sử dụng hàm helper của bạn hoặc viết trực tiếp
    output_table = f"{config.CATALOG}.{config.SCHEMA_ML}.customer_segments"
    
    # 4. Ghi dữ liệu (Unity Catalog sẽ tự quản lý đường dẫn vật lý)
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(output_table)
    
    count = final_df.count()
    print(f"  ✅ Đã lưu {count:,} khách hàng vào bảng: {output_table}")
    
    return final_df

customer_segment_df = save_customer_segments(customer_segments)
display(customer_segment_df.orderBy(desc("customer_id")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Model 2: Revenue Forecasting

# COMMAND ----------

# DBTITLE 1,Prepare Time Series Data
def prepare_revenue_data():
    """Chuẩn bị dữ liệu doanh thu theo ngày kèm các đặc trưng thời gian."""
    print(f"📊 Đang chuẩn bị dữ liệu chuỗi thời gian từ {config.TABLE_FACT_ORDERS}...")
    
    # Xác định bảng thanh toán trong Silver
    payments_table = f"{config.CATALOG}.{config.SILVER_SCHEMA}.payments"
    
    # Kết hợp Fact và Payments để tính doanh thu hàng ngày
    ts_spark_df = spark.table(config.TABLE_FACT_ORDERS) \
        .join(spark.table(payments_table), "order_id") \
        .withColumn("ds", to_date("order_purchase_timestamp")) \
        .groupBy("ds").agg(
            sum("payment_value").alias("y"),         # Doanh thu thực tế
            count("order_id").alias("total_orders")  # Số lượng đơn hàng
        ) \
        .withColumn("day_of_week", dayofweek("ds")) \
        .withColumn("month", month("ds")) \
        .withColumn("day_of_month", dayofmonth("ds")) \
        .orderBy("ds").dropna()
    
    # Chuyển sang Pandas để xử lý Sklearn (Tránh lỗi Whitelist)
    pdf = ts_spark_df.toPandas()
    print(f"✅ Đã chuẩn bị {len(pdf)} ngày dữ liệu.")
    return pdf

# Thực thi chuẩn bị dữ liệu
revenue_pdf = prepare_revenue_data()

# COMMAND ----------

# DBTITLE 1,Train Revenue Forecast Model
def train_revenue_model(pdf):
    """Huấn luyện mô hình GBR và log vào MLflow."""
    print(f"🤖 Đang huấn luyện Gradient Boosting Regressor...")
    
    # Định nghĩa Features và Label
    features = ["day_of_week", "month", "day_of_month", "total_orders"]
    X = pdf[features]
    y = pdf["y"]

    with mlflow.start_run(run_name="revenue_forecast_gbr") as run:
        # Chia dữ liệu Train/Test (80% quá khứ/20% hiện tại)
        split_idx = int(len(pdf) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        
        # Huấn luyện
        model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
        model.fit(X_train, y_train)
        
        # Dự báo và tạo Signature
        test_preds = model.predict(X_test)
        signature = infer_signature(X_test, test_preds)
        
        # Log MLflow
        mlflow.log_param("forecast_horizon", config.FORECAST_HORIZON)
        mae = mean_absolute_error(y_test, test_preds)
        mlflow.log_metric("mae", mae)
        
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="revenue_model",
            signature=signature
        )
        
        print(f"✅ Huấn luyện hoàn tất. MAE: R$ {mae:,.2f}")
        return model, X, run.info.run_id

# Thực thi huấn luyện
revenue_model, features_df, run_id = train_revenue_model(revenue_pdf)

# COMMAND ----------

# DBTITLE 1,Save Predictions to Unity Catalog
def save_forecast_results(model, pdf, features_df):
    """Lưu kết quả dự báo thực tế vào Unity Catalog."""
    print(f"💾 Đang lưu kết quả dự báo vào {config.SCHEMA_ML}...")
    
    # Dự báo cho toàn bộ tập dữ liệu
    pdf_results = pdf.copy()
    pdf_results['prediction'] = model.predict(features_df)
    
    # Chuyển về Spark DataFrame và thêm timestamp đồng bộ
    # Lưu ý: Sử dụng '_updated_at' để khớp với các bảng trước đó
    forecast_spark_df = spark.createDataFrame(pdf_results) \
                             .withColumn("_updated_at", current_timestamp())
    
    output_table = f"{config.CATALOG}.{config.SCHEMA_ML}.revenue_forecasts"
    
    # Ghi đè bảng và cấu hình tự động khớp schema
    forecast_spark_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(output_table)
        
    print(f"✅ Đã lưu kết quả tại: {output_table}")
    return forecast_spark_df

# Thực thi lưu trữ
forecast_final_df = save_forecast_results(revenue_model, revenue_pdf, features_df)

# Kiểm tra kết quả
display(forecast_final_df.orderBy(desc("ds")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Model 3: Anomaly Detection

# COMMAND ----------

# DBTITLE 1,Prepare Data for Anomaly Detection
def prepare_anomaly_data():
    """Chuẩn bị dữ liệu doanh thu và số lượng đơn hàng hàng ngày để phát hiện bất thường."""
    print(f"📊 Đang chuẩn bị dữ liệu từ {config.TABLE_FACT_ORDERS}...")
    
    payments_table = f"{config.CATALOG}.{config.SILVER_SCHEMA}.payments"
    
    # Kết hợp dữ liệu Fact và Payments
    ts_spark_df = spark.table(config.TABLE_FACT_ORDERS) \
        .join(spark.table(payments_table), "order_id") \
        .withColumn("ds", to_date("order_purchase_timestamp")) \
        .groupBy("ds").agg(
            sum("payment_value").alias("revenue"),
            count("order_id").alias("total_orders")
        ).orderBy("ds").dropna()
    
    # Chuyển sang Pandas để xử lý Sklearn
    pdf = ts_spark_df.toPandas()
    print(f"✅ Đã chuẩn bị {len(pdf)} ngày dữ liệu để phân tích bất thường.")
    return pdf

# Thực thi chuẩn bị dữ liệu
anomaly_raw_pdf = prepare_anomaly_data()

# COMMAND ----------

# DBTITLE 1,Train Anomaly Detection Model
def train_anomaly_model(pdf):
    """Huấn luyện mô hình Isolation Forest và log vào MLflow."""
    print(f"🤖 Đang huấn luyện Isolation Forest (Contamination={config.ANOMALY_CONTAMINATION})...")
    
    # Đặc trưng dùng để phát hiện bất thường: Doanh thu và Số lượng đơn hàng
    features = ["revenue", "total_orders"]
    X = pdf[features]
    
    with mlflow.start_run(run_name="revenue_anomaly_detection") as run:
        # Chuẩn hóa dữ liệu
        scaler = SklearnScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Huấn luyện Isolation Forest
        model = IsolationForest(
            contamination=config.ANOMALY_CONTAMINATION,
            random_state=42
        )
        
        # Dự báo: -1 là bất thường, 1 là bình thường
        pdf['anomaly_score'] = model.fit_predict(X_scaled)
        pdf['is_anomaly'] = pdf['anomaly_score'].apply(lambda x: 1 if x == -1 else 0)
        
        # Tạo Signature cho MLflow
        signature = infer_signature(X, pdf[['is_anomaly']])
        
        # Log MLflow
        mlflow.log_param("contamination", config.ANOMALY_CONTAMINATION)
        mlflow.log_metric("total_anomalies", pdf['is_anomaly'].sum())
        
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="anomaly_model",
            signature=signature
        )
        
        print(f"✅ Phát hiện {pdf['is_anomaly'].sum()} điểm bất thường.")
        return model, pdf, run.info.run_id

# Thực thi huấn luyện
anomaly_model, anomaly_results_pdf, anomaly_run_id = train_anomaly_model(anomaly_raw_pdf)

# COMMAND ----------

# DBTITLE 1,Save Anomaly Detection Results
def save_anomaly_results(pdf):
    """Lưu danh sách các điểm bất thường vào Unity Catalog."""
    print(f"💾 Đang lưu kết quả bất thường vào {config.SCHEMA_ML}...")
    
    # Chuyển về Spark DataFrame và thêm timestamp
    anomaly_spark_df = spark.createDataFrame(pdf) \
                            .withColumn("_updated_at", current_timestamp())
    
    output_table = f"{config.CATALOG}.{config.SCHEMA_ML}.revenue_anomalies"
    
    # Ghi đè bảng và cấu hình tự động khớp schema
    anomaly_spark_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(output_table)
        
    print(f"✅ Đã lưu kết quả tại: {output_table}")
    return anomaly_spark_df

# Thực thi lưu trữ
anomaly_final_df = save_anomaly_results(anomaly_results_pdf)

# Hiển thị các ngày có doanh thu bất thường
display(anomaly_final_df.filter("is_anomaly == 1").orderBy(desc("ds")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Model 4: Product Affinity Analysis

# COMMAND ----------

# DBTITLE 1,Prepare Data for Product Affinity
def prepare_affinity_data():
    """Chuẩn bị dữ liệu order_items từ lớp Silver."""
    # Lưu ý: Kiểm tra tên bảng chính xác của bạn trong silver (ví dụ: 'order_items')
    table_name = config.TABLE_SILVER_ITEMS
    print(f"📊 Đang tải dữ liệu từ {table_name}...")
    
    order_items_df = spark.table(table_name)
    
    # Lọc dữ liệu hợp lệ dựa trên logic Pipeline của bạn
    if "_is_valid" in order_items_df.columns:
        order_items_df = order_items_df.filter(col("_is_valid") == True)
        
    print(f"✅ Đã tải {order_items_df.count():,} bản ghi sản phẩm.")
    return order_items_df

# Thực thi
affinity_input_df = prepare_affinity_data()

# COMMAND ----------

# DBTITLE 1,Analyze Product Affinity Logic
def analyze_affinity(order_items):
    """Tính toán Support, Confidence và Lift cho các cặp sản phẩm."""
    print("⏳ Đang thực hiện Self-join để tìm các cặp sản phẩm mua cùng nhau...")
    
    # 1. Tìm các cặp sản phẩm (A, B) trong cùng đơn hàng
    # Lọc a.product_id < b.product_id để không bị lặp cặp (A,B) và (B,A)
    product_pairs = order_items.alias("a") \
        .join(order_items.alias("b"), "order_id") \
        .filter(col("a.product_id") < col("b.product_id")) \
        .select(
            col("a.product_id").alias("product_a"),
            col("b.product_id").alias("product_b")
        )
    
    # 2. Đếm số lần xuất hiện của mỗi cặp
    pair_counts = product_pairs \
        .groupBy("product_a", "product_b") \
        .agg(count("*").alias("co_purchase_count")) \
        .filter("co_purchase_count >= 5") # Chỉ lấy cặp xuất hiện từ 5 lần trở lên
    
    # 3. Tính toán các chỉ số Affinity
    total_orders = order_items.select("order_id").distinct().count()
    product_counts = order_items.groupBy("product_id").agg(countDistinct("order_id").alias("order_count"))
    
    affinity_df = pair_counts \
        .join(product_counts.alias("pa"), col("product_a") == col("pa.product_id")) \
        .withColumnRenamed("order_count", "product_a_count") \
        .join(product_counts.alias("pb"), col("product_b") == col("pb.product_id")) \
        .withColumnRenamed("order_count", "product_b_count") \
        .withColumn("support", col("co_purchase_count") / total_orders) \
        .withColumn("confidence_a_to_b", col("co_purchase_count") / col("product_a_count")) \
        .withColumn("lift", col("support") / ((col("product_a_count") / total_orders) * (col("product_b_count") / total_orders))) \
        .select("product_a", "product_b", "co_purchase_count", "support", "confidence_a_to_b", "lift")
        
    print(f"✅ Phân tích hoàn tất. Tìm thấy {affinity_df.count():,} mối liên hệ sản phẩm.")
    return affinity_df

# Thực thi phân tích
affinity_results_df = analyze_affinity(affinity_input_df)

# COMMAND ----------

# DBTITLE 1,Save Product Affinity to Unity Catalog
def save_affinity_results(affinity_df):
    """Lưu kết quả vào bảng ml_models.product_affinity."""
    output_table = f"{config.CATALOG}.{config.SCHEMA_ML}.product_affinity"
    print(f"💾 Đang lưu kết quả vào {output_table}...")
    
    # Thêm timestamp và ghi đè schema để đảm bảo đồng bộ
    final_df = affinity_df.withColumn("_updated_at", current_timestamp())
    
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(output_table)
        
    print(f"✅ Đã lưu bảng thành công!")
    return final_df

# Thực thi lưu trữ
affinity_final = save_affinity_results(affinity_results_df)

# Hiển thị Top 10 cặp sản phẩm có Lift cao nhất
display(affinity_final.orderBy(desc("lift")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Verify ML Tables

# COMMAND ----------

# DBTITLE 1,Verify ML Tables
# ✅ ML Tables Verification
print(f"\n{'='*70}")
print(f"✅ KIỂM TRA HỆ THỐNG BẢNG ML - CATALOG: {config.CATALOG}")
print(f"{'='*70}\n")

# Danh sách các bảng mục tiêu đã được tạo từ 4 Model
ml_tables = [
    "customer_segments",
    "revenue_forecasts",
    "revenue_anomalies",
    "product_affinity"
]

for table in ml_tables:
    try:
        # Xây dựng full path theo cấu trúc: catalog.ml_models.table_name
        full_table_name = f"{config.CATALOG}.{config.SCHEMA_ML}.{table}"
        
        # Đọc thử bảng từ Unity Catalog
        df = spark.table(full_table_name)
        count = df.count()
        
        print(f"  ✅ {table:<25} | Số lượng bản ghi: {count:>10,}")
        
        # Kiểm tra timestamp cập nhật gần nhất (nếu có cột _updated_at)
        if "_updated_at" in df.columns:
            last_update = df.agg(max("_updated_at")).collect()[0][0]
            print(f"     └─ Cập nhật gần nhất: {last_update}")
            
    except Exception as e:
        # Hiển thị lỗi ngắn gọn nếu bảng chưa tồn tại hoặc bị lỗi schema
        error_msg = str(e).split('\n')[0][:50]
        print(f"  ❌ {table:<25} | LỖI: {error_msg}...")

print(f"\n{'='*70}")
print("🏁 HOÀN TẤT PIPELINE MACHINE LEARNING")
print(f"{'='*70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 MLflow Model Registry

# COMMAND ----------

# DBTITLE 1,List MLflow Runs
# 📊 Kiểm tra lịch sử chạy Experiment & Model Registry
print(f"\n{'='*70}")
print(f"📊 MLFLOW TRACKING & REGISTRY REPORT")
print(f"{'='*70}\n")

try:
    client = MlflowClient()
    
    # 1. Truy xuất thông tin Experiment
    # Sử dụng config.EXPERIMENT_PATH đã định nghĩa ở phần đầu Notebook
    experiment = client.get_experiment_by_name(config.EXPERIMENT_PATH)
    
    if experiment:
        print(f"🧪 Experiment Name: {experiment.name}")
        print(f"🆔 Experiment ID: {experiment.experiment_id}")
        
        # Tìm kiếm 10 lần chạy gần nhất
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=10
        )
        
        print(f"\n{'Run Name':<35} | {'Status':<12} | {'Duration':>8}")
        print("-" * 70)
        
        for run in runs:
            run_name = run.data.tags.get("mlflow.runName", "Unnamed Run")
            status = run.info.status
            # Tính toán thời gian chạy (ms -> s)
            duration = (run.info.end_time - run.info.start_time) / 1000 if run.info.end_time else 0
            print(f"{run_name:<35} | {status:<12} | {duration:>7.1f}s")
    else:
        print(f"⚠️ Không tìm thấy Experiment tại đường dẫn: {config.EXPERIMENT_PATH}")
    
except Exception as e:
    print(f"⚠️ Lỗi khi truy xuất dữ liệu MLflow: {e}")

print(f"\n{'='*70}")