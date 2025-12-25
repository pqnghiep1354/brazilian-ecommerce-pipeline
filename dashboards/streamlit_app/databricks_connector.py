"""
Databricks Connector for Streamlit Dashboard
=============================================
Connect to Databricks SQL Warehouse for data retrieval
"""

import os
import pandas as pd
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import Databricks SQL connector
try:
    from databricks import sql
    DATABRICKS_SQL_AVAILABLE = True
except ImportError:
    DATABRICKS_SQL_AVAILABLE = False
    logger.warning("databricks-sql-connector not installed. Using fallback mode.")


class DatabricksConnector:
    """
    Connector class for Databricks SQL Warehouse.
    
    Environment Variables Required:
        - DATABRICKS_HOST: Databricks workspace URL
        - DATABRICKS_TOKEN: Personal access token
        - DATABRICKS_HTTP_PATH: SQL Warehouse HTTP path
        - DATABRICKS_CATALOG: Unity Catalog name (optional)
    """
    
    def __init__(self):
        """Initialize the connector with environment variables."""
        self.host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
        self.token = os.getenv("DATABRICKS_TOKEN", "")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        self.catalog = os.getenv("DATABRICKS_CATALOG", "")
        
        self.connection = None
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration."""
        if not all([self.host, self.token, self.http_path]):
            logger.warning("Databricks configuration incomplete. Check environment variables.")
    
    def connect(self) -> bool:
        """
        Establish connection to Databricks SQL Warehouse.
        
        Returns:
            bool: True if connection successful
        """
        if not DATABRICKS_SQL_AVAILABLE:
            logger.error("databricks-sql-connector not available")
            return False
        
        try:
            self.connection = sql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token,
                catalog=self.catalog if self.catalog else None
            )
            logger.info("Connected to Databricks SQL Warehouse")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from Databricks")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            pd.DataFrame: Query results
        """
        if not self.connection:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            cursor.close()
            
            return pd.DataFrame(rows, columns=columns)
        
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    # =========================================================================
    # DATA LOADING METHODS
    # =========================================================================
    
    def load_daily_sales(self, days: int = 365) -> pd.DataFrame:
        """Load daily sales data from Gold layer."""
        query = f"""
        SELECT 
            order_date,
            total_orders,
            unique_customers,
            gross_revenue,
            net_revenue,
            total_freight,
            delivered_orders,
            canceled_orders,
            delivery_rate,
            cancellation_rate,
            late_delivery_rate,
            avg_delivery_days
        FROM gold.daily_sales
        WHERE order_date >= DATE_SUB(CURRENT_DATE(), {days})
        ORDER BY order_date
        """
        return self.execute_query(query)
    
    def load_monthly_sales(self) -> pd.DataFrame:
        """Load monthly sales data from Gold layer."""
        query = """
        SELECT 
            month_start,
            year,
            month,
            total_orders,
            gross_revenue,
            net_revenue,
            orders_per_day,
            revenue_per_day,
            revenue_growth_pct,
            orders_growth_pct,
            delivery_rate
        FROM gold.monthly_sales
        ORDER BY year, month
        """
        return self.execute_query(query)
    
    def load_customer_segments(self) -> pd.DataFrame:
        query = """
        SELECT 
            segment as customer_segment, -- Đổi từ customer_segment -> segment
            COUNT(*) as count,
            AVG(monetary) as avg_monetary,
            AVG(frequency) as avg_frequency,
            AVG(recency) as avg_recency
        FROM ml_models.customer_segments
        GROUP BY segment
        ORDER BY count DESC
        """
        return self.execute_query(query)
    
    def load_customer_analytics(self, limit: int = 1000) -> pd.DataFrame:
        """Load customer analytics data from Gold layer."""
        query = f"""
        SELECT 
            customer_id,
            customer_city,
            customer_state,
            total_orders,
            total_spent,
            avg_order_value,
            first_order_date,
            last_order_date,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            rfm_score,
            customer_segment
        FROM gold.customer_analytics
        LIMIT {limit}
        """
        return self.execute_query(query)
    
    def load_product_analytics(self, limit: int = 100) -> pd.DataFrame:
        """Load product analytics data from Gold layer."""
        query = f"""
        SELECT 
            product_id,
            product_category_name,
            product_category_name_english as category,
            total_sold,
            total_orders,
            total_revenue,
            avg_price,
            avg_review_score,
            revenue_rank,
            sales_rank,
            positive_review_rate,
            product_score
        FROM gold.product_analytics
        ORDER BY total_revenue DESC
        LIMIT {limit}
        """
        return self.execute_query(query)
    
    def load_product_categories(self) -> pd.DataFrame:
        query = """
        SELECT 
            product_category_name as category, 
            SUM(total_revenue) as total_revenue,
            SUM(total_orders) as total_orders,
            AVG(avg_review_score) as avg_review_score
        FROM gold.product_analytics
        GROUP BY product_category_name
        ORDER BY total_revenue DESC
        LIMIT 20
        """
        return self.execute_query(query)
    
    def load_seller_analytics(self, limit: int = 100) -> pd.DataFrame:
        """Load seller analytics data from Gold layer."""
        query = f"""
        SELECT 
            seller_id,
            seller_city,
            seller_state,
            total_orders,
            total_items_sold,
            total_revenue,
            avg_order_value,
            avg_review_score,
            delivery_rate,
            on_time_delivery_rate,
            seller_score,
            seller_tier
        FROM gold.seller_analytics
        ORDER BY total_revenue DESC
        LIMIT {limit}
        """
        return self.execute_query(query)
    
    def load_geographic_analytics(self) -> pd.DataFrame:
        query = """
        SELECT 
            customer_state as state,
            total_orders,
            total_revenue,
            avg_order_value,
            avg_delivery_days,
            delivery_rate,
            state_lat as latitude,
            state_lng as longitude
        FROM gold.geographic_analytics
        ORDER BY total_revenue DESC
        """
        return self.execute_query(query)
    
    def load_revenue_anomalies(self, days: int = 30) -> pd.DataFrame:
        """Load revenue anomalies from ML models."""
        query = f"""
        SELECT 
            ds as date,
            y as revenue,
            orders,
            z_score,
            is_anomaly,
            anomaly_type
        FROM ml_models.revenue_anomalies
        WHERE ds >= DATE_SUB(CURRENT_DATE(), {days})
        ORDER BY ds
        """
        return self.execute_query(query)
    
    def load_alerts(self, days: int = 7) -> pd.DataFrame:
        """Load recent alerts."""
        query = f"""
        SELECT 
            alert_id,
            alert_type,
            severity,
            title,
            message,
            metric_name,
            metric_value,
            created_at,
            is_acknowledged
        FROM business.alert_history
        WHERE created_at >= DATE_SUB(CURRENT_TIMESTAMP(), {days})
        ORDER BY created_at DESC
        LIMIT 100
        """
        return self.execute_query(query)
    
    def load_data_quality_results(self, run_id: str = None) -> pd.DataFrame:
        """Load data quality check results."""
        where_clause = f"WHERE run_id = '{run_id}'" if run_id else ""
        
        query = f"""
        SELECT 
            run_id,
            table_name,
            column_name,
            check_name,
            check_type,
            passed,
            expected_value,
            actual_value,
            records_checked,
            records_failed,
            created_at
        FROM bronze._data_quality_results
        {where_clause}
        ORDER BY created_at DESC
        LIMIT 500
        """
        return self.execute_query(query)
    
    # =========================================================================
    # AGGREGATION METHODS
    # =========================================================================
    
    def get_kpis(self) -> Dict[str, Any]:
        """Get current KPI values."""
        query = """
        SELECT 
            SUM(gross_revenue) as total_revenue,
            SUM(total_orders) as total_orders,
            SUM(unique_customers) as total_customers,
            AVG(delivery_rate) as avg_delivery_rate,
            AVG(avg_delivery_days) as avg_delivery_days,
            AVG(cancellation_rate) as avg_cancellation_rate
        FROM gold.daily_sales
        WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
        """
        
        result = self.execute_query(query)
        
        if result.empty:
            return {}
        
        row = result.iloc[0]
        return {
            'total_revenue': float(row['total_revenue'] or 0),
            'total_orders': int(row['total_orders'] or 0),
            'total_customers': int(row['total_customers'] or 0),
            'avg_delivery_rate': float(row['avg_delivery_rate'] or 0),
            'avg_delivery_days': float(row['avg_delivery_days'] or 0),
            'avg_cancellation_rate': float(row['avg_cancellation_rate'] or 0)
        }
    
    def get_trend_comparison(self, days: int = 30) -> Dict[str, float]:
        """Get trend comparison vs previous period."""
        query = f"""
        WITH current_period AS (
            SELECT SUM(gross_revenue) as revenue, SUM(total_orders) as orders
            FROM gold.daily_sales
            WHERE order_date >= DATE_SUB(CURRENT_DATE(), {days})
        ),
        previous_period AS (
            SELECT SUM(gross_revenue) as revenue, SUM(total_orders) as orders
            FROM gold.daily_sales
            WHERE order_date >= DATE_SUB(CURRENT_DATE(), {days * 2})
              AND order_date < DATE_SUB(CURRENT_DATE(), {days})
        )
        SELECT 
            (c.revenue - p.revenue) / NULLIF(p.revenue, 0) * 100 as revenue_change,
            (c.orders - p.orders) / NULLIF(p.orders, 0) * 100 as orders_change
        FROM current_period c, previous_period p
        """
        
        result = self.execute_query(query)
        
        if result.empty:
            return {'revenue_change': 0.0, 'orders_change': 0.0}
        
        row = result.iloc[0]
        return {
            'revenue_change': float(row['revenue_change'] or 0),
            'orders_change': float(row['orders_change'] or 0)
        }
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def load_all_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Load all data needed for dashboard.
        
        Returns:
            Dict with all DataFrames
        """
        logger.info("Loading all dashboard data...")
        
        if not self.connect():
            return None
        
        data = {
            'daily_sales': self.load_daily_sales(),
            'monthly_sales': self.load_monthly_sales(),
            'customer_segments': self.load_customer_segments(),
            'product_categories': self.load_product_categories(),
            'geographic': self.load_geographic_analytics(),
        }
        
        if data['daily_sales'].empty:
            logger.warning("Failed to load daily sales data")
            return None
        
        logger.info("All data loaded successfully")
        return data
    
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            result = self.execute_query("SELECT 1 as test")
            return not result.empty
        except:
            return False


# =============================================================================
# USAGE EXAMPLE
# =============================================================================
if __name__ == "__main__":
    # Test connection
    connector = DatabricksConnector()
    
    if connector.test_connection():
        print("✅ Connection successful!")
        
        # Load sample data
        daily_sales = connector.load_daily_sales(days=30)
        print(f"Loaded {len(daily_sales)} daily sales records")
        
        kpis = connector.get_kpis()
        print(f"KPIs: {kpis}")
        
        connector.disconnect()
    else:
        print("❌ Connection failed. Check your environment variables.")
