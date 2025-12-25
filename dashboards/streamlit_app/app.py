"""
Brazilian E-Commerce Analytics Dashboard
========================================
Streamlit dashboard for Brazilian E-Commerce data visualization
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to import databricks connector
try:
    from databricks_connector import DatabricksConnector
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================
st.set_page_config(
    page_title="Brazilian E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* 1. Định dạng Header chính */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }

    /* 2. Ép các thẻ Metric có nền trắng rõ ràng và nổi bật */
    [data-testid="stMetric"] {
        background-color: #ffffff !important; 
        border-radius: 10px !important;
        padding: 15px !important;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
        border: 1px solid #f0f2f6 !important;
        text-align: center;
    }

    /* 3. Chỉnh màu cho con số (Metric Value) - Màu xanh đậm chuyên nghiệp */
    [data-testid="stMetricValue"] div {
        color: #1f77b4 !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }

    /* 4. Chỉnh màu cho nhãn (Metric Label) - Màu xám đậm dễ đọc */
    [data-testid="stMetricLabel"] p {
        color: #444444 !important;
        font-size: 1rem !important;
        font-weight: 600 !important;

    /* 5. Định dạng phần tăng/giảm (Delta) */
    [data-testid="stMetricDelta"] > div {
        font-weight: 500 !important;
        background-color: rgba(0,0,0,0.03);
        padding: 2px 8px;
        border-radius: 4px;
    }
</style>
""", unsafe_allow_html=True)

# st.markdown("""
#     <style>
#     .main > div { padding-top: 1rem; }
#     .stMetric {
#         background-color: #f5f7f9;
#         border: 1px solid #e6e9ef;
#         padding: 15px;
#         border-radius: 5px;
#     }
#     </style>
# """, unsafe_allow_html=True)


# =============================================================================
# DATA LOADING
# =============================================================================
@st.cache_data(ttl=3600)
def load_sample_data():
    """Load sample data for demo when Databricks is not available."""
    
    # Sample daily sales data
    dates = pd.date_range(start='2017-01-01', end='2018-08-31', freq='D')
    daily_sales = pd.DataFrame({
        'order_date': dates,
        'total_orders': [int(50 + 30 * (1 + 0.5 * np.sin(i/30)) + np.random.randint(-10, 10)) for i in range(len(dates))],
        'gross_revenue': [float(5000 + 3000 * (1 + 0.5 * np.sin(i/30)) + np.random.randint(-500, 500)) for i in range(len(dates))],
        'unique_customers': [int(40 + 20 * (1 + 0.3 * np.sin(i/30)) + np.random.randint(-5, 5)) for i in range(len(dates))],
        'delivery_rate': [float(85 + 10 * np.random.random()) for _ in range(len(dates))],
        'avg_delivery_days': [float(8 + 4 * np.random.random()) for _ in range(len(dates))]
    })
    
    # Sample customer segments
    customer_segments = pd.DataFrame({
        'customer_segment': ['Champions', 'Loyal Customers', 'Potential Loyalists', 'At Risk', 'Lost Customers'],
        'count': [5420, 12350, 28900, 8760, 15230],
        'avg_monetary': [850.5, 420.3, 180.2, 310.8, 95.4],
        'avg_frequency': [4.2, 2.8, 1.5, 1.8, 1.1]
    })
    
    # Sample product categories
    product_categories = pd.DataFrame({
        'category': ['bed_bath_table', 'health_beauty', 'sports_leisure', 'furniture_decor', 
                    'computers_accessories', 'housewares', 'watches_gifts', 'telephony', 
                    'garden_tools', 'auto'],
        'total_revenue': [2850000, 2420000, 1980000, 1750000, 1520000, 1380000, 1250000, 1100000, 980000, 850000],
        'total_orders': [28500, 24200, 19800, 17500, 15200, 13800, 12500, 11000, 9800, 8500],
        'avg_review_score': [4.1, 4.3, 4.0, 3.9, 4.2, 4.0, 4.4, 3.8, 4.1, 3.7]
    })
    
    # Sample geographic data
    geographic = pd.DataFrame({
        'state': ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'DF', 'GO', 'ES'],
        'total_orders': [42000, 13500, 12800, 5600, 5200, 4100, 3800, 2500, 2300, 2100],
        'total_revenue': [8500000, 2850000, 2650000, 1180000, 1050000, 850000, 780000, 520000, 480000, 430000],
        'avg_delivery_days': [7.2, 8.5, 9.1, 12.3, 11.8, 13.2, 15.4, 10.2, 11.5, 9.8],
        'latitude': [-23.55, -22.91, -19.92, -30.03, -25.43, -27.59, -12.97, -15.78, -16.68, -20.32],
        'longitude': [-46.63, -43.17, -43.94, -51.23, -49.27, -48.55, -38.50, -47.93, -49.25, -40.34]
    })
    
    # Sample monthly data
    months = pd.date_range(start='2017-01', end='2018-08', freq='MS')
    monthly_sales = pd.DataFrame({
        'month_start': months,
        'total_orders': [int(1500 + 800 * (1 + 0.3 * np.sin(i/3)) + np.random.randint(-100, 100)) for i in range(len(months))],
        'gross_revenue': [float(150000 + 80000 * (1 + 0.3 * np.sin(i/3)) + np.random.randint(-5000, 5000)) for i in range(len(months))],
        'revenue_growth_pct': [float(-5 + 15 * np.random.random()) for _ in range(len(months))]
    })
    
    return {
        'daily_sales': daily_sales,
        'customer_segments': customer_segments,
        'product_categories': product_categories,
        'geographic': geographic,
        'monthly_sales': monthly_sales
    }


@st.cache_data(ttl=300)
def load_databricks_data():
    """Load data from Databricks."""
    if not DATABRICKS_AVAILABLE:
        return None
    
    try:
        connector = DatabricksConnector()
        return connector.load_all_data()
    except Exception as e:
        st.warning(f"Could not connect to Databricks: {e}")
        return None


def get_data():
    """Get data from Databricks or sample data."""
    db_data = load_databricks_data()
    if db_data:
        return db_data
    return load_sample_data()


# =============================================================================
# SIDEBAR
# =============================================================================
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/0/05/Flag_of_Brazil.svg/200px-Flag_of_Brazil.svg.png", width=100)
st.sidebar.title("🛒 E-Commerce Analytics")
st.sidebar.markdown("---")

# Data source indicator
if DATABRICKS_AVAILABLE:
    st.sidebar.success("✅ Connected to Databricks")
else:
    st.sidebar.warning("⚠️ Using sample data")

# Navigation
page = st.sidebar.radio(
    "Navigation",
    ["📊 Overview", "💰 Revenue Analysis", "👥 Customer Segments", 
     "📦 Product Analytics", "🗺️ Geographic Analysis", "🔍 Data Quality"]
)

# Date filter
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Date Filter")

data = get_data()
daily_sales = data['daily_sales']

# Kiểm tra nếu DataFrame có dữ liệu thì mới lấy min/max date
if not daily_sales.empty and 'order_date' in daily_sales.columns:
    min_date = pd.to_datetime(daily_sales['order_date']).min()
    max_date = pd.to_datetime(daily_sales['order_date']).max()
else:
    # Giá trị mặc định nếu không có dữ liệu
    min_date = datetime.now() - timedelta(days=365)
    max_date = datetime.now()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Filter data by date
if len(date_range) == 2:
    start_date, end_date = date_range
    daily_sales_filtered = daily_sales[
        (daily_sales['order_date'] >= pd.Timestamp(start_date)) &
        (daily_sales['order_date'] <= pd.Timestamp(end_date))
    ]
else:
    daily_sales_filtered = daily_sales

# =============================================================================
# PAGES
# =============================================================================

# -----------------------------------------------------------------------------
# OVERVIEW PAGE
# -----------------------------------------------------------------------------
if page == "📊 Overview":
    st.markdown('<h1 class="main-header">📊 Brazilian E-Commerce Dashboard</h1>', unsafe_allow_html=True)
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    
    total_revenue = daily_sales_filtered['gross_revenue'].sum()
    total_orders = daily_sales_filtered['total_orders'].sum()
    total_customers = daily_sales_filtered['unique_customers'].sum()
    avg_delivery = daily_sales_filtered['avg_delivery_days'].mean()
    
    with col1:
        st.metric("💰 Total Revenue", f"R$ {total_revenue:,.0f}", delta="+12.5%")
    with col2:
        st.metric("📦 Total Orders", f"{total_orders:,}", delta="+8.2%")
    with col3:
        st.metric("👥 Unique Customers", f"{total_customers:,}", delta="+15.3%")
    with col4:
        st.metric("🚚 Avg Delivery Days", f"{avg_delivery:.1f}", delta="-0.5")
    
    st.markdown("---")
    
    # Charts Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Daily Revenue Trend")
        fig = px.line(
            daily_sales_filtered, 
            x='order_date', 
            y='gross_revenue',
            title='Daily Revenue (R$)'
        )
        fig.update_layout(height=350, xaxis_title="Date", yaxis_title="Revenue (R$)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📦 Daily Orders Trend")
        fig = px.bar(
            daily_sales_filtered.tail(30),
            x='order_date',
            y='total_orders',
            title='Daily Orders (Last 30 Days)'
        )
        fig.update_layout(height=350, xaxis_title="Date", yaxis_title="Orders")
        st.plotly_chart(fig, use_container_width=True)
    
    # Charts Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("👥 Customer Segments")
        segments = data['customer_segments']
    
        # Kiểm tra nếu DataFrame không rỗng và có cột cần thiết
        if not segments.empty and 'customer_segment' in segments.columns:
            fig = px.pie(
                segments,
                values='count',
                names='customer_segment',
                title='Customer Distribution by Segment'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu phân khúc khách hàng.")
    
    with col2:
        st.subheader("🏆 Top Product Categories")
        categories = data['product_categories'].head(5)
        fig = px.bar(
            categories,
            x='total_revenue',
            y='category',
            orientation='h',
            title='Top 5 Categories by Revenue',
            color='total_revenue',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=350, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------
# REVENUE ANALYSIS PAGE
# -----------------------------------------------------------------------------
elif page == "💰 Revenue Analysis":
    st.markdown('<h1 class="main-header">💰 Revenue Analysis</h1>', unsafe_allow_html=True)
    
    monthly = data['monthly_sales']
    
    # Monthly Revenue Chart
    st.subheader("📈 Monthly Revenue Trend")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(x=monthly['month_start'], y=monthly['gross_revenue'], name="Revenue", marker_color='#1f77b4'),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=monthly['month_start'], y=monthly['revenue_growth_pct'], name="Growth %", 
                   mode='lines+markers', marker_color='#ff7f0e'),
        secondary_y=True
    )
    
    fig.update_layout(height=400, title="Monthly Revenue and Growth Rate")
    fig.update_yaxes(title_text="Revenue (R$)", secondary_y=False)
    fig.update_yaxes(title_text="Growth (%)", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)
    
    # Revenue by Day of Week
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Revenue by Day of Week")
        daily_sales_filtered['day_of_week'] = pd.to_datetime(daily_sales_filtered['order_date']).dt.day_name()
        dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_revenue = daily_sales_filtered.groupby('day_of_week')['gross_revenue'].mean().reindex(dow_order)
        
        fig = px.bar(
            x=dow_revenue.index,
            y=dow_revenue.values,
            title='Average Revenue by Day of Week',
            color=dow_revenue.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Revenue Distribution")
        fig = px.histogram(
            daily_sales_filtered,
            x='gross_revenue',
            nbins=50,
            title='Daily Revenue Distribution'
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------
# CUSTOMER SEGMENTS PAGE
# -----------------------------------------------------------------------------
elif page == "👥 Customer Segments":
    st.markdown('<h1 class="main-header">👥 Customer Segments Analysis</h1>', unsafe_allow_html=True)
    
    segments = data['customer_segments']
    
    # Segment Overview
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🏆 Champions", f"{segments[segments['customer_segment']=='Champions']['count'].values[0]:,}")
    with col2:
        st.metric("💎 Loyal Customers", f"{segments[segments['customer_segment']=='Loyal Customers']['count'].values[0]:,}")
    with col3:
        st.metric("⚠️ At Risk", f"{segments[segments['customer_segment']=='At Risk']['count'].values[0]:,}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Segment Distribution")
        fig = px.pie(
            segments,
            values='count',
            names='customer_segment',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💰 Avg Monetary by Segment")
        fig = px.bar(
            segments,
            x='customer_segment',
            y='avg_monetary',
            color='avg_monetary',
            color_continuous_scale='RdYlGn'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Segment Details Table
    st.subheader("📋 Segment Details")
    st.dataframe(segments.style.format({
        'count': '{:,}',
        'avg_monetary': 'R$ {:.2f}',
        'avg_frequency': '{:.1f}'
    }), use_container_width=True)

# -----------------------------------------------------------------------------
# PRODUCT ANALYTICS PAGE
# -----------------------------------------------------------------------------
elif page == "📦 Product Analytics":
    st.markdown('<h1 class="main-header">📦 Product Analytics</h1>', unsafe_allow_html=True)
    
    categories = data['product_categories']
    
    # Top Categories by Revenue
    st.subheader("🏆 Top Categories by Revenue")
    fig = px.bar(
        categories,
        x='category',
        y='total_revenue',
        color='avg_review_score',
        color_continuous_scale='RdYlGn',
        title='Revenue by Category with Review Scores'
    )
    fig.update_layout(height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⭐ Review Scores by Category")
        fig = px.scatter(
            categories,
            x='total_orders',
            y='avg_review_score',
            size='total_revenue',
            color='category',
            hover_name='category',
            title='Orders vs Review Score (bubble size = revenue)'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Orders Distribution")
        fig = px.treemap(
            categories,
            path=['category'],
            values='total_orders',
            color='avg_review_score',
            color_continuous_scale='RdYlGn'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------
# GEOGRAPHIC ANALYSIS PAGE
# -----------------------------------------------------------------------------
elif page == "🗺️ Geographic Analysis":
    st.markdown('<h1 class="main-header">🗺️ Geographic Analysis</h1>', unsafe_allow_html=True)
    
    geo = data['geographic']
    
    # Map
    st.subheader("📍 Sales by State")
    fig = px.scatter_geo(
        geo,
        lat='latitude',
        lon='longitude',
        size='total_orders',
        color='total_revenue',
        hover_name='state',
        color_continuous_scale='Viridis',
        scope='south america',
        title='Order Distribution by State'
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top States by Revenue")
        fig = px.bar(
            geo.sort_values('total_revenue', ascending=True),
            x='total_revenue',
            y='state',
            orientation='h',
            color='total_revenue',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🚚 Avg Delivery Days by State")
        fig = px.bar(
            geo.sort_values('avg_delivery_days', ascending=True),
            x='avg_delivery_days',
            y='state',
            orientation='h',
            color='avg_delivery_days',
            color_continuous_scale='RdYlGn_r'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------
# DATA QUALITY PAGE
# -----------------------------------------------------------------------------
elif page == "🔍 Data Quality":
    st.markdown('<h1 class="main-header">🔍 Data Quality Dashboard</h1>', unsafe_allow_html=True)
    
    # Mock DQ metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("✅ Pass Rate", "94.5%", delta="+2.1%")
    with col2:
        st.metric("📊 Checks Run", "156")
    with col3:
        st.metric("⚠️ Warnings", "8", delta="-3")
    with col4:
        st.metric("❌ Critical", "2", delta="-1")
    
    st.markdown("---")
    
    # DQ Status by Layer
    st.subheader("📊 Data Quality by Layer")
    
    dq_data = pd.DataFrame({
        'Layer': ['Bronze', 'Silver', 'Gold', 'Business'],
        'Pass Rate': [98.2, 95.5, 92.1, 91.8],
        'Total Checks': [45, 52, 35, 24],
        'Failed': [1, 2, 3, 2]
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            dq_data,
            x='Layer',
            y='Pass Rate',
            color='Pass Rate',
            color_continuous_scale='RdYlGn',
            title='Pass Rate by Layer'
        )
        fig.add_hline(y=90, line_dash="dash", line_color="red", annotation_text="Threshold")
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(
            dq_data,
            values='Total Checks',
            names='Layer',
            title='Checks Distribution by Layer',
            hole=0.4
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent Issues
    st.subheader("⚠️ Recent Issues")
    
    issues = pd.DataFrame({
        'Timestamp': pd.date_range(end=datetime.now(), periods=5, freq='H'),
        'Table': ['silver.orders', 'silver.payments', 'gold.daily_sales', 'silver.orders', 'business.fact_orders'],
        'Check': ['null_check', 'fk_check', 'freshness', 'validity', 'unique_key'],
        'Severity': ['Warning', 'Critical', 'Warning', 'Info', 'Warning'],
        'Status': ['Open', 'Resolved', 'Open', 'Resolved', 'Open']
    })
    
    st.dataframe(issues, use_container_width=True)

# =============================================================================
# FOOTER
# =============================================================================
st.sidebar.markdown("---")
st.sidebar.markdown("""
**Brazilian E-Commerce Pipeline**  
Version 1.0.0  
Authored by Phạm QUốc Nghiệp  
E-mail: pqnghiep1354@gmail.com  
Last updated: December 2025                
""")
