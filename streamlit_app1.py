"""
Real-time Attribution Dashboard
Displays first-click vs last-click attribution metrics with live streaming events
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
import json
from datetime import datetime, timedelta, date
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardConfig:
    """Configuration for dashboard"""
    PROJECT_ID = "ga4-attribution-demo"  # Replace with actual project ID
    DATASET_ID = "dbt_hp"
    REFRESH_INTERVAL = 60  # seconds
    LOOKBACK_DAYS = 14

@st.cache_resource
def init_bigquery_client():
    """Initialize BigQuery client (cached)"""
    try:
        return bigquery.Client(project=DashboardConfig.PROJECT_ID)
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        st.error(f"Failed to connect to BigQuery: {e}")
        return None

@st.cache_data(ttl=60)  # Cache for 1 minute
def get_attribution_summary(days_back: int = 14) -> pd.DataFrame:
    """Get attribution summary metrics"""
    client = init_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    end_date = date.today()
    start_date="2020-01-01"
    #start_date = end_date - timedelta(days=days_back)
    
    query = f"""
    WITH attribution_metrics AS (
      SELECT
        event_date,
        attributed_channel,
        conversion_event,
        SUM(first_click_conversions) as first_click_conversions,
        SUM(first_click_revenue) as first_click_revenue,
        SUM(last_click_conversions) as last_click_conversions,
        SUM(last_click_revenue) as last_click_revenue,
        SUM(first_click_unique_users) as first_click_users,
        SUM(last_click_unique_users) as last_click_users
      FROM `{DashboardConfig.PROJECT_ID}.{DashboardConfig.DATASET_ID}.mart_attribution_summary`
      WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY 1, 2, 3
    )
    SELECT * FROM attribution_metrics
    ORDER BY event_date DESC, first_click_revenue DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logger.error(f"Error querying attribution summary: {e}")
        st.error(f"Error loading attribution data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_channel_performance(days_back: int = 14) -> pd.DataFrame:
    """Get channel performance metrics"""
    client = init_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    end_date = date.today()
    start_date="2020-01-01"
    #start_date = end_date - timedelta(days=days_back)
    
    query = f"""
    SELECT
      attributed_channel as channel,
      SUM(first_click_revenue + last_click_revenue) / 2 as avg_revenue,
      SUM(first_click_conversions + last_click_conversions) / 2 as avg_conversions,
      CASE 
        WHEN SUM(first_click_conversions + last_click_conversions) > 0
        THEN SUM(first_click_revenue + last_click_revenue) / SUM(first_click_conversions + last_click_conversions)
        ELSE 0
      END as avg_order_value,
      COUNT(DISTINCT event_date) as active_days
    FROM `{DashboardConfig.PROJECT_ID}.{DashboardConfig.DATASET_ID}.mart_attribution_summary`
    WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    ORDER BY avg_revenue DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logger.error(f"Error querying channel performance: {e}")
        st.error(f"Error loading channel data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=30)  # More frequent refresh for live events
def get_streaming_events(limit: int = 100) -> pd.DataFrame:
    """Get recent streaming events"""
    client = init_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    query = f"""
    SELECT
      event_timestamp,
      event_name,
      user_pseudo_id,
      traffic_source,
      traffic_medium,
      traffic_campaign,
      device_category,
      country,
      purchase_revenue_in_usd,
      _stream_inserted_at
    FROM `{DashboardConfig.PROJECT_ID}.{DashboardConfig.DATASET_ID}.streaming_events`
    ORDER BY _stream_inserted_at DESC
    LIMIT {limit}
    """
    
    try:
        df = client.query(query).to_dataframe()
        if not df.empty:
            df['event_time'] = pd.to_datetime(df['event_timestamp'], unit='us')
        return df
    except Exception as e:
        logger.error(f"Error querying streaming events: {e}")
        # Don't show error for streaming events as they might not exist yet
        return pd.DataFrame()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_attribution_comparison(days_back: int = 14) -> pd.DataFrame:
    """Get attribution model comparison data"""
    client = init_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    end_date = date.today()
    start_date="2020-01-01"
    #start_date = end_date - timedelta(days=days_back)
    
    query = f"""
    SELECT
      event_date,
      SUM(first_click_revenue) as first_click_total,
      SUM(last_click_revenue) as last_click_total,
      SUM(first_click_conversions) as first_click_conversions,
      SUM(last_click_conversions) as last_click_conversions,
      COUNT(DISTINCT attributed_channel) as active_channels
    FROM `{DashboardConfig.PROJECT_ID}.{DashboardConfig.DATASET_ID}.mart_attribution_summary`
    WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    ORDER BY 1
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        logger.error(f"Error querying attribution comparison: {e}")
        st.error(f"Error loading comparison data: {e}")
        return pd.DataFrame()

def create_metrics_cards(df_summary: pd.DataFrame):
    """Create metric cards for key KPIs"""
    if df_summary.empty:
        st.warning("No attribution data available for the selected period")
        return
    
    # Calculate totals
    total_fc_revenue = df_summary['first_click_revenue'].sum()
    total_lc_revenue = df_summary['last_click_revenue'].sum()
    total_fc_conversions = df_summary['first_click_conversions'].sum()
    total_lc_conversions = df_summary['last_click_conversions'].sum()
    
    # Create columns for metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        delta_pct = ((total_fc_revenue - total_lc_revenue) / total_lc_revenue * 100) if total_lc_revenue > 0 else 0
        st.metric(
            "First-Click Revenue",
            f"${total_fc_revenue:,.2f}",
            delta=f"{delta_pct:.1f}% vs Last-Click"
        )
    
    with col2:
        delta_pct = ((total_lc_revenue - total_fc_revenue) / total_fc_revenue * 100) if total_fc_revenue > 0 else 0
        st.metric(
            "Last-Click Revenue", 
            f"${total_lc_revenue:,.2f}",
            delta=f"{delta_pct:.1f}% vs First-Click"
        )
    
    with col3:
        st.metric(
            "First-Click Conversions",
            f"{total_fc_conversions:,}",
            delta=f"{total_fc_conversions - total_lc_conversions} vs Last-Click"
        )
    
    with col4:
        st.metric(
            "Last-Click Conversions",
            f"{total_lc_conversions:,}", 
            delta=f"{total_lc_conversions - total_fc_conversions} vs First-Click"
        )

def create_time_series_chart(df_comparison: pd.DataFrame):
    """Create time series chart comparing attribution models"""
    if df_comparison.empty:
        st.warning("No time series data available for the selected period")
        return
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Revenue Attribution Over Time', 'Conversions Over Time'),
        vertical_spacing=0.08
    )
    
    # Revenue chart
    fig.add_trace(
        go.Scatter(
            x=df_comparison['event_date'],
            y=df_comparison['first_click_total'],
            name='First-Click Revenue',
            line=dict(color='#1f77b4', width=3),
            hovertemplate='<b>First-Click</b><br>Date: %{x}<br>Revenue: $%{y:,.2f}<extra></extra>'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df_comparison['event_date'],
            y=df_comparison['last_click_total'],
            name='Last-Click Revenue',
            line=dict(color='#ff7f0e', width=3),
            hovertemplate='<b>Last-Click</b><br>Date: %{x}<br>Revenue: $%{y:,.2f}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Conversions chart
    fig.add_trace(
        go.Scatter(
            x=df_comparison['event_date'],
            y=df_comparison['first_click_conversions'],
            name='First-Click Conversions',
            line=dict(color='#2ca02c', width=3),
            hovertemplate='<b>First-Click</b><br>Date: %{x}<br>Conversions: %{y}<extra></extra>'
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df_comparison['event_date'],
            y=df_comparison['last_click_conversions'],
            name='Last-Click Conversions',
            line=dict(color='#d62728', width=3),
            hovertemplate='<b>Last-Click</b><br>Date: %{x}<br>Conversions: %{y}<extra></extra>'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=600,
        showlegend=True,
        title_text="Attribution Model Comparison - Time Trend"
    )
    
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Revenue ($)", row=1, col=1)
    fig.update_yaxes(title_text="Conversions", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)

def create_channel_breakdown(df_channel: pd.DataFrame):
    """Create channel performance breakdown"""
    if df_channel.empty:
        st.warning("No channel data available for the selected period")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue pie chart
        fig_pie = px.pie(
            df_channel, 
            values='avg_revenue', 
            names='channel',
            title='Revenue by Channel',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_pie.update_traces(
            hovertemplate='<b>%{label}</b><br>Revenue: $%{value:,.2f}<br>Percentage: %{percent}<extra></extra>'
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # AOV bar chart
        fig_bar = px.bar(
            df_channel.sort_values('avg_order_value', ascending=False),
            x='channel',
            y='avg_order_value',
            title='Average Order Value by Channel',
            color='avg_order_value',
            color_continuous_scale='viridis'
        )
        fig_bar.update_traces(
            hovertemplate='<b>%{x}</b><br>AOV: $%{y:,.2f}<extra></extra>'
        )
        fig_bar.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar, use_container_width=True)

def create_live_events_panel(df_streaming: pd.DataFrame):
    """Create live events panel"""
    st.subheader("🔴 Live Streaming Events")
    
    if df_streaming.empty:
        st.info("No streaming events detected. Start the streaming pipeline to see live data.")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_events = len(df_streaming)
        st.metric("Total Events", total_events)
    
    with col2:
        unique_users = df_streaming['user_pseudo_id'].nunique()
        st.metric("Unique Users", unique_users)
    
    with col3:
        conversions = len(df_streaming[df_streaming['event_name'].isin(['purchase', 'add_to_cart'])])
        st.metric("Conversions", conversions)
    
    with col4:
        revenue = df_streaming['purchase_revenue_in_usd'].sum()
        st.metric("Revenue", f"${revenue:,.2f}")
    
    # Event type distribution
    if not df_streaming.empty:
        event_counts = df_streaming['event_name'].value_counts()
        
        fig_events = px.bar(
            x=event_counts.index,
            y=event_counts.values,
            title="Event Type Distribution (Last 100 Events)",
            labels={'x': 'Event Type', 'y': 'Count'}
        )
        st.plotly_chart(fig_events, use_container_width=True)
    
    # Recent events table
    st.subheader("Recent Events")
    display_cols = ['event_time', 'event_name', 'user_pseudo_id', 'traffic_source', 'traffic_medium', 'country', 'purchase_revenue_in_usd']
    display_df = df_streaming.head(20)[display_cols].copy()
    display_df['event_time'] = display_df['event_time'].dt.strftime('%H:%M:%S')
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

def main():
    """Main dashboard application"""
    st.set_page_config(
        page_title="Attribution Pipeline Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📊 Real-Time Attribution Dashboard")
    st.markdown("---")
    
    # Sidebar controls
    st.sidebar.title("Dashboard Controls")
    
    # Lookback period selector
    lookback_days = st.sidebar.selectbox(
        "Lookback Period",
        [7, 14, 30, 90],
        index=1,  # Default to 14 days
        help="Select the number of days to look back for attribution data"
    )
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto Refresh (60s)", value=False)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Auto-refresh logic (corrected)
    if auto_refresh:
        # Use a placeholder for countdown
        refresh_placeholder = st.sidebar.empty()
        for remaining in range(DashboardConfig.REFRESH_INTERVAL, 0, -1):
            refresh_placeholder.text(f"Auto-refresh in {remaining}s")
            time.sleep(1)
        refresh_placeholder.empty()
        st.cache_data.clear()
        st.rerun()
    
    # Data loading
    with st.spinner("Loading attribution data..."):
        df_summary = get_attribution_summary(lookback_days)
        df_channel = get_channel_performance(lookback_days)
        df_comparison = get_attribution_comparison(lookback_days)
        df_streaming = get_streaming_events()
    
    # Main dashboard content
    
    # Key metrics
    st.header("📈 Key Attribution Metrics")
    create_metrics_cards(df_summary)
    
    st.markdown("---")
    
    # Time series analysis
    st.header("📊 Attribution Trends")
    create_time_series_chart(df_comparison)
    
    st.markdown("---")
    
    # Channel breakdown
    st.header("🎯 Channel Performance")
    create_channel_breakdown(df_channel)
    
    st.markdown("---")
    
    # Live streaming panel
    create_live_events_panel(df_streaming)
    
    # Sidebar information
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Data Sources:**")
    st.sidebar.markdown("- GA4 Public Dataset")
    st.sidebar.markdown("- Real-time Streaming Events")
    st.sidebar.markdown("- dbt Attribution Models")
    
    st.sidebar.markdown("**Attribution Models:**")
    st.sidebar.markdown("- First-Click: Full credit to first touchpoint")
    st.sidebar.markdown("- Last-Click: Full credit to last touchpoint")
    st.sidebar.markdown("- 30-day lookback window")
    
    # Footer
    st.markdown("---")
    st.markdown("*Dashboard last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "*")

if __name__ == "__main__":
    main()