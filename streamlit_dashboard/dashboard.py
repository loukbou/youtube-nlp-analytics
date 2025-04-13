import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime
import numpy as np

# Page config
st.set_page_config(
    page_title="YouTube NLP Analytics Dashboard",
    layout="wide",
    page_icon="📊"
)

# Custom CSS
st.markdown("""
    <style>
    .main {background-color: #f8f9fa;}
    .st-emotion-cache-1v0mbdj {border-radius: 10px;}
    .plot-container {border: 1px solid #dee2e6; border-radius: 10px; padding: 20px;}
    </style>
    """, unsafe_allow_html=True)

# Title
st.title("📊 YouTube Video Analysis Dashboard")
st.markdown("Interactive visualization of video sentiment and topic analysis.")

# Load data
@st.cache_data(ttl=3600)
def load_latest_analysis():
    possible_data_dirs = [
        '/data',
        '/opt/airflow/data',
        '/app/data',
        os.path.join(os.path.dirname(__file__), 'data'),
        os.path.join(os.path.dirname(__file__), 'dags/data')
    ]
    
    for data_dir in possible_data_dirs:
        try:
            if os.path.exists(data_dir):
                all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) 
                             if f.endswith('.csv') and os.path.isfile(os.path.join(data_dir, f))]
                if all_files:
                    latest_file = max(all_files, key=os.path.getmtime)
                    return pd.read_csv(latest_file)
        except Exception as e:
            st.warning(f"Tried {data_dir}: {str(e)}")
    
    st.error("No CSV files found.")
    return None

df = load_latest_analysis()

if df is not None:
    st.sidebar.header("Filters")
    df['analysis_date'] = pd.to_datetime(df['analysis_date'])
    min_date = df['analysis_date'].min()
    max_date = df['analysis_date'].max()
    
    date_range = st.sidebar.date_input(
        "Analysis Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    filtered_df = df[
        (df['analysis_date'] >= pd.to_datetime(date_range[0])) &
        (df['analysis_date'] <= pd.to_datetime(date_range[1]))
    ]

    if filtered_df.empty:
        st.warning("Aucune donnée à afficher pour la plage de dates sélectionnée.")
    else:
        # Key Metrics
        st.subheader("📈 Key Metrics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Videos Analyzed", len(filtered_df))
        with col2:
            st.metric("Dominant Sentiment", filtered_df['sentiment'].mode()[0] if not filtered_df['sentiment'].mode().empty else "N/A")
        with col3:
            st.metric("Top Topic", filtered_df['topic'].mode()[0] if not filtered_df['topic'].mode().empty else "N/A")

        # Main Visuals
        st.subheader("📊 Analysis Results")
        tab1, tab2, tab3 = st.tabs(["Sentiment Analysis", "Topic Distribution", "Combined View"])

        with tab1:
            st.plotly_chart(
                px.pie(filtered_df, names='sentiment', title='Sentiment Distribution'),
                use_container_width=True
            )

        with tab2:
            topic_counts = filtered_df['topic'].value_counts().reset_index()
            topic_counts.columns = ['topic', 'count']  # 🛠️ FIXED HERE

            st.plotly_chart(
                px.bar(
                    topic_counts,
                    x='topic', y='count',
                    title='Topic Frequency',
                    labels={'topic': 'Topic', 'count': 'Count'}
                ),
                use_container_width=True
            )

        with tab3:
            st.plotly_chart(
                px.sunburst(filtered_df, path=['topic', 'sentiment'], title='Topic-Sentiment Relationship'),
                use_container_width=True
            )

        # Raw Data
        st.subheader("🔍 Raw Data")
        with st.expander("View Analysis Results"):
            st.dataframe(filtered_df.sort_values('analysis_date', ascending=False))

        # Download
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Analysis",
            data=csv,
            file_name="youtube_analysis_results.csv",
            mime="text/csv"
        )
