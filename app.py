import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup Connection
# In production, use st.secrets for the JSON key
client = bigquery.Client()

st.set_page_config(page_title="PragyanAI Analytics", layout="wide")
st.title("🎓 Student Performance & Attendance Hub")

# --- DATA UPLOAD TAB ---
tab1, tab2 = st.tabs(["Upload & Map Data", "Analysis Dashboard"])

with tab1:
    st.subheader("Data Ingestion")
    category = st.selectbox("Select Table to Update", ["master_students", "attendance", "mcq_results"])
    uploaded_file = st.file_uploader(f"Upload {category} CSV", type="csv")

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.dataframe(df.head())
        
        if st.button("Push to BigQuery"):
            dataset_id = "your_project_id.student_data"
            table_ref = f"{dataset_id}.{category}"
            
            # Configuration to append data
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            
            try:
                client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
                st.success(f"Successfully updated {category}!")
            except Exception as e:
                st.error(f"Error: {e}")

# --- ANALYSIS TAB ---
with tab2:
    st.subheader("Integrated Performance Analytics")
    
    # Query to join tables on student_id
    sql_query = """
    SELECT 
        m.full_name,
        m.department,
        COUNT(a.status) as total_sessions,
        SUM(CASE WHEN a.status = 'Present' THEN 1 ELSE 0 END) as attended_sessions,
        AVG(q.score) as avg_mcq_score
    FROM `your_project_id.student_data.master_students` m
    LEFT JOIN `your_project_id.student_data.attendance` a ON m.student_id = a.student_id
    LEFT JOIN `your_project_id.student_data.mcq_results` q ON m.student_id = q.student_id
    GROUP BY 1, 2
    """
    
    if st.button("Generate Insights"):
        results_df = client.query(sql_query).to_dataframe()
        
        # Calculate Attendance %
        results_df['attendance_rate'] = (results_df['attended_sessions'] / results_df['total_sessions']) * 100
        
        # Display Metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Top Performer", results_df.iloc[results_df['avg_mcq_score'].idxmax()]['full_name'])
        col2.metric("Avg Attendance", f"{results_df['attendance_rate'].mean():.1f}%")
        col3.metric("Batch Strength", len(results_df))
        
        st.divider()
        st.write("### Detailed Student Breakdown")
        st.dataframe(results_df.style.background_gradient(subset=['avg_mcq_score'], cmap='Greens'))
        
        # Performance Chart
        st.bar_chart(data=results_df, x="full_name", y="avg_mcq_score")
