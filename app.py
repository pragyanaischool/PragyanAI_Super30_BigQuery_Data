import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px

# 1. AUTHENTICATION & CLIENT SETUP
def get_bq_client():
    creds_dict = st.secrets["gcp_service_account"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

client = get_bq_client()
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET_ID = "student_analytics" # Ensure this exists in your GCP Console

# 2. UI HEADER
st.set_page_config(page_title="PragyanAI Student Hub", layout="wide")
st.title("🚀 PragyanAI: Student Performance Engine")
st.markdown("---")

# 3. TABS FOR WORKFLOW
tab_upload, tab_dashboard = st.tabs(["📤 Data Ingestion", "📊 Analysis Dashboard"])

# --- TAB 1: UPLOAD & MAPPING ---
with tab_upload:
    st.header("Upload Student Data")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        category = st.selectbox(
            "Select Data Category", 
            ["master_students", "attendance", "mcq_results"],
            help="Master: Student names/IDs | Attendance: Daily logs | MCQ: Quiz scores"
        )
        uploaded_file = st.file_uploader(f"Upload {category} CSV", type="csv")

    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        
        with col2:
            st.write("### Data Preview")
            st.dataframe(df.head(5), use_container_width=True)
            
            # Map Column Names to Standard if necessary
            st.info("Ensure your CSV has 'student_id' for mapping.")
        
        if st.button("🚀 Push to BigQuery"):
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{category}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND", # Adds new data to existing
                autodetect=True,
            )
            
            try:
                with st.spinner("Writing to BigQuery..."):
                    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
                    job.result()
                st.success(f"Successfully updated {category} table!")
                st.balloons()
            except Exception as e:
                st.error(f"Error: {e}")

# --- TAB 2: ANALYSIS & MAPPING ---
with tab_dashboard:
    st.header("Unified Performance Tracking")
    
    # This SQL joins the three tables on student_id
    # It calculates Attendance % and Average MCQ scores per student
    query = f"""
    SELECT 
        m.student_id,
        m.full_name,
        m.department,
        COUNT(a.status) as total_days,
        COUNTIF(a.status = 'Present') as days_present,
        AVG(q.score) as avg_score
    FROM `{PROJECT_ID}.{DATASET_ID}.master_students` m
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.attendance` a ON m.student_id = a.student_id
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.mcq_results` q ON m.student_id = q.student_id
    GROUP BY 1, 2, 3
    """
    
    if st.button("🔄 Refresh Analysis"):
        try:
            results = client.query(query).to_dataframe()
            
            # Post-processing
            results['attendance_pct'] = (results['days_present'] / results['total_days']).fillna(0) * 100
            
            # KPI Metrics
            m1, m2, m3 = st.columns(3)
            m1.metric("Batch Avg Score", f"{results['avg_score'].mean():.1f}%")
            m2.metric("Avg Attendance", f"{results['attendance_pct'].mean():.1f}%")
            m3.metric("Total Students", len(results))
            
            st.markdown("### Student Performance List")
            st.dataframe(
                results[['full_name', 'department', 'attendance_pct', 'avg_score']]
                .sort_values(by='avg_score', ascending=False),
                use_container_width=True
            )
            
            # Visual: Correlation between Attendance and Performance
            st.markdown("### Attendance vs. MCQ Performance")
            fig = px.scatter(
                results, 
                x="attendance_pct", 
                y="avg_score", 
                color="department",
                hover_name="full_name",
                size="days_present",
                title="Correlation Analysis"
            )
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.warning("Ensure all 3 tables exist in BigQuery and contain 'student_id'.")
            st.error(f"Query Error: {e}")
