import streamlit as st
import duckdb
import pandas as pd
import gdown
import os

# =====================================================
# PAGE SETUP
# =====================================================
st.set_page_config(page_title="Domain Enrichment", layout="wide")

# =====================================================
# BASIC STYLING (INLINE ONLY)
# =====================================================
st.markdown("""
<style>
body {
    background-color: #F5F7FB;
}
.app-header {
    background: linear-gradient(90deg, #4F46E5, #6366F1);
    padding: 18px 24px;
    border-radius: 10px;
    color: white;
    margin-bottom: 24px;
}
.card {
    background: white;
    padding: 22px;
    border-radius: 12px;
    box-shadow: 0 4px 14px rgba(0,0,0,0.06);
    margin-bottom: 20px;
}
.metric-box {
    background: #F8FAFC;
    border: 1px solid #E5E7EB;
    border-radius: 10px;
    padding: 14px;
    text-align: center;
}
.metric-value {
    font-size: 22px;
    font-weight: 600;
    color: #111827;
}
.metric-label {
    font-size: 13px;
    color: #6B7280;
}
</style>
""", unsafe_allow_html=True)

# =====================================================
# CONFIG
# =====================================================
DUCKDB_FILE_ID = "16rZDqCd8PQjrHVJnXjDKGM__kD83Cajd"
DUCKDB_LOCAL_PATH = "/tmp/bd_companies.duckdb"
ALLOWED_EMAIL_DOMAIN = "@tryspecter.com"

# =====================================================
# HEADER
# =====================================================
st.markdown("""
<div class="app-header">
    <h2 style="margin:0;">LinkedIn Domain Enrichment</h2>
    <p style="margin:4px 0 0;opacity:0.9;">
        Match domains against the internal LinkedIn companies dataset
    </p>
</div>
""", unsafe_allow_html=True)

# =====================================================
# EMAIL GATE (SIDEBAR)
# =====================================================
with st.sidebar:
    st.markdown("### Access Control")
    email = st.text_input("Work Email")

    if not email:
        st.stop()

    if not email.lower().endswith(ALLOWED_EMAIL_DOMAIN):
        st.error("Only tryspecter.com emails are allowed.")
        st.stop()

    st.success("Access granted")

# =====================================================
# DUCKDB LOADER
# =====================================================
@st.cache_resource
def load_db():
    if os.path.exists(DUCKDB_LOCAL_PATH):
        if os.path.getsize(DUCKDB_LOCAL_PATH) < 100 * 1024 * 1024:
            os.remove(DUCKDB_LOCAL_PATH)

    if not os.path.exists(DUCKDB_LOCAL_PATH):
        with st.spinner("Loading database (first run only)…"):
            url = f"https://drive.google.com/file/d/{DUCKDB_FILE_ID}/view"
            gdown.download(url, DUCKDB_LOCAL_PATH, fuzzy=True, quiet=False)

    return duckdb.connect(DUCKDB_LOCAL_PATH, read_only=True)

con = load_db()

# =====================================================
# TABS
# =====================================================
tab_single, tab_bulk = st.tabs(["Single Domain", "Bulk Domains"])

# =====================================================
# SINGLE DOMAIN
# =====================================================
with tab_single:
    st.markdown('<div class="card">', unsafe_allow_html=True)

    st.subheader("Single Domain Lookup")
    domain = st.text_input("Enter domain", placeholder="example.com")

    if domain:
        clean_domain = domain.strip().lower().replace("www.", "")
        domains_df = pd.DataFrame({"domain": [clean_domain]})
        con.register("input_domains", domains_df)

        query = """
        SELECT c.*
        FROM companies c
        JOIN input_domains d
        ON c.derived_domain = d.domain
        """

        result_df = con.execute(query).fetchdf()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(
                f"<div class='metric-box'><div class='metric-value'>1</div><div class='metric-label'>Domain Entered</div></div>",
                unsafe_allow_html=True
            )
        with c2:
            st.markdown(
                f"<div class='metric-box'><div class='metric-value'>{len(result_df)}</div><div class='metric-label'>Matches Found</div></div>",
                unsafe_allow_html=True
            )

        if len(result_df) > 0:
            st.dataframe(result_df.head(50), use_container_width=True)
            st.download_button(
                "Download results",
                result_df.to_csv(index=False).encode("utf-8"),
                file_name=f"{clean_domain}_result.csv"
            )
        else:
            st.info("No matching records found.")

    st.markdown('</div>', unsafe_allow_html=True)

# =====================================================
# BULK DOMAIN
# =====================================================
with tab_bulk:
    st.markdown('<div class="card">', unsafe_allow_html=True)

    st.subheader("Bulk Domain Enrichment")
    uploaded_file = st.file_uploader("Upload CSV (single column: domains)", type=["csv"])

    if uploaded_file:
        domains_df = pd.read_csv(uploaded_file, header=None)
        domains_df.columns = ["domain"]
        domains_df["domain"] = (
            domains_df["domain"]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("www.", "", regex=False)
        )

        total_domains = len(domains_df)

        con.register("input_domains", domains_df)

        query = """
        SELECT c.*
        FROM companies c
        JOIN input_domains d
        ON c.derived_domain = d.domain
        """

        result_df = con.execute(query).fetchdf()
        matched_domains = result_df["derived_domain"].nunique() if len(result_df) else 0

        c1, c2 = st.columns(2)
        with c1:
            st.markdown(
                f"<div class='metric-box'><div class='metric-value'>{total_domains}</div><div class='metric-label'>Domains Uploaded</div></div>",
                unsafe_allow_html=True
            )
        with c2:
            st.markdown(
                f"<div class='metric-box'><div class='metric-value'>{matched_domains}</div><div class='metric-label'>Domains Matched</div></div>",
                unsafe_allow_html=True
            )

        if len(result_df) > 0:
            st.dataframe(result_df.head(50), use_container_width=True)
            st.download_button(
                "Download full results",
                result_df.to_csv(index=False).encode("utf-8"),
                file_name="bulk_domain_results.csv"
            )
        else:
            st.info("No matching records found.")

    st.markdown('</div>', unsafe_allow_html=True)
