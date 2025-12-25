import streamlit as st
import duckdb
import pandas as pd
import requests
import os

# ===============================
# CONFIG
# ===============================
DUCKDB_FILE_ID = "16rZDqCd8PQjrHVJnXjDKGM__kD83Cajd"
DUCKDB_LOCAL_PATH = "/tmp/bd_companies.duckdb"
ALLOWED_EMAIL_DOMAIN = "@12252025trypecter.com"

# ===============================
# EMAIL GATE
# ===============================
st.title("BD Domain Matcher")

email = st.text_input("Enter your work email to continue")

if not email:
    st.stop()

if not email.lower().endswith(ALLOWED_EMAIL_DOMAIN):
    st.error("Access restricted to tryspecter.com emails only.")
    st.stop()

# ===============================
# DOWNLOAD DUCKDB FILE (ONCE)
# ===============================
@st.cache_resource
def load_db():
    if not os.path.exists(DUCKDB_LOCAL_PATH):
        url = f"https://drive.google.com/uc?id={DUCKDB_FILE_ID}&export=download"
        with st.spinner("Loading database… (first time only)"):
            r = requests.get(url, stream=True)
            with open(DUCKDB_LOCAL_PATH, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

    return duckdb.connect(DUCKDB_LOCAL_PATH, read_only=True)

con = load_db()

# ===============================
# UI — DOMAIN UPLOAD
# ===============================
st.subheader("Upload domains CSV")

uploaded_file = st.file_uploader(
    "Upload CSV (single column, domains only)",
    type=["csv"]
)

if not uploaded_file:
    st.stop()

domains_df = pd.read_csv(uploaded_file, header=None)
domains_df.columns = ["domain"]

# clean domains (same as your script)
domains_df["domain"] = (
    domains_df["domain"]
    .astype(str)
    .str.strip()
    .str.lower()
    .str.replace("www.", "", regex=False)
)

con.register("input_domains", domains_df)

# ===============================
# QUERY (IDENTICAL LOGIC)
# ===============================
st.info("Running domain match…")

query = """
SELECT c.*
FROM companies c
JOIN input_domains d
ON c.derived_domain = d.domain
"""

result_df = con.execute(query).fetchdf()

st.success(f"Matched rows: {len(result_df)}")

st.dataframe(result_df.head(100))

# ===============================
# DOWNLOAD
# ===============================
csv = result_df.to_csv(index=False).encode("utf-8")

st.download_button(
    "Download full matched CSV",
    csv,
    "matched_domains.csv",
    "text/csv"
)
