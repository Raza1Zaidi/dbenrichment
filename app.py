import streamlit as st
import duckdb
import pandas as pd
import gdown
import os

# ===============================
# CONFIG
# ===============================
DUCKDB_FILE_ID = "16rZDqCd8PQjrHVJnXjDKGM__kD83Cajd"
DUCKDB_LOCAL_PATH = "/tmp/bd_companies.duckdb"
ALLOWED_EMAIL_DOMAIN = "@tryspecter.com"

# ===============================
# EMAIL GATE
# ===============================
st.title("LinkedIn Domain Enrichment")

email = st.text_input("Enter your work email to continue")

if not email:
    st.stop()

if not email.lower().endswith(ALLOWED_EMAIL_DOMAIN):
    st.error("Access restricted to tryspecter.com emails only.")
    st.stop()

# ===============================
# SAFE DUCKDB LOADER
# ===============================
@st.cache_resource
def load_db():
    # If file exists but is small, it's invalid → delete it
    if os.path.exists(DUCKDB_LOCAL_PATH):
        if os.path.getsize(DUCKDB_LOCAL_PATH) < 100 * 1024 * 1024:  # <100MB = wrong
            os.remove(DUCKDB_LOCAL_PATH)

    # Download if missing
    if not os.path.exists(DUCKDB_LOCAL_PATH):
        with st.spinner("Downloading database (first run only, ~2.6GB)…"):
            url = f"https://drive.google.com/file/d/{DUCKDB_FILE_ID}/view"
            gdown.download(
                url=url,
                output=DUCKDB_LOCAL_PATH,
                quiet=False,
                fuzzy=True
            )

    # Final safety check
    size_gb = os.path.getsize(DUCKDB_LOCAL_PATH) / (1024**3)
    if size_gb < 1.0:
        raise RuntimeError("Downloaded file is not a valid DuckDB database.")

    return duckdb.connect(DUCKDB_LOCAL_PATH, read_only=True)

con = load_db()

# ===============================
# UPLOAD DOMAINS
# ===============================
st.subheader("Upload domains CSV")

uploaded_file = st.file_uploader(
    "Upload CSV (single column: domains)",
    type=["csv"]
)

if not uploaded_file:
    st.stop()

domains_df = pd.read_csv(uploaded_file, header=None)
domains_df.columns = ["domain"]

# Clean domains (exactly like your internal logic)
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
st.download_button(
    "Download full matched CSV",
    result_df.to_csv(index=False).encode("utf-8"),
    "matched_domains.csv",
    "text/csv"
)
