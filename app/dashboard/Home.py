import os
import requests
import streamlit as st

st.set_page_config(page_title="RiskGraph", page_icon="🕸️", layout="wide")
st.title("🕸️ RiskGraph")
st.caption("Mapping systemic dependency risk in the npm / JavaScript ecosystem")

api = os.getenv("API_URL", "http://localhost:8000")
try:
    status = requests.get(f"{api}/health", timeout=3).json()
    st.success(f"Backend connected: {status}")
except requests.RequestException:
    st.warning("Backend is not reachable yet. Start the services with Docker Compose.")

st.subheader("Project workspace")
st.info("This starter dashboard confirms the service connection. Graph exploration, risk metrics, and failure simulations will be added as the analytics pipeline is implemented.")
if st.button("Check object storage"):
    try:
        st.json(requests.get(f"{api}/storage", timeout=5).json())
    except requests.RequestException as exc:
        st.error(str(exc))
