import os
from pathlib import Path
from urllib.parse import unquote
import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
# The dashboard reads the processed Parquet files committed under app/dashboard/data/.
# Set API_URL only if you intentionally want to use a separately hosted FastAPI service.
API_URL = (os.getenv("API_URL", "") or "").rstrip("/")
DATA_DIR = Path(__file__).resolve().parent / "data"
DATASETS = {
    "package_enriched": "package_enriched",
    "graph_vertices": "graph_vertices",
    "graph_edges": "graph_edges",
    "graph_metrics": "graph_metrics",
    "risk_scores": "risk_scores",
    "cascade_summary": "cascade_summary",
    "cascade_affected_nodes": "cascade_affected_nodes",
}
st.set_page_config(
    page_title="RiskGraph",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)
# ============================================================
# DARK THEME
# ============================================================
C = {
    "bg": "#080B12",
    "sidebar": "#0B101A",
    "panel": "#111927",
    "panel2": "#172235",
    "border": "#26354A",
    "text": "#F3F6FC",
    "muted": "#A2AEC2",
    "accent": "#8B7CFF",
    "danger": "#FF4D5F",
    "warning": "#FF9F43",
    "success": "#35D07F",
    "cyan": "#43C7F5",
    "dependency": "#667085",
    "dependent": "#667085",
    "plot": "#0B1220",
}
PLOT_TEMPLATE = "plotly_dark"
# ============================================================
# STYLE
# ============================================================
st.markdown(
    f"""
    <style>
    :root {{
        --bg: {C["bg"]};
        --sidebar: {C["sidebar"]};
        --panel: {C["panel"]};
        --panel2: {C["panel2"]};
        --border: {C["border"]};
        --text: {C["text"]};
        --muted: {C["muted"]};
        --accent: {C["accent"]};
        --danger: {C["danger"]};
        --warning: {C["warning"]};
        --success: {C["success"]};
    }}
    html, body, [data-testid="stAppViewContainer"] {{
        background: radial-gradient(ellipse at 72% -8%, rgba(139,124,255,.11), transparent 34%), var(--bg) !important;
    }}
    .stApp {{
        background: var(--bg);
        color: var(--text);
    }}
    /* Space below Streamlit's top toolbar / Deploy area */
    .block-container {{
        max-width: 1480px;
        padding-top: 4.4rem !important;
        padding-bottom: 3.5rem !important;
        padding-left: 2.6rem !important;
        padding-right: 2.6rem !important;
    }}
    /* Compact proper sidebar */
    [data-testid="stSidebar"] {{
        background: var(--sidebar) !important;
        border-right: 1px solid var(--border);
        min-width: 220px !important;
        max-width: 220px !important;
    }}
    [data-testid="stSidebar"] > div:first-child {{
        padding: 1.35rem .85rem 1.25rem !important;
    }}
    .brand {{
        padding: .15rem .65rem 1.1rem;
        margin-bottom: 1rem;
        border-bottom: 1px solid var(--border);
    }}
    .brand-name {{
        color: var(--text);
        font-size: 1.2rem;
        font-weight: 800;
        letter-spacing: -.035em;
    }}
    .brand-sub {{
        color: var(--muted);
        font-size: .68rem;
        margin-top: .25rem;
        line-height: 1.45;
    }}
    .nav-label {{
        color: var(--muted);
        font-size: .62rem;
        font-weight: 750;
        letter-spacing: .13em;
        text-transform: uppercase;
        padding: 0 .65rem .45rem;
    }}
    /* Hide native radio circles so the navigation looks like a menu */
    [data-testid="stSidebar"] [role="radiogroup"] {{
        gap: 3px !important;
    }}
    [data-testid="stSidebar"] [role="radiogroup"] label {{
        background: transparent !important;
        border: 1px solid transparent !important;
        border-radius: 7px !important;
        padding: .48rem .68rem !important;
        min-height: 33px !important;
        margin: 0 !important;
    }}
    [data-testid="stSidebar"] [role="radiogroup"] label > div:first-child {{
        display: none !important;
    }}
    [data-testid="stSidebar"] [role="radiogroup"] label p {{
        color: var(--muted) !important;
        font-size: .81rem !important;
        font-weight: 540 !important;
        margin: 0 !important;
    }}
    [data-testid="stSidebar"] [role="radiogroup"] label:hover {{
        background: var(--panel2) !important;
        border-color: var(--border) !important;
    }}
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {{
        background: rgba(139,124,255,.13) !important;
        border-color: rgba(139,124,255,.48) !important;
    }}
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) p {{
        color: var(--text) !important;
        font-weight: 680 !important;
    }}
    .sidebar-footer {{
        border-top: 1px solid var(--border);
        margin-top: 1.15rem;
        padding: 1rem .65rem 0;
        color: var(--muted);
        font-size: .66rem;
        line-height: 1.55;
    }}
    /* Page header */
    .page-kicker {{
        color: var(--accent);
        font-size: .67rem;
        font-weight: 750;
        letter-spacing: .15em;
        text-transform: uppercase;
        margin-bottom: .48rem;
    }}
    .page-title {{
        color: var(--text);
        font-size: 2.45rem;
        line-height: 1.05;
        font-weight: 820;
        letter-spacing: -.045em;
        margin: 0;
    }}
    .page-subtitle {{
        color: var(--muted);
        max-width: 880px;
        font-size: .92rem;
        line-height: 1.62;
        margin-top: .65rem;
    }}
    .top-rule {{
        height: 1px;
        background: linear-gradient(90deg, var(--accent), var(--cyan) 38%, transparent);
        margin: 1.65rem 0 1.45rem;
    }}
    /* Dashboard cards */
    .metric-card {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 11px;
        padding: .95rem 1rem;
        min-height: 91px;
    }}
    .metric-label {{
        color: var(--muted);
        font-size: .65rem;
        font-weight: 650;
        letter-spacing: .09em;
        text-transform: uppercase;
    }}
    .metric-value {{
        color: var(--text);
        font-size: 1.58rem;
        font-weight: 790;
        letter-spacing: -.035em;
        margin-top: .32rem;
    }}
    .metric-danger {{ color: #FF4D5F; }}
    .metric-warning {{ color: #FF9F43; }}
    .metric-success {{ color: #35D07F; }}
    .section {{
        margin-top: 1.85rem;
        margin-bottom: .75rem;
    }}
    .section-title {{
        color: var(--text);
        font-size: 1.1rem;
        font-weight: 740;
        letter-spacing: -.018em;
        padding-left: .65rem;
        border-left: 3px solid var(--accent);\n        padding-top: .12rem;\n        padding-bottom: .12rem;
    }}
    .section-caption {{
        color: var(--muted);
        font-size: .79rem;
        margin-top: .22rem;
        line-height: 1.5;
    }}
    .panel {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 13px;
        padding: 1.15rem 1.2rem;
    }}
    .note {{
        color: var(--muted);
        font-size: .78rem;
        line-height: 1.6;
    }}
    .risk-high {{ color: #8B7CFF; font-weight: 750; }}
    .risk-medium {{ color: #FF9F43; font-weight: 750; }}
    .risk-low {{ color: #35D07F; font-weight: 750; }}
    .graph-legend {{
        display: flex;
        gap: 1.15rem;
        flex-wrap: wrap;
        color: var(--muted);
        font-size: .75rem;
        margin: .45rem 0 .85rem;
    }}
    .dot {{
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 5px;
    }}
    .stTextInput input,
    .stSelectbox div[data-baseweb="select"] > div {{
        background: var(--panel) !important;
        color: var(--text) !important;
        border-color: var(--border) !important;
    }}
    .stButton > button {{
        border-radius: 7px;
        border: 1px solid var(--border);
        background: var(--panel);
        color: var(--text);
        font-size: .79rem;
    }}
    .stButton > button:hover {{
        border-color: var(--accent);
        color: var(--text);
    }}
    [data-testid="stAlert"] {{
        background: #043780 !important;
        border: 1px solid #025EC4 !important;
        border-left: 4px solid #00C5E8 !important;
        color: #EAF8FF !important;
    }}
    [data-testid="stMetric"] {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 10px;
    }}
    .brand-name {{
        background: linear-gradient(100deg, #F3F6FC 10%, #BDB5FF 65%, #55C8F2 100%);
        -webkit-background-clip: text;
        background-clip: text;
        -webkit-text-fill-color: transparent;
    }}
    [data-testid="stExpander"] {{
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        background: rgba(17,25,39,.72) !important;
        overflow: hidden;
    }}
    [data-testid="stExpander"] summary {{
        padding: .65rem .8rem !important;
    }}
    [data-testid="stExpander"] summary:hover {{
        color: var(--text) !important;
        background: rgba(139,124,255,.07) !important;
    }}
    [data-testid="stDataFrame"], [data-testid="stTable"] {{
        border: 1px solid var(--border);
        border-radius: 12px;
        overflow: hidden;
    }}
    .stTabs [data-baseweb="tab-list"] {{
        gap: .35rem;
        border-bottom: 1px solid var(--border);
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 8px 8px 0 0;
        padding: .65rem .9rem;
    }}
    .stTabs [aria-selected="true"] {{
        color: var(--text) !important;
        border-bottom-color: var(--accent) !important;
    }}
    [data-testid="stAlert"] p {{
        color: #F3F6FC !important;
    }}
\n    [data-testid="stAlert"] {{
        background: #174F96 !important;
        border: 1px solid #2368B5 !important;
        border-left: 4px solid #43C7F5 !important;
        border-radius: 2px !important;
        color: #FFFFFF !important;
        box-shadow: none !important;
    }}
    [data-testid="stAlert"] p,
    [data-testid="stAlert"] div,
    [data-testid="stAlert"] span {{
        color: #FFFFFF !important;
    }}\n\n    /* Responsive: content and Plotly charts follow the available viewport.
       Streamlit's sidebar toggle controls the sidebar; the main container stays fluid. */
    [data-testid="stAppViewContainer"] .main .block-container {{
        width: 100%;
        max-width: 1600px;
        transition: padding .18s ease, max-width .18s ease;
    }}
    [data-testid="stSidebar"][aria-expanded="false"] {{
        min-width: 0 !important;
        max-width: 0 !important;
        border-right: 0 !important;
    }}
    [data-testid="stSidebar"][aria-expanded="false"] > div {{
        visibility: hidden !important;
    }}
    @media (max-width: 900px) {{
        [data-testid="stSidebar"] {{
            min-width: 205px !important;
            max-width: 205px !important;
        }}
        .block-container {{
            padding-left: 1.2rem !important;
            padding-right: 1.2rem !important;
            padding-top: 3.9rem !important;
        }}
        .page-title {{
            font-size: 2rem;
        }}
    }}
    @media (max-width: 650px) {{
        [data-testid="stSidebar"] {{
            min-width: 190px !important;
            max-width: 190px !important;
        }}
        .block-container {{
            padding-left: .85rem !important;
            padding-right: .85rem !important;
            padding-top: 3.5rem !important;
        }}
        .page-title {{
            font-size: 1.75rem;
        }}
    }}
    </style>
    """,
    unsafe_allow_html=True,
)
# ============================================================
# ============================================================
# DATA ACCESS
# ============================================================
@st.cache_data(ttl=600, show_spinner=False)
def read_parquet_prefix(dataset_folder):
    folder = DATA_DIR / dataset_folder
    files = sorted(folder.rglob("*.parquet")) if folder.exists() else []
    frames = []
    for file_path in files:
        try:
            frames.append(pd.read_parquet(file_path))
        except Exception as exc:
            st.warning(f"Could not read dataset file {file_path.name}: {exc}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
def dataset(name):
    return read_parquet_prefix(DATASETS[name])
def normalize_risk(df):
    if df.empty:
        return df
    df = df.copy()
    if "package_name" not in df.columns and "id" in df.columns:
        df["package_name"] = df["id"]
    return df
def normalize_cascade(df):
    if df.empty:
        return df
    df = df.copy()
    if "package_name" not in df.columns and "failed_package" in df.columns:
        df["package_name"] = df["failed_package"]
    return df
def clean_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        return value.item()
    except Exception:
        return value
def clean_row(row):
    return {key: clean_value(value) for key, value in row.items()}
@st.cache_data(ttl=600, show_spinner=False)
def risk_data():
    return normalize_risk(dataset("risk_scores"))
@st.cache_data(ttl=600, show_spinner=False)
def vertices_data():
    return dataset("graph_vertices")
@st.cache_data(ttl=600, show_spinner=False)
def edges_data():
    return dataset("graph_edges")
@st.cache_data(ttl=600, show_spinner=False)
def enriched_data():
    return dataset("package_enriched")
@st.cache_data(ttl=600, show_spinner=False)
def cascade_summary_data():
    return normalize_cascade(dataset("cascade_summary"))
@st.cache_data(ttl=600, show_spinner=False)
def cascade_affected_data():
    return dataset("cascade_affected_nodes")
@st.cache_resource
def dependency_graph():
    graph = nx.DiGraph()
    edges = edges_data()
    if not edges.empty and {"src", "dst"}.issubset(edges.columns):
        for row in edges[["src", "dst"]].itertuples(index=False, name=None):
            graph.add_edge(str(row[0]), str(row[1]))
    return graph
def direct_get(path, params=None):
    params = params or {}
    parts = [unquote(part) for part in path.strip("/").split("/") if part]
    if not parts:
        return {"name": "RiskGraph", "status": "running"}
    if parts == ["overview"]:
        risk = risk_data()
        vertices = vertices_data()
        edges = edges_data()
        if risk.empty:
            return None
        levels = risk["risk_level"].astype(str).str.upper() if "risk_level" in risk.columns else pd.Series([], dtype=str)
        observed = 0
        if "has_github_observation" in risk.columns:
            observed = int(risk["has_github_observation"].fillna(False).astype(bool).sum())
        return {
            "packages": int(risk["package_name"].nunique()) if "package_name" in risk.columns else 0,
            "graph_nodes": int(len(vertices)),
            "graph_edges": int(len(edges)),
            "high_risk": int((levels == "HIGH").sum()),
            "medium_risk": int((levels == "MEDIUM").sum()),
            "low_risk": int((levels == "LOW").sum()),
            "github_observed": observed,
        }
    if parts == ["risk", "distribution"]:
        risk = risk_data()
        if risk.empty or "risk_level" not in risk.columns:
            return []
        counts = risk["risk_level"].astype(str).str.upper().value_counts()
        return [{"risk_level": str(level), "count": int(count)} for level, count in counts.items()]
    if parts == ["risk", "top"]:
        risk = risk_data()
        if risk.empty:
            return []
        if "final_risk_score" in risk.columns:
            risk = risk.sort_values("final_risk_score", ascending=False)
        limit = max(1, min(int(params.get("limit", 20)), 100))
        return [clean_row(row) for _, row in risk.head(limit).iterrows()]
    if len(parts) >= 3 and parts[0] == "package" and parts[-1] in {"dependencies", "dependents"}:
        package_name = "/".join(parts[1:-1])
        depth = max(1, min(int(params.get("depth", 1)), 3))
        graph = dependency_graph()
        search_graph = graph if parts[-1] == "dependencies" else graph.reverse(copy=False)
        visited = {package_name: 0}
        if package_name in search_graph:
            queue = [package_name]
            while queue:
                current = queue.pop(0)
                current_depth = visited[current]
                if current_depth >= depth:
                    continue
                for neighbor in search_graph.successors(current):
                    if neighbor not in visited:
                        visited[neighbor] = current_depth + 1
                        queue.append(neighbor)
        selected = set(visited)
        nodes = [{"id": node, "depth": node_depth} for node, node_depth in visited.items()]
        result_edges = [
            {"source": src, "target": dst}
            for src, dst in graph.edges()
            if src in selected and dst in selected
        ]
        return {"package": package_name, "nodes": nodes, "edges": result_edges}
    if len(parts) >= 2 and parts[0] == "package":
        package_name = "/".join(parts[1:])
        risk = risk_data()
        if risk.empty or "package_name" not in risk.columns:
            return None
        rows = risk[risk["package_name"].astype(str) == package_name]
        if rows.empty:
            return None
        result = clean_row(rows.iloc[0])
        enriched = enriched_data()
        if not enriched.empty and "package_name" in enriched.columns:
            extra_rows = enriched[enriched["package_name"].astype(str) == package_name]
            if not extra_rows.empty:
                for key, value in clean_row(extra_rows.iloc[0]).items():
                    if key not in result:
                        result[key] = value
        return result
    if parts == ["cascade", "summary"]:
        df = cascade_summary_data()
        if df.empty:
            return []
        if "total_affected" in df.columns:
            df = df.sort_values("total_affected", ascending=False)
        return [clean_row(row) for _, row in df.iterrows()]
    if len(parts) >= 2 and parts[0] == "cascade":
        package_name = "/".join(parts[1:])
        summary = cascade_summary_data()
        if summary.empty or "package_name" not in summary.columns:
            return None
        rows = summary[summary["package_name"].astype(str) == package_name]
        if rows.empty:
            return None
        affected = cascade_affected_data()
        if not affected.empty:
            if "failed_package" in affected.columns:
                affected_rows = affected[affected["failed_package"].astype(str) == package_name]
            elif "seed_package" in affected.columns:
                affected_rows = affected[affected["seed_package"].astype(str) == package_name]
            elif "package_name" in affected.columns:
                affected_rows = affected[affected["package_name"].astype(str) == package_name]
            else:
                affected_rows = pd.DataFrame()
        else:
            affected_rows = pd.DataFrame()
        return {
            "summary": clean_row(rows.iloc[0]),
            "affected_nodes": [clean_row(row) for _, row in affected_rows.iterrows()],
        }
    return None
def api_get(path, params=None, timeout=30):
    # Optional compatibility mode: use an externally hosted FastAPI if API_URL is set.
    if API_URL:
        try:
            response = requests.get(f"{API_URL}{path}", params=params, timeout=timeout)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            st.error(f"API request failed: {exc}")
            return None
    try:
        return direct_get(path, params=params)
    except Exception as exc:
        st.error(
            "Could not load RiskGraph data from the local processed Parquet files. "
            "Check that app/dashboard/data contains the required dataset folders. "
            f"Details: {exc}"
        )
        return None
@st.cache_data(ttl=60)
def get_overview():
    return api_get("/overview")
@st.cache_data(ttl=60)
def get_distribution():
    return api_get("/risk/distribution")
@st.cache_data(ttl=60)
def get_top_risk(limit=500):
    return api_get("/risk/top", {"limit": limit})
@st.cache_data(ttl=60)
def get_package(name):
    return api_get(f"/package/{name}")
@st.cache_data(ttl=60)
def get_dependencies(name, depth):
    return api_get(f"/package/{name}/dependencies", {"depth": depth})
@st.cache_data(ttl=60)
def get_dependents(name, depth):
    return api_get(f"/package/{name}/dependents", {"depth": depth})
@st.cache_data(ttl=60)
def get_cascade_summary():
    return api_get("/cascade/summary")
@st.cache_data(ttl=60)
def get_cascade(name):
    return api_get(f"/cascade/{name}")
overview = get_overview()
if not overview:
    st.error("RiskGraph data is unavailable. Check that app/dashboard/data contains the required processed Parquet files.")
    st.stop()
# ============================================================
# SIDEBAR
# ============================================================
if "page" not in st.session_state:
    st.session_state.page = "About RiskGraph"
with st.sidebar:
    st.markdown(
        """
        <div class="brand">
            <div class="brand-name">RiskGraph</div>
            <div class="brand-sub">npm dependency intelligence</div>
        </div>
        <div class="nav-label">Navigation</div>
        """,
        unsafe_allow_html=True,
    )
    navigation = [
        "About RiskGraph",
        "Overview",
        "High-Risk Packages",
        "Package Intelligence",
        "Dependency Network",
        "Cascade Analysis",
    ]
    selected = st.radio(
        "Navigation",
        navigation,
        index=navigation.index(st.session_state.page),
        label_visibility="collapsed",
        key="navigation_radio",
    )
    if selected != st.session_state.page:
        st.session_state.page = selected
        st.rerun()
    st.markdown("<div style='height:.55rem'></div>", unsafe_allow_html=True)
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown(
        """
        <div class="sidebar-footer">
            Seven-day GitHub Archive observation window.<br><br>
            RiskGraph measures systemic dependency exposure,
            not security vulnerabilities.
        </div>
        """,
        unsafe_allow_html=True,
    )
page = st.session_state.page
# Each navigation selection should open at the top of its page.
if "last_rendered_page" not in st.session_state:
    st.session_state.last_rendered_page = page
elif st.session_state.last_rendered_page != page:
    st.session_state.last_rendered_page = page
    st.components.v1.html(
        """
        <script>
        const scrollPageTop = () => {
            try {
                window.parent.scrollTo({ top: 0, left: 0, behavior: "instant" });
                const main = window.parent.document.querySelector(
                    '[data-testid="stAppViewContainer"] .main'
                );
                if (main) main.scrollTo({ top: 0, left: 0, behavior: "instant" });
            } catch (e) {}
        };
        scrollPageTop();
        </script>
        """,
        height=0,
    )
# ============================================================
# PAGE HEADER
# ============================================================
headers = {
    "About RiskGraph": (
        "About RiskGraph",
        "A short guide to the system, the data, and how to explore the dashboard."
    ),
    "Overview": (
        "Ecosystem overview",
        "See the scale of the package graph, how risk levels are distributed, and which packages rank highest. Use this as the starting point before investigating individual packages."
    ),
    "High-Risk Packages": (
        "High-risk packages",
        "Search and compare all packages classified as HIGH by the current model. These scores indicate modeled systemic exposure, not confirmed security vulnerabilities."
    ),
    "Package Intelligence": (
        "Package intelligence",
        "Look up a package to understand its risk score, graph centrality, incoming and outgoing degree, and the GitHub activity observed during the collection window."
    ),
    "Dependency Network": (
        "Dependency network",
        "Explore the package’s local dependency neighborhood. Dependencies are packages it uses; dependents are packages that rely on it. Use Both mode to compare the two directions."
    ),
    "Cascade Analysis": (
        "Cascade analysis",
        "Choose a package and inspect a structural what-if simulation showing potentially affected packages, propagation depth, and the high-risk packages reached."
    ),
}
title, subtitle = headers[page]
st.markdown(
    '<div class="page-kicker">Open-source ecosystem analytics</div>',
    unsafe_allow_html=True,
)
st.markdown(f'<h1 class="page-title">{title}</h1>', unsafe_allow_html=True)
st.markdown(
    f'<div class="page-subtitle">{subtitle}</div>',
    unsafe_allow_html=True,
)
st.markdown('<div class="top-rule"></div>', unsafe_allow_html=True)
# ============================================================
# ABOUT RISKGRAPH
# ============================================================
if page == "About RiskGraph":
    st.subheader("Dependency risk, made visible")
    st.write(
        "RiskGraph is a graph-analytics workspace for studying systemic "
        "dependency risk in the npm / JavaScript open-source ecosystem. "
        "It models packages as nodes and dependency relationships as "
        "directed edges, then combines graph structure with observed "
        "GitHub maintenance signals."
    )
    st.write(
        "The goal is not to identify ordinary software vulnerabilities. "
        "Instead, RiskGraph asks which packages are structurally important "
        "to the ecosystem and what could happen if an important dependency "
        "becomes unavailable."
    )
    st.divider()
    st.subheader("Current dataset")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Observed packages", f'{int(overview.get("packages", 0)):,}')
    with c2:
        st.metric("Graph nodes", f'{int(overview.get("graph_nodes", 0)):,}')
    with c3:
        st.metric("Dependency edges", f'{int(overview.get("graph_edges", 0)):,}')
    with c4:
        st.metric("High-risk packages", f'{int(overview.get("high_risk", 0)):,}')
    st.caption(
        "The pipeline processed approximately 15.3 million raw GitHub Archive events "
        "from a seven-day collection window. After filtering for repositories "
        "associated with the observed npm package set, approximately 16,800 relevant "
        "events across 679 repositories were retained. Raw input volume and filtered "
        "analysis volume are different measures."
    )
    st.divider()
    st.subheader("Original dataset and filtering")
    st.write(
        "The GitHub Archive input contained approximately 15.3 million events "
        "across seven UTC days. Spark processed the raw event stream and filtered "
        "it to activity associated with repositories linked to the collected npm "
        "packages. The resulting analysis set contained approximately 16,800 "
        "relevant events across 679 repositories. The 15.3 million figure describes "
        "the raw input, not the final filtered dataset."
    )
    st.write(
        "The npm side contains 4,000 collected package records. Together, the "
        "package metadata, dependency graph, and filtered GitHub activity form the "
        "data used by the graph metrics, risk model, and cascade simulations."
    )
    st.divider()
    st.subheader("How RiskGraph was built")
    st.write(
        "RiskGraph was built as a distributed data-processing and graph-analytics "
        "pipeline. Kafka handled the ingestion streams, RustFS stored the raw and "
        "processed datasets in S3-compatible object storage, and Apache Spark "
        "processed the large GitHub Archive input and prepared the package and "
        "dependency datasets."
    )
    build_steps = [
        (
            "01 · Data ingestion",
            "Collected 4,000 npm package records and ingested approximately 15.3 million "
            "GitHub Archive events covering seven UTC days through Kafka."
        ),
        (
            "02 · Object storage and processing",
            "Stored raw JSONL data in RustFS and used Spark to parse and filter GitHub "
            "events to repositories associated with the collected npm package set."
        ),
        (
            "03 · Dependency graph",
            "Created package vertices and directed dependency edges. The resulting graph "
            "contains 8,819 vertices and 15,456 edges, including external dependencies "
            "and observed packages with no edges."
        ),
        (
            "04 · Graph analytics and risk scoring",
            "Calculated in-degree, out-degree, PageRank, sampled betweenness, k-core "
            "and community labels. Combined structural metrics with observed GitHub "
            "maintenance signals using a heuristic risk model."
        ),
        (
            "05 · Cascade simulation and dashboard",
            "Used reverse dependency traversal to estimate potentially affected packages "
            "for selected failure seeds. FastAPI exposes the processed results and "
            "Streamlit presents interactive charts, package profiles and network views."
        ),
    ]
    for step_title, step_description in build_steps:
        with st.expander(step_title, expanded=False):
            st.write(step_description)
    st.caption(
        "The risk weights are heuristic design choices. Betweenness is sampled, "
        "and the cascade model is a structural what-if simulation rather than a "
        "runtime or lockfile-aware outage prediction."
    )
    st.divider()
    st.subheader("Team contributions")
    st.write(
        "Use this section to describe who implemented each part of the project. "
        "The available project context identifies the pipeline components, but it "
        "does not provide verified team-member names or an agreed person-by-person "
        "contribution split."
    )
    contribution_rows = [
        ("", "Data ingestion & storage", "Kafka ingestion, npm metadata collection, GitHub Archive ingestion, and RustFS storage"),
        ("", "Data engineering & processing", "Spark preprocessing, GitHub event filtering, and preparation of analysis datasets"),
        ("", "Graph analytics & risk", "Dependency graph construction, graph metrics, heuristic risk scoring, and cascade simulation"),
        ("", "API, dashboard & integration", "FastAPI endpoints, Streamlit frontend, interactive visualizations, and end-to-end integration"),
    ]
    st.dataframe(
        pd.DataFrame(
            contribution_rows,
            columns=["Team member name", "Workstream", "Contribution area"],
        ),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Team member name": st.column_config.TextColumn(
                "Team member name",
                help="Enter the name of the person responsible for this workstream.",
                width="medium",
            ),
            "Workstream": st.column_config.TextColumn("Workstream", width="medium"),
            "Contribution area": st.column_config.TextColumn(
                "Contribution area",
                width="large",
            ),
        },
    )
    st.caption(
        "Fill in the team member names and adjust the contribution areas to match "
        "your team's actual division of work."
    )
    st.info(
        "RiskGraph risk is a model-derived systemic dependency measure, not a security "
        "vulnerability score. Cascade results are structural simulations and do not "
        "model lockfile resolution, runtime behavior, or an actual package outage."
    )
    st.divider()
    st.subheader("Explore the workspace")
    st.write(
        "Each section answers a different question. Start with the overview, "
        "find packages worth investigating, then move into their dependency "
        "relationships and potential cascade impact."
    )
    with st.expander("About RiskGraph", expanded=False):
        st.write(
            "The landing page explains the project scope, dataset, graph "
            "direction, and how the risk score should be interpreted. Use it "
            "as a quick orientation guide whenever you need context."
        )
    with st.expander("Overview", expanded=False):
        st.write(
            "A high-level summary of the npm dependency ecosystem. Review "
            "package and edge counts, the risk distribution, and a ranked "
            "snapshot of packages with the highest model-derived scores."
        )
    with st.expander("High-Risk Packages", expanded=False):
        st.write(
            "A searchable list of packages classified as HIGH risk. Compare "
            "their risk scores and structural metrics to decide which packages "
            "deserve a closer look. A high score signals modeled systemic "
            "exposure, not a confirmed vulnerability."
        )
    with st.expander("Package Intelligence", expanded=False):
        st.write(
            "A focused profile for one package. Inspect its risk score, "
            "in-degree, out-degree, PageRank, sampled betweenness, k-core, "
            "and observed GitHub activity to understand the factors behind "
            "its classification."
        )
    with st.expander("Dependency Network", expanded=False):
        st.write(
            "An interactive, zoomable graph centered on a selected package. "
            "Dependencies are packages it uses; dependents are packages that "
            "use it. Choose Both to see incoming and outgoing relationships "
            "together, and adjust depth to change the network size."
        )
    with st.expander("Cascade Analysis", expanded=False):
        st.write(
            "A structural what-if simulation for a selected package. It shows "
            "how many packages may be reachable through reverse dependency "
            "relationships, the depth of propagation, and how many high-risk "
            "packages are in the affected set."
        )
    st.divider()
    st.subheader("Reading the dependency graph")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Dependency direction**")
        st.write(
            "An arrow A -> B means package A depends on package B. "
            "Out-degree is the number of dependencies used by A. "
            "In-degree is the number of packages that depend on A."
        )
    with c2:
        st.markdown("**Why Both mode matters**")
        st.write(
            "A package can have very few dependencies of its own while still "
            "being highly important because many other packages depend on it. "
            "Both mode makes this distinction visible."
        )
    st.divider()
    st.subheader("How the risk score is built")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**1 · Structural exposure**")
        st.write(
            "PageRank, in-degree, sampled betweenness, and k-core position "
            "describe where a package sits in the dependency graph."
        )
    with c2:
        st.markdown("**2 · Maintenance exposure**")
        st.write(
            "Observed contributors, bus-factor signals, commit activity, "
            "and recency provide a maintenance perspective."
        )
    with c3:
        st.markdown("**3 · Final risk**")
        st.write(
            "Structural and maintenance components are combined into the "
            "final 0–1 systemic risk score."
        )
    st.info(
        "RiskGraph risk is a model-derived systemic dependency measure, "
        "not a security vulnerability score. Cascade results are structural "
        "simulations and do not model lockfile resolution, runtime behavior, "
        "or an actual package outage."
    )
# ============================================================
# OVERVIEW
# ============================================================
elif page == "Overview":
    kpis = [
        ("Packages", overview.get("packages", 0), ""),
        ("Graph nodes", overview.get("graph_nodes", 0), ""),
        ("Dependency edges", overview.get("graph_edges", 0), ""),
        ("High risk", overview.get("high_risk", 0), "metric-danger"),
        ("Medium risk", overview.get("medium_risk", 0), "metric-warning"),
        ("GitHub observed", overview.get("github_observed", 0), ""),
    ]
    cols = st.columns(6)
    for col, (label, value, cls) in zip(cols, kpis):
        with col:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">{label}</div>
                    <div class="metric-value {cls}">{int(value):,}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.markdown(
        '<div class="section"><div class="section-title">Risk landscape</div>'
        '<div class="section-caption">Distribution across the observed package set.'
        '</div></div>',
        unsafe_allow_html=True,
    )
    distribution = get_distribution()
    left, right = st.columns([1.05, 1])
    with left:
        if distribution:
            df_dist = pd.DataFrame(distribution)
            fig = px.pie(
                df_dist,
                names="risk_level",
                values="count",
                hole=.68,
            )
            fig.update_layout(
                template=PLOT_TEMPLATE,
                colorway=[C["accent"], C["cyan"], C["success"], C["warning"], C["danger"]],
                paper_bgcolor=C["bg"],
                plot_bgcolor=C["plot"],
                height=390,
                margin=dict(l=10, r=10, t=20, b=10),
                legend=dict(orientation="h", y=-.05),
            )
            st.plotly_chart(fig, use_container_width=True)
    with right:
        st.markdown(
            """
            <div class="panel">
                <div class="section-title">Risk model</div>
                <br>
                <div class="note">
                    <b>Structural exposure</b><br>
                    PageRank, in-degree, sampled betweenness and k-core
                    position capture a package's position in the dependency network.
                    <br><br>
                    <b>Maintenance exposure</b><br>
                    Observed contributors, bus-factor signals, commit activity
                    and recency provide a maintenance perspective.
                    <br><br>
                    <b>Final risk</b><br>
                    Structural and maintenance components are combined into
                    the final 0–1 risk score.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown(
        '<div class="section"><div class="section-title">Highest-risk packages</div>'
        '<div class="section-caption">Top packages ranked by systemic risk score.'
        '</div></div>',
        unsafe_allow_html=True,
    )
    top = get_top_risk(20)
    if top:
        df = pd.DataFrame(top)
        columns = [
            "package_name",
            "final_risk_score",
            "risk_level",
            "in_degree",
            "pagerank",
            "kcore",
        ]
        columns = [c for c in columns if c in df.columns]
        table = df[columns].copy()
        if "final_risk_score" in table:
            table["final_risk_score"] = table["final_risk_score"].round(4)
        st.dataframe(table, use_container_width=True, hide_index=True)
# ============================================================
# HIGH-RISK PACKAGES
# ============================================================
elif page == "High-Risk Packages":
    data = get_top_risk(500)
    if data:
        df = pd.DataFrame(data)
        high = df[
            df["risk_level"].astype(str).str.upper() == "HIGH"
        ].copy()
        high = high.sort_values("final_risk_score", ascending=False)
        search = st.text_input(
            "Search high-risk packages",
            placeholder="Type a package name",
        )
        if search:
            high = high[
                high["package_name"].astype(str).str.contains(
                    search,
                    case=False,
                    na=False,
                )
            ]
        st.caption(f"Showing {len(high):,} matching HIGH-risk packages.")
        columns = [
            "package_name",
            "final_risk_score",
            "in_degree",
            "pagerank",
            "betweenness_sampled",
            "kcore",
            "event_count",
            "active_contributors",
        ]
        columns = [c for c in columns if c in high.columns]
        table = high[columns].copy()
        for column in ["final_risk_score", "pagerank", "betweenness_sampled"]:
            if column in table:
                table[column] = table[column].round(5)
        st.dataframe(
            table,
            use_container_width=True,
            hide_index=True,
            height=650,
        )
# ============================================================
# PACKAGE INTELLIGENCE
# ============================================================
elif page == "Package Intelligence":
    package = st.text_input(
        "Package",
        value="express",
        placeholder="e.g. express, debug, minimatch",
    ).strip()
    if package:
        data = get_package(package)
        if not data:
            st.warning(f"Package '{package}' was not found.")
        else:
            risk = float(data.get("final_risk_score", 0))
            level = str(data.get("risk_level", "UNKNOWN")).upper()
            risk_colors = {
                "HIGH": "#FF4D5F",
                "MEDIUM": "#FF9F43",
                "LOW": "#35D07F",
            }
            risk_color = risk_colors.get(level, C["muted"])
            risk_label = f"{level} RISK" if level in risk_colors else "RISK UNKNOWN"
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(135deg, #111927 0%, #172235 100%);
                    border: 1px solid #26354A;
                    border-left: 4px solid {risk_color};
                    border-radius: 14px;
                    padding: 1.2rem 1.35rem;
                    box-shadow: 0 12px 30px rgba(0,0,0,.16);
                ">
                    <div style="font-size:1.65rem;font-weight:790;color:#F3F6FC;line-height:1.25;">
                        {data.get("package_name", package)}
                    </div>
                    <div style="color:#A2AEC2;font-size:.84rem;margin-top:.35rem;">
                        Version {data.get("version", "unknown")}
                    </div>
                    <div style="display:flex;align-items:center;gap:.7rem;flex-wrap:wrap;margin-top:1.15rem;">
                        <span style="
                            display:inline-flex;align-items:center;
                            padding:.36rem .65rem;border-radius:6px;
                            background:{risk_color}1A;border:1px solid {risk_color}80;
                            color:{risk_color};font-size:.78rem;font-weight:800;
                            letter-spacing:.04em;
                        ">{risk_label}</span>
                        <span style="color:#526078;">|</span>
                        <span style="font-size:1.05rem;font-weight:780;color:#F3F6FC;">
                            {risk:.4f}
                        </span>
                        <span style="color:#A2AEC2;font-size:.76rem;">systemic risk score</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.write("")
            a, b, c, d = st.columns(4)
            a.metric("Risk score", f"{risk:.3f}")
            b.metric("In-degree", data.get("in_degree", 0))
            c.metric("Out-degree", data.get("out_degree", 0))
            d.metric("GitHub events", data.get("event_count", 0))
            st.markdown(
                '<div class="section"><div class="section-title">Risk composition'
                '</div></div>',
                unsafe_allow_html=True,
            )
            component_df = pd.DataFrame(
                {
                    "component": ["Structural", "Maintenance"],
                    "score": [
                        data.get("centrality_risk", 0),
                        data.get("maintenance_risk", 0),
                    ],
                }
            )
            fig = px.bar(
                component_df,
                x="component",
                y="score",
                range_y=[0, 1],
                text_auto=".3f",
            )
            fig.update_layout(
                template=PLOT_TEMPLATE,
                colorway=[C["accent"], C["cyan"], C["success"], C["warning"], C["danger"]],
                paper_bgcolor=C["bg"],
                plot_bgcolor=C["plot"],
                height=330,
                margin=dict(l=10, r=10, t=20, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("PageRank", f"{data.get('pagerank', 0):.6f}")
            m2.metric(
                "Betweenness",
                f"{data.get('betweenness_sampled', 0):.6f}",
            )
            m3.metric("K-core", data.get("kcore", 0))
            m4.metric("Bus factor", data.get("bus_factor", 0))
            st.info(
                "GitHub values describe activity observed during the selected "
                "seven-day Archive window."
            )
# ============================================================
# DEPENDENCY NETWORK
# ============================================================
elif page == "Dependency Network":
    st.markdown(
        '<div class="section"><div class="section-title">Dependency network'
        '</div><div class="section-caption">'
        'Both mode combines incoming dependents and outgoing dependencies around the selected package.'
        '</div></div>',
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns([2.1, 1, 1])
    with c1:
        graph_package = st.text_input(
            "Package",
            value="express",
            key="graph_package",
            placeholder="e.g. express, debug, execa",
        ).strip()
    with c2:
        graph_mode = st.selectbox(
            "View",
            ["Both", "Dependencies", "Dependents"],
            index=0,
        )
    with c3:
        depth = st.selectbox("Depth", [1, 2, 3], index=0)
    package_data = get_package(graph_package) if graph_package else None
    if package_data:
        a, b, c = st.columns(3)
        a.metric("In-degree", package_data.get("in_degree", 0))
        b.metric("Out-degree", package_data.get("out_degree", 0))
        c.metric(
            "Total degree",
            int(package_data.get("in_degree", 0))
            + int(package_data.get("out_degree", 0)),
        )
    st.markdown(
        """
        <div class="graph-legend">
            <span><span class="dot" style="background:#FF4D8D;"></span>Selected package</span>
            <span><span class="dot" style="background:#43C7F5;"></span>Dependencies (what it uses)</span>
            <span><span class="dot" style="background:#B28DFF;"></span>Dependents (what uses it)</span>
            <span><span class="dot" style="background:#4B586B;"></span>Other visible nodes</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    dependencies = (
        get_dependencies(graph_package, depth)
        if graph_mode in ["Both", "Dependencies"]
        else None
    )
    dependents = (
        get_dependents(graph_package, depth)
        if graph_mode in ["Both", "Dependents"]
        else None
    )
    node_map = {
        graph_package: {
            "id": graph_package,
            "relationship": "selected",
        }
    }
    edge_set = set()
    edge_roles = {}
    def ingest_network(payload, relationship):
        if not payload:
            return
        for node in payload.get("nodes", []):
            if not isinstance(node, dict):
                continue
            node_id = node.get("id") or node.get("name")
            if node_id:
                node_map[node_id] = {
                    **node_map.get(node_id, {}),
                    **node,
                    "relationship": relationship,
                }
        for edge in payload.get("edges", []):
            if not isinstance(edge, dict):
                continue
            source = (
                edge.get("source")
                or edge.get("src")
                or edge.get("from")
            )
            target = (
                edge.get("target")
                or edge.get("dst")
                or edge.get("to")
            )
            if source and target:
                edge_set.add((source, target))
                edge_roles[(source, target)] = relationship
    ingest_network(dependencies, "dependency")
    ingest_network(dependents, "dependent")
    graph = nx.DiGraph()
    graph.add_nodes_from(node_map.keys())
    graph.add_edges_from(edge_set)
    if graph.number_of_nodes() > 1:
        positions = nx.spring_layout(
            graph,
            seed=42,
            k=1.65,
            iterations=150,
        )
        positions[graph_package] = (0.0, 0.0)
        edge_traces = []
        for role, edge_color in [
            ("dependency", C["dependency"]),
            ("dependent", C["dependent"]),
        ]:
            edge_x, edge_y = [], []
            for source, target in graph.edges():
                if edge_roles.get((source, target)) != role:
                    continue
                x0, y0 = positions[source]
                x1, y1 = positions[target]
                edge_x += [x0, x1, None]
                edge_y += [y0, y1, None]
            if edge_x:
                edge_traces.append(
                    go.Scatter(
                        x=edge_x,
                        y=edge_y,
                        mode="lines",
                        line=dict(width=0.8, color="#667085"),
                        opacity=0.42,
                        hoverinfo="none",
                        name="Dependencies" if role == "dependency" else "Dependents",
                    )
                )
        node_x, node_y = [], []
        labels, hover, sizes, colors = [], [], [], []
        for node in graph.nodes():
            x, y = positions[node]
            node_x.append(x)
            node_y.append(y)
            labels.append(node)
            local_in = graph.in_degree(node)
            local_out = graph.out_degree(node)
            hover.append(
                f"<b>{node}</b><br>"
                f"In-degree in view: {local_in}<br>"
                f"Out-degree in view: {local_out}"
            )
            relationship = node_map.get(node, {}).get("relationship", "")
            if node == graph_package:
                sizes.append(40)
                colors.append("#FF4D8D")
            elif relationship == "dependency":
                sizes.append(23)
                colors.append("#43C7F5")
            elif relationship == "dependent":
                sizes.append(23)
                colors.append("#B28DFF")
            else:
                sizes.append(13)
                colors.append("#4B586B")
        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=labels,
            textposition="top center",
            hovertext=hover,
            hoverinfo="text",
            marker=dict(
                size=sizes,
                color=colors,
                line=dict(width=1.1, color="#0B1220"),
            ),
            textfont=dict(
                size=10,
                color="#E6EAF2",
            ),
        )
        fig = go.Figure(data=edge_traces + [node_trace])
        fig.update_layout(
            template=PLOT_TEMPLATE,
                colorway=[C["accent"], C["cyan"], C["success"], C["warning"], C["danger"]],
            paper_bgcolor=C["bg"],
            plot_bgcolor=C["plot"],
            height=700,
            margin=dict(l=5, r=5, t=10, b=5),
            showlegend=False,
            dragmode="pan",
            hovermode="closest",
            xaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
            ),
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": True,
            },
        )
        st.caption(
            f"{graph_package} is the selected package. "
            f"This view contains {graph.number_of_nodes():,} visible nodes "
            f"and {graph.number_of_edges():,} relationships. "
            "A -> B means A depends on B."
        )
    else:
        st.warning("No dependency relationships were returned.")
# ============================================================
# CASCADE ANALYSIS
# ============================================================
elif page == "Cascade Analysis":
    cascade = get_cascade_summary()
    if cascade:
        df = pd.DataFrame(cascade)
        if not df.empty:
            df = df.sort_values("total_affected", ascending=False)
            fig = px.bar(
                df.head(20),
                x="failed_package",
                y="total_affected",
                title="Potentially affected packages",
                labels={
                    "failed_package": "Failed package",
                    "total_affected": "Affected packages",
                },
            )
            fig.update_layout(
                template=PLOT_TEMPLATE,
                colorway=[C["accent"], C["cyan"], C["success"], C["warning"], C["danger"]],
                paper_bgcolor=C["bg"],
                plot_bgcolor=C["plot"],
                height=430,
                margin=dict(l=10, r=10, t=50, b=20),
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
            )
            st.markdown(
                '<div class="section"><div class="section-title">'
                'Investigate a cascade</div></div>',
                unsafe_allow_html=True,
            )
            selected = st.selectbox(
                "Package",
                df["failed_package"].tolist(),
            )
            result = get_cascade(selected)
            if result:
                summary = result.get("summary", {})
                a, b, c, d, e = st.columns(5)
                a.metric("Risk score", f'{summary.get("risk_score", 0):.3f}')
                b.metric(
                    "Directly affected",
                    summary.get("directly_affected", 0),
                )
                c.metric(
                    "Total affected",
                    summary.get("total_affected", 0),
                )
                d.metric(
                    "High-risk affected",
                    summary.get("high_risk_affected", 0),
                )
                e.metric(
                    "Cascade depth",
                    summary.get("cascade_depth", 0),
                )
                affected = result.get("affected_nodes", [])
                if affected:
                    affected_df = pd.DataFrame(affected)
                    columns = [
                        "id",
                        "depth",
                        "final_risk_score",
                        "risk_level",
                        "is_high_risk",
                    ]
                    columns = [
                        c for c in columns
                        if c in affected_df.columns
                    ]
                    st.dataframe(
                        affected_df[columns].sort_values("depth"),
                        use_container_width=True,
                        hide_index=True,
                        height=520,
                    )
            st.info(
                "Cascade results are structural simulations. They do not "
                "model npm lockfiles, version resolution, optional dependencies, "
                "runtime behavior, or actual package outages."
            )
