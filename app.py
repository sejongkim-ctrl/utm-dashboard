#!/usr/bin/env python3
"""UTM Performance Dashboard — Redash Direct Integration"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import re
import time
import requests
from collections import defaultdict
from urllib.parse import urlencode, unquote_plus
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
SPREADSHEET_ID = "1MTS1Aa8NmAbcvnpPs78LsQmAImSLbSHwEp5QbKE7JbI"
SHEET_NAME = "UTM생성기"

# Redash
REDASH_BASE_URL = "https://redash.datastream.co.kr"
REDASH_DATA_SOURCE_ID = 3
JOB_POLL_INTERVAL = 3
JOB_TIMEOUT = 180

# Full UTM Parameters + Performance (utm_content별 source/medium/campaign + UV/전환/CVR)
FULL_UTM_SQL = """
WITH utm_visits AS (
  SELECT
    anonymousid,
    CASE WHEN context_page_url LIKE '%utm_source=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_source=', 2), '&', 1)
      ELSE NULL END AS utm_source,
    CASE WHEN context_page_url LIKE '%utm_medium=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_medium=', 2), '&', 1)
      ELSE NULL END AS utm_medium,
    CASE WHEN context_page_url LIKE '%utm_campaign=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_campaign=', 2), '&', 1)
      ELSE NULL END AS utm_campaign,
    CASE WHEN context_page_url LIKE '%utm_content=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_content=', 2), '&', 1)
      ELSE NULL END AS utm_content,
    DATE("timestamp") as visit_date,
    "timestamp" as visit_ts
  FROM soo_segment.segment_log
  WHERE context_page_url LIKE '%utm_content=%'
),
params AS (
  SELECT
    utm_content, utm_source, utm_medium, utm_campaign,
    ROW_NUMBER() OVER (
      PARTITION BY utm_content
      ORDER BY COUNT(*) DESC
    ) AS rn
  FROM utm_visits
  WHERE utm_content IS NOT NULL AND utm_content != ''
  GROUP BY utm_content, utm_source, utm_medium, utm_campaign
),
visitors AS (
  SELECT
    utm_content,
    MIN(visit_date) as first_visit,
    MAX(visit_date) as last_visit,
    COUNT(DISTINCT anonymousid) AS unique_visitors
  FROM utm_visits
  WHERE utm_content IS NOT NULL AND utm_content != ''
  GROUP BY utm_content
),
purchases AS (
  SELECT anonymousid, "timestamp" as purchase_ts
  FROM soo_segment.segment_log
  WHERE event = 'purchaseFinView'
),
conversions AS (
  SELECT
    u.utm_content,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL THEN u.anonymousid END) AS purchase_complete
  FROM utm_visits u
  LEFT JOIN purchases p ON u.anonymousid = p.anonymousid
    AND p.purchase_ts >= u.visit_ts
    AND p.purchase_ts <= DATEADD(day, 3, u.visit_ts)
  WHERE u.utm_content IS NOT NULL AND u.utm_content != ''
  GROUP BY u.utm_content
)
SELECT
  COALESCE(p.utm_source, 'unknown') AS utm_source,
  COALESCE(p.utm_medium, 'unknown') AS utm_medium,
  COALESCE(p.utm_campaign, 'unknown') AS utm_campaign,
  v.utm_content,
  v.first_visit,
  v.last_visit,
  v.unique_visitors,
  COALESCE(c.purchase_complete, 0) AS purchase_complete,
  ROUND(COALESCE(c.purchase_complete, 0)::DECIMAL / NULLIF(v.unique_visitors, 0) * 100, 2) AS cvr_pct
FROM visitors v
JOIN params p ON v.utm_content = p.utm_content AND p.rn = 1
LEFT JOIN conversions c ON v.utm_content = c.utm_content
ORDER BY v.unique_visitors DESC
""".strip()

# Revenue + Products SQL (utm_content별 매출/품목 상세)
REVENUE_PRODUCTS_SQL = """
WITH utm_visits AS (
  SELECT DISTINCT anonymousid,
    CASE WHEN context_page_url LIKE '%&utm_content=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_content=', 2), '&', 1)
      WHEN context_page_url LIKE '%?utm_content=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_content=', 2), '&', 1)
      ELSE NULL END AS utm_content,
    "timestamp" as visit_ts
  FROM soo_segment.segment_log WHERE context_page_url LIKE '%utm_content=%'
),
purchases AS (
  SELECT anonymousid,
    properties."purchaseAmount"::BIGINT AS purchase_amount,
    properties."productName"::VARCHAR AS product_name,
    "timestamp" as purchase_ts,
    messageid
  FROM soo_segment.segment_log WHERE event = 'purchaseFinView' AND properties IS NOT NULL
),
utm_purchases AS (
  SELECT DISTINCT u.utm_content, p.purchase_amount, p.product_name, p.messageid
  FROM utm_visits u JOIN purchases p ON u.anonymousid = p.anonymousid
    AND p.purchase_ts >= u.visit_ts
    AND p.purchase_ts <= DATEADD(day, 3, u.visit_ts)
  WHERE u.utm_content IS NOT NULL
)
SELECT utm_content, product_name, COUNT(*) as cnt, SUM(purchase_amount) as revenue
FROM utm_purchases
GROUP BY utm_content, product_name
ORDER BY utm_content, cnt DESC
""".strip()

# Cache (Redash 결과를 CSV로 저장 → Streamlit Cloud에서 읽기)
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CACHE_PATH = os.path.join(CACHE_DIR, "utm_performance.csv")

CHART_PALETTE = ["#C5A774", "#891C21", "#4ECDC4", "#45B7D1", "#D4636C", "#96648C", "#7BC67E", "#E5D4B0", "#FF9F43", "#6B1419", "#A68B5B", "#FF6B6B"]
PLOTLY_LAYOUT = dict(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#ccc"), margin=dict(l=55, r=15, t=10, b=25))

st.set_page_config(page_title="UTM Performance Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

# ─────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────
st.markdown("""<style>
#MainMenu, footer {visibility: hidden;}
.block-container {padding-top: 2.5rem; padding-bottom: 2rem;}

@keyframes fadeSlideUp { 0% { opacity: 0; transform: translateY(12px); } 100% { opacity: 1; transform: translateY(0); } }
.stPlotlyChart, .stDataFrame, [data-testid="stMetric"], .section-hd { animation: fadeSlideUp 0.45s ease-out forwards; }

/* KPI 메트릭 카드 */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid rgba(197, 167, 116, 0.2);
    border-radius: 12px;
    padding: 18px 20px;
    text-align: center;
    transition: border-color 0.2s;
}
[data-testid="stMetric"]:hover { border-color: rgba(197, 167, 116, 0.5); }
[data-testid="stMetricLabel"] { font-size: 11px !important; color: #777 !important; text-transform: uppercase; letter-spacing: 0.5px; justify-content: center !important; }
[data-testid="stMetricValue"] { font-size: 28px !important; font-weight: 700 !important; justify-content: center !important; }

/* 섹션 헤딩 */
.section-hd { font-size: 15px; font-weight: 600; color: #C5A774; margin: 28px 0 10px; padding-bottom: 6px; border-bottom: 1px solid rgba(197, 167, 116, 0.15); }

/* 데이터 테이블 */
.stDataFrame { font-size: 13px; }

/* 차트 클릭 안내 */
.click-hint-badge {
    background-color: rgba(255, 51, 51, 0.12);
    color: #FF6B6B;
    padding: 8px 14px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 600;
    display: inline-block;
    margin-bottom: 12px;
    border: 1px solid rgba(255, 51, 51, 0.25);
}

/* 드릴다운 컨테이너 (st.container border 스타일 오버라이드) */
[data-testid="stVerticalBlockBorderWrapper"] {
    border-color: rgba(197, 167, 116, 0.25) !important;
    border-radius: 14px !important;
}

/* 날짜 태그 */
.date-tag {
    display: inline-block;
    background-color: rgba(197, 167, 116, 0.18);
    color: #C5A774;
    padding: 3px 12px;
    border-radius: 6px;
    border: 1px solid rgba(197, 167, 116, 0.5);
    margin-right: 6px;
    margin-bottom: 6px;
    font-size: 13px;
    font-weight: 600;
}

/* 데이터 소스 배지 */
.data-source-badge {
    display: inline-block;
    background-color: rgba(78, 205, 196, 0.12);
    color: #4ECDC4;
    padding: 4px 12px;
    border-radius: 6px;
    font-size: 11px;
    font-weight: 600;
    border: 1px solid rgba(78, 205, 196, 0.25);
    margin-bottom: 8px;
}

/* 섹션 간 여백 */
.section-spacer { margin-top: 20px; }
</style>""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# Redash Client (embedded)
# ─────────────────────────────────────────
class RedashClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Key {api_key}"})

    def execute_adhoc_query(self, data_source_id: int, sql: str) -> list:
        resp = self.session.post(
            f"{self.base_url}/api/query_results",
            json={"data_source_id": data_source_id, "query": sql, "max_age": 0},
            timeout=30,
        )
        resp.raise_for_status()
        return self._handle_response(resp.json())

    def _handle_response(self, data: dict) -> list:
        if "query_result" in data:
            return data["query_result"]["data"]["rows"]
        if "job" in data:
            result_id = self._poll_job(data["job"]["id"])
            return self._get_results(result_id)
        raise RuntimeError(f"Unexpected Redash response: {list(data.keys())}")

    def _poll_job(self, job_id: str) -> str:
        elapsed = 0
        while elapsed < JOB_TIMEOUT:
            time.sleep(JOB_POLL_INTERVAL)
            elapsed += JOB_POLL_INTERVAL
            resp = self.session.get(f"{self.base_url}/api/jobs/{job_id}", timeout=10)
            resp.raise_for_status()
            job = resp.json().get("job", resp.json())
            status = job.get("status")
            if status == 3:
                return str(job["query_result_id"])
            if status == 4:
                raise RuntimeError(f"Redash query failed: {job.get('error', 'unknown')}")
        raise RuntimeError(f"Redash job timed out ({JOB_TIMEOUT}s)")

    def _get_results(self, result_id: str) -> list:
        resp = self.session.get(f"{self.base_url}/api/query_results/{result_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()["query_result"]["data"]["rows"]

# ─────────────────────────────────────────
# Product Name Cleaner (embedded)
# ─────────────────────────────────────────
def clean_product_name(raw: str) -> str:
    name = raw.strip()
    name = re.sub(r"^\[.*?\]\s*", "", name)
    name = name.replace(" 수壽", "")
    return " ".join(name.split())

def aggregate_products(revenue_rows: list, utm_content: str) -> tuple:
    product_counts = defaultdict(int)
    total_revenue = 0
    for row in revenue_rows:
        if row["utm_content"] != utm_content:
            continue
        cnt = row["cnt"]
        rev = row.get("revenue") or 0
        total_revenue += rev
        for p in str(row.get("product_name", "")).split(","):
            cleaned = clean_product_name(p)
            if cleaned:
                product_counts[cleaned] += cnt
    if not product_counts:
        return 0, "-"
    sorted_items = sorted(product_counts.items(), key=lambda x: -x[1])
    formatted = ", ".join(f"{name} ({count})" for name, count in sorted_items)
    return total_revenue, formatted

# ─────────────────────────────────────────
# Data Engine
# ─────────────────────────────────────────
def get_credentials():
    token_json = st.secrets.get("GOOGLE_TOKEN_JSON") or os.getenv("GOOGLE_TOKEN_JSON")
    if not token_json:
        for p in [os.path.join(os.path.dirname(__file__), "token.json"), os.path.expanduser("~/token.json")]:
            if os.path.exists(p):
                with open(p) as f: token_json = f.read(); break
    if not token_json: raise ValueError("TOKEN_NOT_FOUND")
    creds = Credentials.from_authorized_user_info(json.loads(token_json))
    if creds.expired and creds.refresh_token: creds.refresh(Request())
    return creds

def _build_dataframe(main_rows, rev_rows):
    """Redash 원시 데이터 → 대시보드용 DataFrame 변환"""
    df = pd.DataFrame(main_rows)
    df = df.rename(columns={
        "unique_visitors": "UV",
        "purchase_complete": "결제완료",
        "cvr_pct": "CVR_num",
    })
    for col in ["utm_source", "utm_medium", "utm_campaign", "utm_content"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: unquote_plus(str(x)) if pd.notna(x) else "")
    df["UV"] = pd.to_numeric(df["UV"], errors="coerce").fillna(0).astype(int)
    df["결제완료"] = pd.to_numeric(df["결제완료"], errors="coerce").fillna(0).astype(int)
    df["CVR_num"] = pd.to_numeric(df["CVR_num"], errors="coerce").fillna(0.0)
    df["CVR"] = df["CVR_num"].apply(lambda x: f"{x:.2f}%")

    rev_map = {}
    for content in df["utm_content"].unique():
        total_rev, products = aggregate_products(rev_rows, content)
        rev_map[content] = (total_rev, products)
    df["결제금액_num"] = df["utm_content"].map(lambda x: rev_map.get(x, (0, "-"))[0])
    df["결제금액"] = df["결제금액_num"].apply(lambda x: f"₩{x:,}" if x > 0 else "-")
    df["결제품목"] = df["utm_content"].map(lambda x: rev_map.get(x, (0, "-"))[1])

    df["날짜_dt"] = pd.to_datetime(df.get("first_visit", pd.Series(dtype=str)), errors="coerce")
    df["최근유입_dt"] = pd.to_datetime(df.get("last_visit", pd.Series(dtype=str)), errors="coerce")
    return df

@st.cache_data(ttl=600, show_spinner="데이터를 가져오는 중...")
def load_data():
    """이중 모드: Redash 직접 → 실패 시 캐시 CSV 폴백"""
    api_key = os.getenv("REDASH_API_KEY")

    # ── Mode 1: Redash Direct (로컬) ──
    if api_key:
        try:
            client = RedashClient(REDASH_BASE_URL, api_key)
            main_rows = client.execute_adhoc_query(REDASH_DATA_SOURCE_ID, FULL_UTM_SQL)
            if not main_rows:
                return pd.DataFrame(), "Redash에서 반환된 데이터가 없습니다.", None
            rev_rows = client.execute_adhoc_query(REDASH_DATA_SOURCE_ID, REVENUE_PRODUCTS_SQL)
            for row in rev_rows:
                row["utm_content"] = unquote_plus(str(row.get("utm_content", "")))

            df = _build_dataframe(main_rows, rev_rows)

            # 캐시 저장 (Streamlit Cloud용)
            os.makedirs(CACHE_DIR, exist_ok=True)
            df.to_csv(CACHE_PATH, index=False)

            return df, None, "redash"
        except requests.exceptions.ConnectionError:
            pass  # 캐시 폴백
        except Exception as e:
            # Redash 실패 시 캐시 시도
            if os.path.exists(CACHE_PATH):
                pass  # 아래 캐시 로드로 진행
            else:
                return pd.DataFrame(), f"Redash 오류: {e}", None

    # ── Mode 2: Cache CSV (Streamlit Cloud / Redash 실패) ──
    if os.path.exists(CACHE_PATH):
        try:
            df = pd.read_csv(CACHE_PATH)
            df["UV"] = pd.to_numeric(df["UV"], errors="coerce").fillna(0).astype(int)
            df["결제완료"] = pd.to_numeric(df["결제완료"], errors="coerce").fillna(0).astype(int)
            df["CVR_num"] = pd.to_numeric(df["CVR_num"], errors="coerce").fillna(0.0)
            df["CVR"] = df["CVR_num"].apply(lambda x: f"{x:.2f}%")
            df["결제금액_num"] = pd.to_numeric(df["결제금액_num"], errors="coerce").fillna(0).astype(int)
            df["날짜_dt"] = pd.to_datetime(df["first_visit"], errors="coerce")
            df["최근유입_dt"] = pd.to_datetime(df["last_visit"], errors="coerce")
            mtime = datetime.fromtimestamp(os.path.getmtime(CACHE_PATH)).strftime("%m/%d %H:%M")
            return df, None, f"cache · {mtime}"
        except Exception as e:
            return pd.DataFrame(), f"캐시 읽기 실패: {e}", None

    if not api_key:
        return pd.DataFrame(), "REDASH_API_KEY 미설정 + 캐시 없음. 로컬에서 먼저 실행해주세요.", None
    return pd.DataFrame(), "Redash 연결 실패 + 캐시 없음.", None

def fmt_currency(v):
    if v >= 100_000_000: return f"{v/100_000_000:.1f}억원"
    if v >= 10_000: return f"{v/10_000:.0f}만원"
    return f"{v:,}원"

# ─────────────────────────────────────────
# Dashboard Logic
# ─────────────────────────────────────────
def render_dashboard(df, data_source="redash"):
    badge_text = "Redash Direct · 3일 기여기간" if data_source == "redash" else f"Cached Data ({data_source})"
    st.markdown(f'<span class="data-source-badge">{badge_text}</span>', unsafe_allow_html=True)

    with st.expander("🔍 상세 필터", expanded=True):
        st.markdown("<span style='font-size: 13px; color: #aaa;'>💡 <b>Tip:</b> 드롭다운 클릭 후 키보드로 <b>직접 텍스트를 입력</b>하여 빠르게 검색할 수 있습니다.</span>", unsafe_allow_html=True)
        st.write("")

        v_dates = df["날짜_dt"].dropna()
        min_d, max_d = (v_dates.min().date(), v_dates.max().date()) if not v_dates.empty else (datetime.now().date(), datetime.now().date())
        date_range = st.date_input("조회 기간", value=(min_d, max_d))

        st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        sel_src = c1.selectbox("Source 🔍", ["전체"] + sorted(df["utm_source"].unique()))
        sel_med = c2.selectbox("Medium 🔍", ["전체"] + sorted(df["utm_medium"].unique()))
        sel_cam = c3.selectbox("Campaign 🔍", ["전체"] + sorted(df["utm_campaign"].unique()))

    fdf = df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        fdf = fdf[(fdf["날짜_dt"].dt.date >= date_range[0]) & (fdf["날짜_dt"].dt.date <= date_range[1])]
    if sel_src != "전체": fdf = fdf[fdf["utm_source"] == sel_src]
    if sel_med != "전체": fdf = fdf[fdf["utm_medium"] == sel_med]
    if sel_cam != "전체": fdf = fdf[fdf["utm_campaign"] == sel_cam]

    k1, k2, k3, k4, k5 = st.columns(5)
    uv, pay = fdf["UV"].sum(), fdf["결제완료"].sum()
    k1.metric("Total UV", f"{uv:,}")
    k2.metric("Total 결제", f"{pay:,}")
    k3.metric("Avg CVR", f"{(pay/uv*100 if uv>0 else 0):.2f}%")
    k4.metric("Total 매출", fmt_currency(fdf["결제금액_num"].sum()))
    k5.metric("Active UTM", f"{(fdf['UV']>0).sum():,}")

    # Main Chart
    c1, c2 = st.columns([2.2, 1])

    with c1:
        st.markdown('<div class="section-hd">UV & 전환 트렌드</div>', unsafe_allow_html=True)
        st.markdown('<div class="click-hint-badge">👆 차트의 빨간색 <b>전환값(n건)을 클릭</b>하면 상세 내용 확인이 가능합니다.</div>', unsafe_allow_html=True)

        t_unit = st.radio("단위", ["일간", "주간", "월간"], index=0, horizontal=True, label_visibility="collapsed")

        chart_df = fdf.copy()
        if t_unit == "일간": chart_df["g"] = chart_df["날짜_dt"].dt.strftime("%y.%m.%d")
        elif t_unit == "월간": chart_df["g"] = chart_df["날짜_dt"].dt.to_period("M").dt.start_time.dt.strftime("%y년 %m월")
        else: chart_df["g"] = chart_df["날짜_dt"].dt.to_period("W").dt.start_time.dt.strftime("%y.%m.%d") + "(주)"

        grp = chart_df.groupby("g").agg(UV=("UV","sum"), pay=("결제완료","sum")).reset_index().sort_values("g")

        if "selected_points" not in st.session_state: st.session_state.selected_points = []

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=grp["g"], y=[grp["UV"].max()*1.2]*len(grp), marker_color="rgba(0,0,0,0)", hoverinfo="skip", showlegend=False), secondary_y=False)
        colors = ["#C5A774" if x not in st.session_state.selected_points else "#E5D4B0" for x in grp["g"]]
        fig.add_trace(go.Bar(x=grp["g"], y=grp["UV"], name="UV", marker_color=colors, text=grp["UV"], textposition="outside"), secondary_y=False)
        fig.add_trace(go.Scatter(x=grp["g"], y=grp["pay"], name="전환", mode="lines+markers", line=dict(color="#FF3333", width=3), marker=dict(size=12, color="#FF3333", line=dict(color="white", width=2))), secondary_y=True)

        for _, r in grp.iterrows():
            if r["pay"] > 0:
                is_sel = r["g"] in st.session_state.selected_points
                fig.add_annotation(x=r["g"], y=r["pay"], yref="y2", text=f"<b>{int(r['pay'])}건</b>", showarrow=False, yshift=25,
                                   bgcolor="#FF3333" if not is_sel else "white", font=dict(color="white" if not is_sel else "#FF3333", size=13), borderpad=5)

        fig.update_layout(PLOTLY_LAYOUT, height=420, hovermode="x", clickmode="event+select", margin=dict(l=55, r=15, t=15, b=50), xaxis=dict(showgrid=False, tickangle=-45))
        fig.update_yaxes(secondary_y=False, range=[0, grp["UV"].max()*1.3] if not grp.empty else [0, 1])
        fig.update_yaxes(secondary_y=True, range=[0, grp["pay"].max()*2.5] if not grp.empty and grp["pay"].max() > 0 else [0, 1], showgrid=False)

        sel = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="main_chart")
        if sel and "selection" in sel and sel["selection"]["points"]:
            clicked_x = sel["selection"]["points"][0]["x"]
            if clicked_x in st.session_state.selected_points: st.session_state.selected_points.remove(clicked_x)
            else: st.session_state.selected_points.append(clicked_x)
            st.rerun()

    with c2:
        st.markdown('<div class="section-hd">소스별 UV</div>', unsafe_allow_html=True)
        src = fdf.groupby("utm_source")["UV"].sum().reset_index().sort_values("UV", ascending=False)
        src = src[src["UV"] > 0]
        if not src.empty:
            fig = px.pie(src, values="UV", names="utm_source", color_discrete_sequence=CHART_PALETTE, hole=0.45)
            fig.update_layout(PLOTLY_LAYOUT, height=370, showlegend=True, legend=dict(font=dict(size=11)))
            fig.update_traces(textposition="inside", textinfo="percent+label", textfont_size=11)
            st.plotly_chart(fig, use_container_width=True)
        else: st.info("데이터가 없습니다.")

    # ── Drill-down View ──
    if st.session_state.selected_points:
        st.write("")
        st.divider()
        st.write("")

        with st.container(border=True):
            st.markdown("#### 🎯 선택된 기간 상세 성과")

            tag_cols = st.columns([0.12, 0.88])
            with tag_cols[0]:
                if st.button("선택 초기화 ✖️", key="clear_btn"): st.session_state.selected_points = []; st.rerun()
            with tag_cols[1]:
                tags_html = "".join([f'<span class="date-tag">{p}</span>' for p in st.session_state.selected_points])
                st.markdown(tags_html, unsafe_allow_html=True)

            detail_df = chart_df[chart_df["g"].isin(st.session_state.selected_points)].sort_values("결제완료", ascending=False)

            # 선택 기간 집계 KPI
            sel_grp = grp[grp["g"].isin(st.session_state.selected_points)]
            if not sel_grp.empty:
                st.write("")
                sk1, sk2, sk3, sk4 = st.columns(4)
                sel_uv = sel_grp["UV"].sum()
                sel_pay = sel_grp["pay"].sum()
                sk1.metric("선택 UV", f"{sel_uv:,}")
                sk2.metric("선택 전환", f"{sel_pay:,}")
                sk3.metric("선택 CVR", f"{(sel_pay/sel_uv*100 if sel_uv>0 else 0):.2f}%")
                sel_rev = detail_df["결제금액_num"].sum()
                sk4.metric("선택 매출", fmt_currency(sel_rev))

            if not detail_df.empty:
                conv_df = detail_df[detail_df["결제완료"] > 0]
                if not conv_df.empty:
                    st.write("")
                    sc1, sc2 = st.columns(2)
                    drill_margin = dict(l=60, r=15, t=40, b=40)
                    with sc1:
                        fig_cvr = px.bar(conv_df, x="utm_content", y="CVR_num", title="CVR (%)", color_discrete_sequence=["#C5A774"], text_auto=".2f")
                        fig_cvr.update_layout(PLOTLY_LAYOUT, margin=drill_margin, height=320, xaxis_tickangle=-30)
                        fig_cvr.update_yaxes(ticksuffix="%")
                        st.plotly_chart(fig_cvr, use_container_width=True)
                    with sc2:
                        fig_rev = px.bar(conv_df, x="utm_content", y="결제금액_num", title="매출액", color_discrete_sequence=["#891C21"], text_auto=",.0f")
                        fig_rev.update_layout(PLOTLY_LAYOUT, margin=drill_margin, height=320, xaxis_tickangle=-30)
                        fig_rev.update_yaxes(tickformat=",d", ticksuffix="원")
                        st.plotly_chart(fig_rev, use_container_width=True)

                st.write("")
                display_detail = detail_df.copy()
                display_detail["최초유입"] = display_detail["날짜_dt"].dt.strftime("%Y-%m-%d")
                st.dataframe(display_detail[["최초유입", "utm_content", "utm_campaign", "utm_source", "UV", "결제완료", "CVR", "결제금액", "결제품목"]].reset_index(drop=True), use_container_width=True, hide_index=True)
            else: st.info("선택한 날짜에 데이터가 없습니다.")

    st.markdown('<div class="section-spacer"></div>', unsafe_allow_html=True)
    c_sub1, c_sub2 = st.columns([2, 1])
    with c_sub1:
        st.markdown('<div class="section-hd">캠페인 성과</div>', unsafe_allow_html=True)
        cp = fdf.groupby("utm_campaign").agg(UV=("UV","sum"), pay=("결제완료","sum")).reset_index().sort_values("UV")
        fig_cp = go.Figure()
        fig_cp.add_trace(go.Bar(y=cp["utm_campaign"], x=cp["UV"], orientation="h", marker_color="#C5A774", name="UV"))
        fig_cp.update_layout(PLOTLY_LAYOUT, height=max(300, len(cp)*30), margin=dict(l=120, r=15, t=10, b=25))
        st.plotly_chart(fig_cp, use_container_width=True)
    with c_sub2:
        st.markdown('<div class="section-hd">미디엄 비중</div>', unsafe_allow_html=True)
        md = fdf.groupby("utm_medium")["UV"].sum().reset_index()
        st.plotly_chart(px.pie(md, values="UV", names="utm_medium", hole=0.4, color_discrete_sequence=CHART_PALETTE).update_layout(PLOTLY_LAYOUT), use_container_width=True)

    st.markdown('<div class="section-hd">전체 UTM 데이터</div>', unsafe_allow_html=True)
    all_disp = fdf.copy()
    all_disp["최초유입"] = all_disp["날짜_dt"].dt.strftime("%Y-%m-%d")
    all_disp["최근유입"] = all_disp["최근유입_dt"].dt.strftime("%Y-%m-%d")

    all_cols = ["최초유입", "최근유입", "utm_source", "utm_medium", "utm_campaign", "utm_content", "UV", "결제완료", "CVR", "결제금액", "결제품목"]
    default_visible_cols = ["최초유입", "utm_content", "utm_source", "UV", "결제완료", "CVR", "결제금액", "결제품목"]

    st.dataframe(
        all_disp[all_cols].sort_values("최초유입", ascending=False),
        use_container_width=True,
        hide_index=True,
        height=420,
        column_order=default_visible_cols
    )

# ─────────────────────────────────────────
# UTM Generator (Google Sheets 기록용)
# ─────────────────────────────────────────
def render_gen():
    st.markdown('<div class="section-hd">UTM 링크 생성기</div>', unsafe_allow_html=True)
    with st.container(border=True):
        col1, col2 = st.columns(2)

        with col1:
            creator = st.text_input("생성자 👤", placeholder="예: 홍길동")
            url = st.text_input("랜딩 URL", "https://thesoo.co/")
            src = st.selectbox("Source", ["kakao", "naver", "instagram", "facebook", "blog", "직접입력"])
            if src=="직접입력": src = st.text_input("Source 입력")

        with col2:
            cam = st.text_input("Campaign", placeholder="예: 2602_seolevent")
            cnt = st.text_input("Content", placeholder="예: 260226_kakao")
            med = st.selectbox("Medium", ["text", "image", "banner", "video", "instant", "직접입력"])
            if med=="직접입력": med = st.text_input("Medium 입력")

        memo = st.text_input("메모 (시트 전용)")

        if url and src and med and cam and cnt:
            params = {"utm_source": src, "utm_medium": med, "utm_campaign": cam, "utm_content": cnt}
            final_url = f"{url}{'&' if '?' in url else '?'}{urlencode(params)}"
            st.code(final_url, language=None)

            if st.button("🚀 구글 시트에 추가", use_container_width=True):
                if not creator:
                    st.warning("⚠️ '생성자' 항목을 먼저 입력해주세요!")
                else:
                    row = [
                        datetime.now().strftime("%Y. %m. %d"),  # A: 생성일
                        creator,                                 # B: 생성자
                        url,                                     # C: 랜딩 URL
                        src,                                     # D: Source
                        med,                                     # E: Medium
                        cam,                                     # F: Campaign
                        cnt,                                     # G: Content
                        0,                                       # H: UV
                        0,                                       # I: 결제완료
                        "0%",                                    # J: CVR
                        "-",                                     # K: 결제금액
                        "-",                                     # L: 결제품목
                        final_url,                               # M: 완성 URL
                        "",                                      # N: bit
                        memo                                     # O: 메모
                    ]
                    try:
                        creds = get_credentials()
                        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
                        service.spreadsheets().values().append(
                            spreadsheetId=SPREADSHEET_ID,
                            range=f"'{SHEET_NAME}'!A:A",
                            valueInputOption="USER_ENTERED",
                            insertDataOption="INSERT_ROWS",
                            body={"values": [row]}
                        ).execute()
                        st.success("✅ 구글 시트에 성공적으로 전송되었습니다."); st.cache_data.clear()
                    except Exception as e:
                        st.error(f"저장 실패: {e}")

def main():
    df, err, data_source = load_data()
    if not df.empty:
        t1, t2 = st.tabs(["📊 Performance", "🔗 UTM Generator"])
        with t1: render_dashboard(df, data_source)
        with t2: render_gen()
    else: st.error(f"데이터 로드 실패: {err}")

if __name__ == "__main__": main()
