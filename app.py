#!/usr/bin/env python3
"""UTM Performance Dashboard — Redash Direct Integration (Daily Granularity)"""
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

# Daily UTM — 방문일 기준 일별 UV/전환 집계 (1d/3d/7d 기여기간 동시 계산)
DAILY_UTM_SQL = """
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
purchases AS (
  SELECT anonymousid, "timestamp" as purchase_ts
  FROM soo_segment.segment_log
  WHERE event = 'purchaseFinView'
),
daily_metrics AS (
  SELECT
    u.utm_content,
    u.visit_date,
    COUNT(DISTINCT u.anonymousid) AS unique_visitors,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL
      AND p.purchase_ts <= DATEADD(day, 1, u.visit_ts) THEN u.anonymousid END) AS purchase_1d,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL
      AND p.purchase_ts <= DATEADD(day, 3, u.visit_ts) THEN u.anonymousid END) AS purchase_3d,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL THEN u.anonymousid END) AS purchase_7d
  FROM utm_visits u
  LEFT JOIN purchases p ON u.anonymousid = p.anonymousid
    AND p.purchase_ts >= u.visit_ts
    AND p.purchase_ts <= DATEADD(day, 7, u.visit_ts)
  WHERE u.utm_content IS NOT NULL AND u.utm_content != ''
  GROUP BY u.utm_content, u.visit_date
),
content_totals AS (
  SELECT
    u.utm_content,
    COUNT(DISTINCT u.anonymousid) AS total_uv,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL
      AND p.purchase_ts <= DATEADD(day, 1, u.visit_ts) THEN u.anonymousid END) AS total_purchase_1d,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL
      AND p.purchase_ts <= DATEADD(day, 3, u.visit_ts) THEN u.anonymousid END) AS total_purchase_3d,
    COUNT(DISTINCT CASE WHEN p.anonymousid IS NOT NULL THEN u.anonymousid END) AS total_purchase_7d
  FROM utm_visits u
  LEFT JOIN purchases p ON u.anonymousid = p.anonymousid
    AND p.purchase_ts >= u.visit_ts
    AND p.purchase_ts <= DATEADD(day, 7, u.visit_ts)
  WHERE u.utm_content IS NOT NULL AND u.utm_content != ''
  GROUP BY u.utm_content
)
SELECT
  COALESCE(p.utm_source, 'unknown') AS utm_source,
  COALESCE(p.utm_medium, 'unknown') AS utm_medium,
  COALESCE(p.utm_campaign, 'unknown') AS utm_campaign,
  d.utm_content,
  d.visit_date,
  d.unique_visitors,
  d.purchase_1d,
  d.purchase_3d,
  d.purchase_7d,
  ROUND(d.purchase_1d::DECIMAL / NULLIF(d.unique_visitors, 0) * 100, 2) AS cvr_1d,
  ROUND(d.purchase_3d::DECIMAL / NULLIF(d.unique_visitors, 0) * 100, 2) AS cvr_3d,
  ROUND(d.purchase_7d::DECIMAL / NULLIF(d.unique_visitors, 0) * 100, 2) AS cvr_7d,
  ct.total_uv,
  ct.total_purchase_1d,
  ct.total_purchase_3d,
  ct.total_purchase_7d
FROM daily_metrics d
JOIN params p ON d.utm_content = p.utm_content AND p.rn = 1
LEFT JOIN content_totals ct ON d.utm_content = ct.utm_content
ORDER BY d.visit_date DESC, d.unique_visitors DESC
""".strip()

# Revenue + Products SQL (utm_content별 매출/품목 — 1d/3d/7d 조건부 집계)
REVENUE_PRODUCTS_SQL = """
WITH utm_visits AS (
  SELECT DISTINCT anonymousid,
    CASE WHEN context_page_url LIKE '%&utm_content=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_content=', 2), '&', 1)
      WHEN context_page_url LIKE '%?utm_content=%'
      THEN SPLIT_PART(SPLIT_PART(context_page_url, 'utm_content=', 2), '&', 1)
      ELSE NULL END AS utm_content,
    "timestamp" as visit_ts,
    DATE("timestamp") as visit_date
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
  SELECT u.utm_content, u.visit_date, p.purchase_amount, p.product_name, p.messageid,
    MAX(CASE WHEN p.purchase_ts <= DATEADD(day, 1, u.visit_ts) THEN 1 ELSE 0 END) AS within_1d,
    MAX(CASE WHEN p.purchase_ts <= DATEADD(day, 3, u.visit_ts) THEN 1 ELSE 0 END) AS within_3d
  FROM utm_visits u JOIN purchases p ON u.anonymousid = p.anonymousid
    AND p.purchase_ts >= u.visit_ts
    AND p.purchase_ts <= DATEADD(day, 7, u.visit_ts)
  WHERE u.utm_content IS NOT NULL
  GROUP BY u.utm_content, u.visit_date, p.purchase_amount, p.product_name, p.messageid
)
SELECT utm_content, visit_date, product_name,
  SUM(CASE WHEN within_1d = 1 THEN 1 ELSE 0 END) AS cnt_1d,
  SUM(CASE WHEN within_3d = 1 THEN 1 ELSE 0 END) AS cnt_3d,
  COUNT(*) AS cnt_7d,
  SUM(CASE WHEN within_1d = 1 THEN purchase_amount ELSE 0 END) AS revenue_1d,
  SUM(CASE WHEN within_3d = 1 THEN purchase_amount ELSE 0 END) AS revenue_3d,
  SUM(purchase_amount) AS revenue_7d
FROM utm_purchases
GROUP BY utm_content, visit_date, product_name
ORDER BY utm_content, visit_date DESC, cnt_7d DESC
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

def _aggregate_rev_rows(rows: list, days: int = 3) -> tuple:
    """이미 필터된 revenue 행의 매출/품목 집계. days=1,3,7 기여기간 지원."""
    cnt_col = f"cnt_{days}d"
    rev_col = f"revenue_{days}d"
    product_counts = defaultdict(int)
    total_revenue = 0
    for row in rows:
        cnt = row.get(cnt_col, row.get("cnt", 0)) or 0
        rev = row.get(rev_col, row.get("revenue", 0)) or 0
        if cnt == 0:
            continue
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

def _build_dataframe(daily_rows, rev_rows):
    """Redash 일별 데이터 → 대시보드용 DataFrame (1행 = utm_content × visit_date)"""
    df = pd.DataFrame(daily_rows)
    df = df.rename(columns={"unique_visitors": "UV"})
    for col in ["utm_source", "utm_medium", "utm_campaign", "utm_content"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: unquote_plus(str(x)) if pd.notna(x) else "")
    df["UV"] = pd.to_numeric(df["UV"], errors="coerce").fillna(0).astype(int)

    # 3개 기여기간별 전환/CVR
    for d in [1, 3, 7]:
        pcol = f"purchase_{d}d"
        ccol = f"cvr_{d}d"
        df[pcol] = pd.to_numeric(df.get(pcol, 0), errors="coerce").fillna(0).astype(int)
        df[ccol] = pd.to_numeric(df.get(ccol, 0.0), errors="coerce").fillna(0.0)

    # utm_content 전체 UV/전환 (드릴다운용)
    df["UV_total"] = pd.to_numeric(df.get("total_uv", 0), errors="coerce").fillna(0).astype(int)
    for d in [1, 3, 7]:
        df[f"purchase_total_{d}d"] = pd.to_numeric(df.get(f"total_purchase_{d}d", 0), errors="coerce").fillna(0).astype(int)

    # Revenue pre-index: (utm_content, visit_date)별 + utm_content 전체
    rev_by_cd = defaultdict(list)   # (content, date) → rows
    rev_by_c = defaultdict(list)    # content → rows
    for row in rev_rows:
        c = row["utm_content"]
        vd = str(row.get("visit_date", ""))
        rev_by_cd[(c, vd)].append(row)
        rev_by_c[c].append(row)

    for d in [1, 3, 7]:
        # 날짜별 매출 (드릴다운용)
        date_rev_cache = {}
        for key, rows in rev_by_cd.items():
            date_rev_cache[key] = _aggregate_rev_rows(rows, days=d)
        df[f"결제금액_{d}d"] = df.apply(
            lambda x, dd=d: date_rev_cache.get((x["utm_content"], str(x["visit_date"])), (0, "-"))[0], axis=1)
        df[f"결제품목_{d}d"] = df.apply(
            lambda x, dd=d: date_rev_cache.get((x["utm_content"], str(x["visit_date"])), (0, "-"))[1], axis=1)

        # utm_content 전체 매출 (KPI/요약 테이블용)
        total_rev_cache = {}
        for c, rows in rev_by_c.items():
            total_rev_cache[c] = _aggregate_rev_rows(rows, days=d)
        df[f"결제금액_total_{d}d"] = df["utm_content"].map(lambda x, dd=d: total_rev_cache.get(x, (0, "-"))[0])
        df[f"결제품목_total_{d}d"] = df["utm_content"].map(lambda x, dd=d: total_rev_cache.get(x, (0, "-"))[1])

    df["날짜_dt"] = pd.to_datetime(df["visit_date"], errors="coerce")
    return df

@st.cache_data(ttl=600, show_spinner="데이터를 가져오는 중...")
def load_data():
    """이중 모드: Redash 직접 → 실패 시 캐시 CSV 폴백"""
    api_key = os.getenv("REDASH_API_KEY")

    # ── Mode 1: Redash Direct (로컬) ──
    if api_key:
        try:
            client = RedashClient(REDASH_BASE_URL, api_key)
            main_rows = client.execute_adhoc_query(REDASH_DATA_SOURCE_ID, DAILY_UTM_SQL)
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
            if os.path.exists(CACHE_PATH):
                pass  # 아래 캐시 로드로 진행
            else:
                return pd.DataFrame(), f"Redash 오류: {e}", None

    # ── Mode 2: Cache CSV (Streamlit Cloud / Redash 실패) ──
    if os.path.exists(CACHE_PATH):
        try:
            df = pd.read_csv(CACHE_PATH)
            if "visit_date" not in df.columns and "first_visit" in df.columns:
                return pd.DataFrame(), "캐시가 구버전입니다. 로컬에서 먼저 실행하여 캐시를 갱신해주세요.", None
            df["UV"] = pd.to_numeric(df["UV"], errors="coerce").fillna(0).astype(int)
            # 멀티윈도우 컬럼 파싱
            for d in [1, 3, 7]:
                p = f"purchase_{d}d"
                c = f"cvr_{d}d"
                if p in df.columns:
                    df[p] = pd.to_numeric(df[p], errors="coerce").fillna(0).astype(int)
                    df[c] = pd.to_numeric(df.get(c, 0.0), errors="coerce").fillna(0.0)
                rc = f"결제금액_{d}d"
                if rc in df.columns:
                    df[rc] = pd.to_numeric(df[rc], errors="coerce").fillna(0).astype(int)
                # total 컬럼 하위호환: 없으면 기존 결제금액(utm_content 전체)으로 폴백
                rtc = f"결제금액_total_{d}d"
                if rtc in df.columns:
                    df[rtc] = pd.to_numeric(df[rtc], errors="coerce").fillna(0).astype(int)
                elif rc in df.columns:
                    df[rtc] = df[rc]
                    df[f"결제품목_total_{d}d"] = df.get(f"결제품목_{d}d", "-")
                # purchase_total 하위호환
                ptc = f"purchase_total_{d}d"
                if ptc in df.columns:
                    df[ptc] = pd.to_numeric(df[ptc], errors="coerce").fillna(0).astype(int)
                elif p in df.columns:
                    df[ptc] = df.groupby("utm_content")[p].transform("sum")
            # UV_total 하위호환 (근사치: 날짜별 UV 합산)
            if "UV_total" not in df.columns:
                df["UV_total"] = df.groupby("utm_content")["UV"].transform("sum")
            else:
                df["UV_total"] = pd.to_numeric(df["UV_total"], errors="coerce").fillna(0).astype(int)
            # 구버전 CSV 호환 (purchase_1d 없으면 기존 컬럼 매핑)
            if "purchase_3d" not in df.columns and "결제완료" in df.columns:
                df["purchase_3d"] = pd.to_numeric(df["결제완료"], errors="coerce").fillna(0).astype(int)
                df["cvr_3d"] = pd.to_numeric(df.get("CVR_num", 0.0), errors="coerce").fillna(0.0)
                df["purchase_1d"] = df["purchase_3d"]
                df["purchase_7d"] = df["purchase_3d"]
                df["cvr_1d"] = df["cvr_3d"]
                df["cvr_7d"] = df["cvr_3d"]
                if "결제금액_num" in df.columns:
                    for d in [1, 3, 7]:
                        df[f"결제금액_{d}d"] = pd.to_numeric(df["결제금액_num"], errors="coerce").fillna(0).astype(int)
                        df[f"결제품목_{d}d"] = df.get("결제품목", "-")
            df["날짜_dt"] = pd.to_datetime(df["visit_date"], errors="coerce")
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
# Dashboard Logic (수정된 함수)
# ─────────────────────────────────────────
def render_dashboard(df, data_source="redash"):
    # 기여기간에 따라 사용할 컬럼명 결정
    attr_options = {"+1일": 1, "+3일": 3, "+7일": 7}

    # 인라인 필터 (expander 제거 → 항상 노출)
    f_attr, f_date = st.columns([1, 2.5])
    with f_attr:
        attr_label = st.radio("기여기간", list(attr_options.keys()), index=1, horizontal=True)
    with f_date:
        data_min_d = df["날짜_dt"].min().date() if not df.empty else datetime.now().date()
        data_max_d = df["날짜_dt"].max().date() if not df.empty else datetime.now().date()
        date_range = st.date_input("조회 기간", value=(data_min_d, data_max_d))

    attr_days = attr_options[attr_label]
    pcol = f"purchase_{attr_days}d"  # 전환 컬럼
    ccol = f"cvr_{attr_days}d"       # CVR 컬럼
    rcol = f"결제금액_{attr_days}d"  # 날짜별 매출 컬럼
    prcol = f"결제품목_{attr_days}d" # 날짜별 품목 컬럼
    rcol_total = f"결제금액_total_{attr_days}d"  # utm_content 전체 매출
    prcol_total = f"결제품목_total_{attr_days}d" # utm_content 전체 품목
    pcol_total = f"purchase_total_{attr_days}d"  # utm_content 전체 전환

    badge_src = "Redash Direct" if data_source == "redash" else f"Cache ({data_source})"
    st.markdown(f'<span class="data-source-badge">{badge_src} · 일별 집계 · {attr_days}일 기여</span>', unsafe_allow_html=True)

    # 캐스케이드 필터: 상위 필터가 하위 필터 옵션을 동적으로 제한
    fc1, fc2, fc3 = st.columns(3)
    sel_src = fc1.selectbox("Source", ["전체"] + sorted(df["utm_source"].unique()))
    # Source 선택 시 해당 Source의 Medium/Campaign만 옵션으로 제공
    _filt = df if sel_src == "전체" else df[df["utm_source"] == sel_src]
    sel_med = fc2.selectbox("Medium", ["전체"] + sorted(_filt["utm_medium"].unique()))
    _filt2 = _filt if sel_med == "전체" else _filt[_filt["utm_medium"] == sel_med]
    sel_cam = fc3.selectbox("Campaign", ["전체"] + sorted(_filt2["utm_campaign"].unique()))

    fdf = df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        fdf = fdf[(fdf["날짜_dt"].dt.date >= date_range[0]) & (fdf["날짜_dt"].dt.date <= date_range[1])]
    if sel_src != "전체": fdf = fdf[fdf["utm_source"] == sel_src]
    if sel_med != "전체": fdf = fdf[fdf["utm_medium"] == sel_med]
    if sel_cam != "전체": fdf = fdf[fdf["utm_campaign"] == sel_cam]

    # 핵심 KPI 3개 (크게)
    uv = fdf["UV"].sum()
    pay = fdf[pcol].sum() if pcol in fdf.columns else 0
    _rcol_kpi = rcol_total if rcol_total in fdf.columns else rcol
    rev_by_content = fdf.drop_duplicates(subset=["utm_content"])[_rcol_kpi].sum() if _rcol_kpi in fdf.columns else 0
    k1, k2, k3 = st.columns(3)
    k1.metric("Total 매출", fmt_currency(rev_by_content))
    k2.metric("Avg CVR", f"{(pay/uv*100 if uv>0 else 0):.2f}%")
    k3.metric("Total UV", f"{uv:,}")
    # 보조 KPI (작게, 텍스트)
    st.markdown(f'<span style="color:#888; font-size:13px;">전환 {pay:,}건 · Active UTM {fdf["utm_content"].nunique():,}개</span>', unsafe_allow_html=True)

    # Main Chart
    c1, c2 = st.columns([2.2, 1])

    with c1:
        st.markdown('<div class="section-hd">UV & 전환 트렌드</div>', unsafe_allow_html=True)
        t_unit = st.radio("단위", ["일간", "주간", "월간"], index=0, horizontal=True, label_visibility="collapsed")

        chart_df = fdf.copy()
        if t_unit == "일간": chart_df["g"] = chart_df["날짜_dt"].dt.strftime("%y.%m.%d")
        elif t_unit == "월간": chart_df["g"] = chart_df["날짜_dt"].dt.to_period("M").dt.start_time.dt.strftime("%y년 %m월")
        else: chart_df["g"] = chart_df["날짜_dt"].dt.to_period("W").dt.start_time.dt.strftime("%y.%m.%d") + "(주)"

        grp = chart_df.groupby("g").agg(UV=("UV","sum"), pay=(pcol,"sum")).reset_index().sort_values("g")

        if "selected_points" not in st.session_state: st.session_state.selected_points = []

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=grp["g"], y=[grp["UV"].max()*1.2]*len(grp), marker_color="rgba(0,0,0,0)", hoverinfo="skip", showlegend=False), secondary_y=False)
        colors = ["#C5A774" if x not in st.session_state.selected_points else "#E5D4B0" for x in grp["g"]]
        fig.add_trace(go.Bar(x=grp["g"], y=grp["UV"], name="UV", marker_color=colors, text=grp["UV"], textposition="outside"), secondary_y=False)
        # 전환 0건인 날짜는 None 처리 → 선 보간 방지
        pay_display = grp["pay"].apply(lambda v: v if v > 0 else None)
        fig.add_trace(go.Scatter(x=grp["g"], y=pay_display, name="전환", mode="lines+markers", connectgaps=False, line=dict(color="#FF3333", width=3), marker=dict(size=12, color="#FF3333", line=dict(color="white", width=2))), secondary_y=True)

        for _, r in grp.iterrows():
            if r["pay"] > 0:
                is_sel = r["g"] in st.session_state.selected_points
                fig.add_annotation(x=r["g"], y=r["pay"], yref="y2", text=f"<b>{int(r['pay'])}건</b>", showarrow=False, yshift=25,
                                   bgcolor="#FF3333" if not is_sel else "white", font=dict(color="white" if not is_sel else "#FF3333", size=13), borderpad=5)

        fig.update_layout(PLOTLY_LAYOUT, height=420, hovermode="x", margin=dict(l=55, r=15, t=15, b=50), xaxis=dict(showgrid=False, tickangle=-45))
        fig.update_yaxes(secondary_y=False, range=[0, grp["UV"].max()*1.3] if not grp.empty else [0, 1])
        fig.update_yaxes(secondary_y=True, range=[0, grp["pay"].max()*2.5] if not grp.empty and grp["pay"].max() > 0 else [0, 1], showgrid=False)

        st.plotly_chart(fig, use_container_width=True)

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

    # 전환 발생 날짜 multiselect
    available_dates = grp[grp["pay"] > 0]["g"].tolist()
    if available_dates:
        selected_dates = st.multiselect(
            "상세 보기 날짜 선택",
            options=available_dates,
            default=[d for d in st.session_state.get("selected_points", []) if d in available_dates],
            placeholder="전환 발생 날짜를 선택하세요...",
        )
        st.session_state.selected_points = selected_dates
    else:
        st.session_state.selected_points = []

    # ── Drill-down View (항상 표시) ──
    st.markdown('<div class="section-hd">기간별 상세 성과</div>', unsafe_allow_html=True)
    if not st.session_state.selected_points:
        st.info("위 날짜 선택에서 전환 발생 날짜를 선택하면 상세 성과를 확인할 수 있습니다.")
    else:
        with st.container(border=True):
            tags_html = "".join([f'<span class="date-tag">{p}</span>' for p in st.session_state.selected_points])
            st.markdown(tags_html, unsafe_allow_html=True)

            # 선택 기간 집계 KPI (차트 그룹 데이터 기준)
            sel_grp = grp[grp["g"].isin(st.session_state.selected_points)]
            if not sel_grp.empty:
                st.write("")
                sk1, sk2, sk3 = st.columns(3)
                sel_uv = sel_grp["UV"].sum()
                sel_pay = sel_grp["pay"].sum()
                sk1.metric("선택 UV", f"{sel_uv:,}")
                sk2.metric("선택 전환", f"{sel_pay:,}")
                sk3.metric("선택 CVR", f"{(sel_pay/sel_uv*100 if sel_uv>0 else 0):.2f}%")

            # 선택된 날짜의 utm_content별 집계 (선택 날짜 범위 내 성과만 표시)
            detail_raw = chart_df[chart_df["g"].isin(st.session_state.selected_points)]
            if not detail_raw.empty:
                # 선택 날짜 내 utm_content별 UV/전환 합산
                detail_df = detail_raw.groupby(
                    ["utm_content", "utm_campaign", "utm_source", "utm_medium"]
                ).agg(
                    UV=("UV", "sum"),
                    결제완료=(pcol, "sum"),
                ).reset_index()

                detail_df["CVR_num"] = (detail_df["결제완료"] / detail_df["UV"].replace(0, float('nan')) * 100).fillna(0).round(2)
                detail_df["CVR"] = detail_df["CVR_num"].apply(lambda x: f"{x:.2f}%")

                # 결제금액: 선택 날짜 범위 매출 합산
                if rcol in detail_raw.columns:
                    rev_sum = detail_raw.groupby("utm_content")[rcol].apply(
                        lambda x: pd.to_numeric(x, errors="coerce").sum()
                    ).reset_index(name="결제금액_num")
                    detail_df = detail_df.merge(rev_sum, on="utm_content", how="left")
                    detail_df["결제금액"] = detail_df["결제금액_num"].fillna(0).apply(
                        lambda x: f"₩{int(x):,}" if x > 0 else "-")
                else:
                    detail_df["결제금액"] = "-"

                # 결제품목: 선택 날짜의 품목 병합
                if prcol in detail_raw.columns:
                    prod_merge = detail_raw.groupby("utm_content")[prcol].apply(
                        lambda x: ", ".join(sorted(set(
                            str(v) for v in x if pd.notna(v) and str(v) not in ["-", "nan", ""]
                        ))) or "-"
                    ).reset_index(name="결제품목")
                    detail_df = detail_df.merge(prod_merge, on="utm_content", how="left")
                else:
                    detail_df["결제품목"] = "-"

                # 전환수 내림차순 → UV 내림차순 정렬
                detail_df = detail_df.sort_values(by=["결제완료", "UV"], ascending=[False, False])
                
                # 차트용 데이터 (전환이 1건 이상인 것만)
                conv_chart_df = detail_df[detail_df["결제완료"] > 0]

                if not conv_chart_df.empty:
                    st.write("")
                    sc1, sc2 = st.columns(2)
                    drill_margin = dict(l=60, r=15, t=40, b=40)
                    with sc1:
                        fig_conv = px.bar(conv_chart_df, x="utm_content", y="결제완료", title="전환 건수", color_discrete_sequence=["#FF3333"], text_auto="d")
                        fig_conv.update_layout(PLOTLY_LAYOUT, margin=drill_margin, height=320, xaxis_tickangle=-30)
                        st.plotly_chart(fig_conv, use_container_width=True)
                    with sc2:
                        fig_cvr = px.bar(conv_chart_df, x="utm_content", y="CVR_num", title="CVR (%)", color_discrete_sequence=["#C5A774"], text_auto=".2f")
                        fig_cvr.update_layout(PLOTLY_LAYOUT, margin=drill_margin, height=320, xaxis_tickangle=-30)
                        fig_cvr.update_yaxes(ticksuffix="%")
                        st.plotly_chart(fig_cvr, use_container_width=True)

                st.write("")
                st.markdown('<div class="section-hd" style="margin-top:5px;">선택 날짜 UTM 상세 성과 (전환 없는 UTM 포함)</div>', unsafe_allow_html=True)
                st.dataframe(
                    detail_df[["utm_content", "utm_campaign", "utm_source", "UV", "결제완료", "CVR", "결제금액", "결제품목"]].reset_index(drop=True),
                    use_container_width=True, hide_index=True
                )
            else: st.info("선택한 날짜에 데이터가 없습니다.")

    st.markdown('<div class="section-spacer"></div>', unsafe_allow_html=True)
    c_sub1, c_sub2 = st.columns([2, 1])
    with c_sub1:
        st.markdown('<div class="section-hd">캠페인 성과</div>', unsafe_allow_html=True)
        cp = fdf.groupby("utm_campaign").agg(UV=("UV","sum"), pay=(pcol,"sum")).reset_index().sort_values("UV")
        fig_cp = go.Figure()
        fig_cp.add_trace(go.Bar(y=cp["utm_campaign"], x=cp["UV"], orientation="h", marker_color="#C5A774", name="UV"))
        fig_cp.update_layout(PLOTLY_LAYOUT, height=max(300, len(cp)*30), margin=dict(l=120, r=15, t=10, b=25))
        st.plotly_chart(fig_cp, use_container_width=True)
    with c_sub2:
        st.markdown('<div class="section-hd">미디엄 비중</div>', unsafe_allow_html=True)
        md = fdf.groupby("utm_medium")["UV"].sum().reset_index()
        st.plotly_chart(px.pie(md, values="UV", names="utm_medium", hole=0.4, color_discrete_sequence=CHART_PALETTE).update_layout(PLOTLY_LAYOUT), use_container_width=True)

# ─────────────────────────────────────────
# 전체 UTM 기록 (df 전체 데이터 기반)
# ─────────────────────────────────────────
def render_all_utm(df):
    """전체 UTM 자산 관리 — df(전체 데이터)를 직접 받아 독립 필터링"""
    # 독립 기여기간 선택
    attr_opts = {"+1일": 1, "+3일": 3, "+7일": 7}
    ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([1.2, 1, 1, 1])
    with ctrl1:
        query = st.text_input("UTM 검색", placeholder="utm_content 검색...", key="all_utm_search")
    with ctrl2:
        view_mode = st.radio("뷰", ["기본", "상세"], horizontal=True, key="all_utm_view")
    with ctrl3:
        group_by = st.radio("그룹", ["content별", "campaign별", "source별"], horizontal=True, key="all_utm_group")
    with ctrl4:
        attr_label = st.radio("기여기간", list(attr_opts.keys()), index=1, horizontal=True, key="all_utm_attr")
    attr_days = attr_opts[attr_label]
    pcol = f"purchase_{attr_days}d"
    rcol_t = f"결제금액_total_{attr_days}d"
    prcol_t = f"결제품목_total_{attr_days}d"

    # 데이터 소스 배지
    utm_count = df["utm_content"].nunique()
    date_min = df["날짜_dt"].min().strftime("%Y-%m-%d") if not df.empty else "-"
    date_max = df["날짜_dt"].max().strftime("%Y-%m-%d") if not df.empty else "-"
    st.markdown(f'<span class="data-source-badge">{utm_count}개 UTM · 전체기간 {date_min} ~ {date_max}</span>', unsafe_allow_html=True)

    # 검색 필터
    wdf = df.copy()
    if query:
        wdf = wdf[wdf["utm_content"].str.contains(query, case=False, na=False)]

    # 그룹핑
    if group_by == "content별":
        group_cols = ["utm_content", "utm_source", "utm_medium", "utm_campaign"]
    elif group_by == "campaign별":
        group_cols = ["utm_campaign"]
    else:
        group_cols = ["utm_source"]

    agg_dict = {
        "최초유입": ("날짜_dt", "min"),
        "최근유입": ("날짜_dt", "max"),
        "UV": ("UV", "sum"),
        "결제완료": (pcol, "sum") if pcol in wdf.columns else ("UV", lambda x: 0),
    }

    # content별일 때만 결제금액/품목 표시 (그룹핑 시 문자열 합산 불가)
    _rcol = rcol_t if rcol_t in wdf.columns else f"결제금액_{attr_days}d"
    _prcol = prcol_t if prcol_t in wdf.columns else f"결제품목_{attr_days}d"
    if group_by == "content별" and _rcol in wdf.columns:
        agg_dict["결제금액_num"] = (_rcol, "first")
    if group_by == "content별" and _prcol in wdf.columns:
        agg_dict["결제품목"] = (_prcol, "first")

    summary = wdf.groupby(group_cols).agg(**agg_dict).reset_index()
    summary["CVR_num"] = (summary["결제완료"] / summary["UV"].replace(0, float('nan')) * 100).fillna(0).round(2)
    summary["CVR"] = summary["CVR_num"].apply(lambda x: f"{x:.2f}%")
    summary["최초유입"] = summary["최초유입"].dt.strftime("%Y-%m-%d")
    summary["최근유입"] = summary["최근유입"].dt.strftime("%Y-%m-%d")

    if "결제금액_num" in summary.columns:
        summary["결제금액"] = summary["결제금액_num"].apply(lambda x: f"₩{int(x):,}" if pd.notna(x) and x > 0 else "-")
    else:
        summary["결제금액"] = "-"
    if "결제품목" not in summary.columns:
        summary["결제품목"] = "-"

    # 컬럼 구성 (뷰 모드별)
    if group_by == "content별":
        if view_mode == "상세":
            cols = ["최근유입", "최초유입", "utm_content", "utm_source", "utm_medium", "utm_campaign", "UV", "결제완료", "CVR", "결제금액", "결제품목"]
        else:
            cols = ["utm_content", "utm_source", "UV", "결제완료", "CVR", "결제금액"]
    else:
        base_col = "utm_campaign" if group_by == "campaign별" else "utm_source"
        if view_mode == "상세":
            cols = ["최근유입", "최초유입", base_col, "UV", "결제완료", "CVR"]
        else:
            cols = [base_col, "UV", "결제완료", "CVR"]

    # 존재하는 컬럼만 필터
    cols = [c for c in cols if c in summary.columns]
    display_df = summary[cols].sort_values("최근유입" if "최근유입" in cols else "UV", ascending=False)

    tbl_height = min(800, max(420, len(display_df) * 35 + 50))
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=tbl_height)

    # CSV 다운로드
    csv = display_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("CSV 다운로드", csv, f"utm_all_{group_by}_{attr_days}d.csv", "text/csv", key="all_utm_csv")

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
        t1, t2, t3 = st.tabs(["성과 분석", "전체 UTM 기록", "UTM 생성"])
        with t1: render_dashboard(df, data_source)
        with t2: render_all_utm(df)
        with t3: render_gen()
    else: st.error(f"데이터 로드 실패: {err}")

if __name__ == "__main__": main()
