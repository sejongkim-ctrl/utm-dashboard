#!/usr/bin/env python3
"""UTM Performance Dashboard

Google Sheets UTM 데이터를 시각화하는 Streamlit 대시보드.
- KPI 카드 (UV, 전환, CVR, 매출)
- 소스/미디엄/캠페인별 분석 차트
- 주/일/월별 트렌드 기능 추가
- utm_content 기반 스마트 날짜 추출 기능 추가
- 부드러운 UI 로딩 애니메이션 적용
- 🎯 차트 클릭형 Drill-down: 세로 점선 제거 및 클릭 편의성 극대화
- UTM 링크 생성기 (구글 시트 자동 적재 기능 추가)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import re
from urllib.parse import urlencode
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
SPREADSHEET_ID = "1MTS1Aa8NmAbcvnpPs78LsQmAImSLbSHwEp5QbKE7JbI"
SHEET_NAME = "UTM생성기"

CHART_PALETTE = [
    "#C5A774", "#891C21", "#4ECDC4", "#45B7D1",
    "#D4636C", "#96648C", "#7BC67E", "#E5D4B0",
    "#FF9F43", "#6B1419", "#A68B5B", "#FF6B6B",
]

PLOTLY_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#ccc"),
    margin=dict(l=0, r=0, t=10, b=0),
)

# ─────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────
st.set_page_config(
    page_title="UTM Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────
st.markdown("""<style>
/* Hide Streamlit defaults */
#MainMenu, footer {visibility: hidden;}

/* Overall spacing */
.block-container {padding-top: 1.5rem; padding-bottom: 1rem;}

/* Smooth Fade & Slide-up Animation */
@keyframes fadeSlideUp {
    0% { opacity: 0; transform: translateY(15px); }
    100% { opacity: 1; transform: translateY(0); }
}
.stPlotlyChart, .stDataFrame, [data-testid="stMetric"], .section-hd, .drilldown-container {
    animation: fadeSlideUp 0.6s cubic-bezier(0.2, 0.8, 0.2, 1) forwards;
}

/* KPI metric override */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid rgba(197, 167, 116, 0.2);
    border-radius: 10px;
    padding: 16px 20px;
    text-align: center;
}
[data-testid="stMetricLabel"] {
    font-size: 12px !important;
    color: #888 !important;
    text-transform: uppercase;
    letter-spacing: 1px;
    justify-content: center !important;
}
[data-testid="stMetricValue"] {
    font-size: 26px !important;
    font-weight: 700 !important;
    justify-content: center !important;
}

/* Section header */
.section-hd {
    font-size: 15px;
    font-weight: 600;
    color: #C5A774;
    margin: 20px 0 8px;
    padding-bottom: 6px;
    border-bottom: 1px solid rgba(197, 167, 116, 0.15);
}

/* Tab styling */
.stTabs [data-baseweb="tab"] {
    font-size: 15px;
    font-weight: 600;
}

/* Dataframe */
.stDataFrame {font-size: 13px;}
</style>""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Google Sheets Auth Helper
# ─────────────────────────────────────────
def get_credentials() -> Credentials:
    token_json = None
    try:
        token_json = st.secrets["GOOGLE_TOKEN_JSON"]
    except (KeyError, FileNotFoundError):
        pass

    if not token_json:
        token_json = os.getenv("GOOGLE_TOKEN_JSON")

    if not token_json:
        for path in [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json"),
            os.path.expanduser("~/utm-tracker-updater/token.json"),
        ]:
            if os.path.exists(path):
                with open(path) as f:
                    token_json = f.read()
                break

    if not token_json:
        raise ValueError("TOKEN_NOT_FOUND")

    token_data = json.loads(token_json)
    creds = Credentials.from_authorized_user_info(token_data)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    
    return creds


# ─────────────────────────────────────────
# Data Loading & Saving
# ─────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data() -> tuple:
    try:
        creds = get_credentials()
    except ValueError as e:
        if str(e) == "TOKEN_NOT_FOUND":
            return pd.DataFrame(), "TOKEN_NOT_FOUND"
        return pd.DataFrame(), f"인증 에러: {e}"
    except Exception as e:
        return pd.DataFrame(), f"Google 인증 실패: {type(e).__name__}: {e}"

    try:
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A1:O")
            .execute()
        )
    except Exception as e:
        return pd.DataFrame(), f"Google Sheets API 실패: {type(e).__name__}: {e}"

    values = result.get("values", [])
    if len(values) < 2:
        return pd.DataFrame(), "시트에 데이터가 없습니다."

    headers = values[0]
    rows = []
    for row in values[1:]:
        if not any(cell.strip() for cell in row if cell):
            continue
        padded = row + [""] * (len(headers) - len(row))
        rows.append(padded[: len(headers)])

    df = pd.DataFrame(rows, columns=headers)

    df["UV"] = pd.to_numeric(df["UV"].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
    df["결제완료"] = pd.to_numeric(df["결제완료"].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
    df["CVR_num"] = df["CVR"].astype(str).str.replace("%", "").apply(lambda x: float(x) if x.strip() not in ["-", "", "0%"] else 0.0)
    df["결제금액_num"] = df["결제금액"].apply(_parse_currency)
    df["생성일_parsed"] = pd.to_datetime(df["생성일"], format="mixed", dayfirst=False, errors="coerce")

    def parse_utm_date(row):
        fallback = row["생성일_parsed"]
        content = str(row.get("utm_content", ""))
        m6 = re.search(r'(?<!\d)(\d{6})(?!\d)', content)
        if m6:
            try: return pd.to_datetime(m6.group(1), format="%y%m%d")
            except: pass
        m4 = re.search(r'(?<!\d)(\d{4})(?!\d)', content)
        if m4:
            try:
                year = fallback.year if pd.notna(fallback) else datetime.now().year
                return pd.to_datetime(f"{year}{m4.group(1)}", format="%Y%m%d")
            except: pass
        return fallback

    df["날짜"] = df.apply(parse_utm_date, axis=1)
    df["주차_시작일"] = df["날짜"].dt.to_period("W").dt.start_time

    return df, None

def save_utm_to_sheet(row_data: list):
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    body = {"values": [row_data]}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A:A",
        valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body=body
    ).execute()

def _parse_currency(val) -> int:
    s = str(val).strip()
    if s in ["-", "", "0"]: return 0
    s = s.replace("₩", "").replace(",", "").strip()
    try: return int(s)
    except ValueError: return 0

def fmt_currency(val: int) -> str:
    if val == 0: return "0원"
    elif val >= 100_000_000: return f"{val / 100_000_000:.1f}억원"
    elif val >= 10_000: return f"{val / 10_000:.0f}만원"
    else: return f"{val:,}원"

def fmt_num(val: int) -> str:
    return f"{val:,}"

# ─────────────────────────────────────────
# Dashboard Tab
# ─────────────────────────────────────────
def render_dashboard(df: pd.DataFrame):
    # ── Filters ──
    with st.expander("필터", expanded=True):
        valid_dates = df["날짜"].dropna()
        min_date, max_date = (valid_dates.min().date(), valid_dates.max().date()) if not valid_dates.empty else (datetime.now().date(), datetime.now().date())
        date_range = st.date_input("조회 기간", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1: sel_source = st.selectbox("Source", ["전체"] + sorted(df["utm_source"].dropna().unique().tolist()))
        with fc2: sel_medium = st.selectbox("Medium", ["전체"] + sorted(df["utm_medium"].dropna().unique().tolist()))
        with fc3: sel_campaign = st.selectbox("Campaign", ["전체"] + sorted(df["utm_campaign"].dropna().unique().tolist()))
        with fc4: sel_creator = st.selectbox("생성자", ["전체"] + sorted(df["생성자"].dropna().unique().tolist()))

    fdf = df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        fdf = fdf[(fdf["날짜"].dt.date >= date_range[0]) & (fdf["날짜"].dt.date <= date_range[1])]
    if sel_source != "전체": fdf = fdf[fdf["utm_source"] == sel_source]
    if sel_medium != "전체": fdf = fdf[fdf["utm_medium"] == sel_medium]
    if sel_campaign != "전체": fdf = fdf[fdf["utm_campaign"] == sel_campaign]
    if sel_creator != "전체": fdf = fdf[fdf["생성자"] == sel_creator]

    # ── KPI Cards ──
    total_uv, total_purchase = int(fdf["UV"].sum()), int(fdf["결제완료"].sum())
    overall_cvr = (total_purchase / total_uv * 100) if total_uv > 0 else 0
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total UV", fmt_num(total_uv))
    k2.metric("결제 전환", fmt_num(total_purchase))
    k3.metric("Overall CVR", f"{overall_cvr:.2f}%")
    k4.metric("총 매출", fmt_currency(int(fdf["결제금액_num"].sum())))
    k5.metric("활성 UTM", f"{int((fdf['UV'] > 0).sum())} / {len(fdf)}")

    st.markdown("")

    # ── Row 1: Timeline + Source Donut ──
    c1, c2 = st.columns([2.2, 1])
    selected_date_str = None

    with c1:
        st.markdown('<div class="section-hd">UV & 전환 추이</div>', unsafe_allow_html=True)
        time_group = st.radio("조회 단위", ["일간", "주간", "월간"], index=1, horizontal=True, label_visibility="collapsed")
        
        trend_df = fdf.copy()
        if time_group == "일간": 
            trend_df["기준일"] = trend_df["날짜"].dt.normalize()
            date_fmt = "%y.%m.%d"
        elif time_group == "월간": 
            trend_df["기준일"] = trend_df["날짜"].dt.to_period("M").dt.start_time
            date_fmt = "%y년 %m월"
        else: 
            trend_df["기준일"] = trend_df["날짜"].dt.to_period("W").dt.start_time
            date_fmt = "%y.%m.%d"
            
        grouped = trend_df.groupby("기준일", dropna=True).agg(UV=("UV", "sum"), 전환=("결제완료", "sum")).reset_index().sort_values("기준일")
        grouped["표시_날짜"] = grouped["기준일"].dt.strftime(date_fmt)
        if time_group == "주간": grouped["표시_날짜"] = grouped["표시_날짜"] + "(주)"

        if not grouped.empty:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # 🚨 투명한 클릭용 바 추가 (어디든 클릭하면 반응하게 함)
            fig.add_trace(go.Bar(
                x=grouped["표시_날짜"], y=[grouped["UV"].max()*1.5]*len(grouped),
                marker_color="rgba(0,0,0,0)", hoverinfo="skip", showlegend=False,
            ), secondary_y=False)

            fig.add_trace(go.Bar(
                x=grouped["표시_날짜"], y=grouped["UV"], name="UV",
                marker_color="#C5A774", opacity=0.85,
                text=[f"{v:,}" if v > 0 else "" for v in grouped["UV"]],
                textposition="outside", textfont=dict(size=11, color="#ccc"),
            ), secondary_y=False)
            
            line_color = "#FF3333"
            fig.add_trace(go.Scatter(
                x=grouped["표시_날짜"], y=grouped["전환"], name="전환",
                mode="lines+markers", line=dict(color=line_color, width=3), 
                marker=dict(size=11, color=line_color, line=dict(color="white", width=1.5)),
            ), secondary_y=True)
            
            annotations = []
            for _, row in grouped.iterrows():
                if row["전환"] > 0:
                    annotations.append(dict(
                        x=row["표시_날짜"], y=row["전환"], xref="x", yref="y2",
                        text=f"<b>{int(row['전환']):,}건</b>", showarrow=False,
                        yshift=22, bgcolor=line_color, borderpad=4, font=dict(color="white", size=12)
                    ))

            fig.update_layout(
                PLOTLY_LAYOUT, height=370, legend=dict(orientation="h", y=1.15, x=0),
                xaxis=dict(tickangle=-45, tickfont=dict(size=10), showspikes=False), # 🚨 점선(Spikes) 제거
                yaxis=dict(showspikes=False), yaxis2=dict(showspikes=False),
                bargap=0.3, hovermode="x", annotations=annotations, clickmode="event+select"
            )
            
            max_uv, max_conv = grouped["UV"].max(), grouped["전환"].max()
            fig.update_yaxes(secondary_y=False, range=[0, max_uv * 1.25 if max_uv > 0 else 10])
            fig.update_yaxes(secondary_y=True, range=[0, max_conv * 2.2 if max_conv > 0 else 10], tickformat="d")
            
            chart_selection = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points", key="trend_chart")
            try:
                if chart_selection and hasattr(chart_selection, "selection") and chart_selection.selection.points:
                    selected_date_str = chart_selection.selection.points[0]["x"]
            except: pass
        else: st.info("데이터가 없습니다.")

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

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Row 2: Drill-down (Only show on click) ──
    if selected_date_str:
        st.markdown('<div class="drilldown-container" style="background: rgba(197, 167, 116, 0.05); border: 1px solid rgba(197, 167, 116, 0.2); padding: 20px; border-radius: 12px; margin-bottom: 30px;">', unsafe_allow_html=True)
        st.markdown(f'<div class="section-hd" style="color:#C5A774; margin-top:0;">🎯 {selected_date_str} 전환 발생 상세 내역</div>', unsafe_allow_html=True)
        
        drill_df = fdf.copy()
        if time_group == "일간": drill_df["표시_날짜"] = drill_df["날짜"].dt.strftime("%y.%m.%d")
        elif time_group == "월간": drill_df["표시_날짜"] = drill_df["날짜"].dt.to_period("M").dt.start_time.dt.strftime("%y년 %m월")
        else: drill_df["표시_날짜"] = drill_df["날짜"].dt.to_period("W").dt.start_time.dt.strftime("%y.%m.%d") + "(주)"
            
        converting = drill_df[(drill_df["결제완료"] > 0) & (drill_df["표시_날짜"] == selected_date_str)].sort_values("CVR_num", ascending=False)

        if not converting.empty:
            cc1, cc2 = st.columns(2)
            with cc1:
                fig = px.bar(converting, x="utm_content", y="CVR_num", color_discrete_sequence=["#C5A774"], text_auto=".1f")
                fig.update_layout(PLOTLY_LAYOUT, height=280, title="CVR 순위 (%)")
                st.plotly_chart(fig, use_container_width=True)
            with cc2:
                fig = px.bar(converting, x="utm_content", y="결제금액_num", color_discrete_sequence=["#891C21"])
                fig.update_layout(PLOTLY_LAYOUT, height=280, title="매출 순위 (₩)")
                st.plotly_chart(fig, use_container_width=True)

            show_cols = ["날짜", "utm_content", "utm_campaign", "utm_source", "utm_medium", "UV", "결제완료", "CVR", "결제금액", "결제품목"]
            converting["날짜"] = converting["날짜"].dt.strftime("%Y-%m-%d")
            st.dataframe(converting[show_cols].reset_index(drop=True), use_container_width=True, hide_index=True)
        else: st.info(f"{selected_date_str}에 발생한 전환 내역이 없습니다.")
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Row 3: Campaign Performance ──
    c3, c4 = st.columns([2.2, 1])
    with c3:
        st.markdown('<div class="section-hd">캠페인별 성과</div>', unsafe_allow_html=True)
        camp = fdf.groupby("utm_campaign").agg(UV=("UV", "sum"), 전환=("결제완료", "sum")).reset_index()
        camp = camp[camp["UV"] > 0].sort_values("UV", ascending=True)
        if not camp.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(y=camp["utm_campaign"], x=camp["UV"], orientation="h", marker_color="#C5A774", name="UV"))
            fig.update_layout(PLOTLY_LAYOUT, height=max(320, len(camp)*32), barmode="overlay")
            st.plotly_chart(fig, use_container_width=True)
    with c4:
        st.markdown('<div class="section-hd">미디엄별 UV</div>', unsafe_allow_html=True)
        med = fdf.groupby("utm_medium")["UV"].sum().reset_index().sort_values("UV", ascending=False)
        if not med.empty:
            fig = px.pie(med, values="UV", names="utm_medium", color_discrete_sequence=CHART_PALETTE[2:], hole=0.45)
            fig.update_layout(PLOTLY_LAYOUT, height=370, showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

    # ── Row 4: Source × Medium Heatmap ──
    st.markdown('<div class="section-hd">소스 x 미디엄 UV 히트맵</div>', unsafe_allow_html=True)
    pivot = fdf.pivot_table(values="UV", index="utm_source", columns="utm_medium", aggfunc="sum", fill_value=0)
    if not pivot.empty:
        fig = px.imshow(pivot, text_auto=True, color_continuous_scale=["#1a1a2e", "#C5A774", "#891C21"])
        fig.update_layout(PLOTLY_LAYOUT, height=max(200, len(pivot)*55+80))
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 5: Full Data Table ──
    st.markdown('<div class="section-hd">전체 UTM 데이터</div>', unsafe_allow_html=True)
    table_cols = ["생성일", "생성자", "랜딩 URL", "utm_source", "utm_medium", "utm_campaign", "utm_content", "UV", "결제완료", "CVR", "결제금액", "결제품목", "완성 URL", "메모"]
    st.dataframe(fdf.sort_values("날짜", ascending=False)[table_cols].reset_index(drop=True), use_container_width=True, hide_index=True, height=420)

# ─────────────────────────────────────────
# UTM Generator Tab
# ─────────────────────────────────────────
def render_generator():
    st.markdown('<div class="section-hd">UTM 링크 생성기</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        base_url = st.text_input("랜딩 URL", value="https://thesoo.co/")
        source = st.selectbox("utm_source", ["kakao", "naver", "instagram", "blog", "web_page", "직접 입력"])
        if source == "직접 입력": source = st.text_input("source 입력")
        medium = st.selectbox("utm_medium", ["text", "image", "banner", "video", "instant", "직접 입력"])
        if medium == "직접 입력": medium = st.text_input("medium 입력")
    with c2:
        campaign = st.text_input("utm_campaign", placeholder="예: 2602_seolevent")
        content = st.text_input("utm_content", placeholder="예: 260226_kakao")
        sheet_memo = st.text_input("메모 (선택)")

    if base_url and source and medium and campaign and content:
        params = {"utm_source": source, "utm_medium": medium, "utm_campaign": campaign, "utm_content": content}
        full_url = f"{base_url}{'&' if '?' in base_url else '?'}{urlencode(params)}"
        st.code(full_url, language=None)
        if st.button("시트에 추가하기", use_container_width=True):
            save_utm_to_sheet([datetime.now().strftime("%Y. %m. %d"), "담당자", base_url, source, medium, campaign, content, 0, 0, "0%", "-", "-", full_url, "", sheet_memo])
            st.success("저장되었습니다!")
            st.cache_data.clear()

def main():
    df, err = load_data()
    if df.empty and err: 
        st.error(f"에러: {err}")
        return
    hc1, hc2 = st.columns([9, 1])
    with hc1: st.markdown("## UTM Performance Dashboard")
    with hc2: 
        if st.button("새로고침"): 
            st.cache_data.clear()
            st.rerun()
    tab_dash, tab_gen = st.tabs(["📊 Performance", "🔗 UTM Generator"])
    with tab_dash: render_dashboard(df)
    with tab_gen: render_generator()

if __name__ == "__main__": main()
