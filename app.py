#!/usr/bin/env python3
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

CHART_PALETTE = ["#C5A774", "#891C21", "#4ECDC4", "#45B7D1", "#D4636C", "#96648C", "#7BC67E", "#E5D4B0", "#FF9F43", "#6B1419", "#A68B5B", "#FF6B6B"]
PLOTLY_LAYOUT = dict(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#ccc"), margin=dict(l=0, r=0, t=10, b=0))

st.set_page_config(page_title="UTM Performance Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

# ─────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────
st.markdown("""<style>
#MainMenu, footer {visibility: hidden;}
.block-container {padding-top: 1.5rem; padding-bottom: 1rem;}
@keyframes fadeSlideUp { 0% { opacity: 0; transform: translateY(15px); } 100% { opacity: 1; transform: translateY(0); } }
.stPlotlyChart, .stDataFrame, [data-testid="stMetric"], .section-hd, .drilldown-box { animation: fadeSlideUp 0.5s ease-out forwards; }
[data-testid="stMetric"] { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border: 1px solid rgba(197, 167, 116, 0.2); border-radius: 10px; padding: 16px 20px; text-align: center; }
[data-testid="stMetricLabel"] { font-size: 12px !important; color: #888 !important; text-transform: uppercase; justify-content: center !important; }
[data-testid="stMetricValue"] { font-size: 26px !important; font-weight: 700 !important; justify-content: center !important; }
.section-hd { font-size: 15px; font-weight: 600; color: #C5A774; margin: 20px 0 8px; padding-bottom: 6px; border-bottom: 1px solid rgba(197, 167, 116, 0.15); }
.stDataFrame {font-size: 13px;}
.date-tag {
    display: inline-block;
    background-color: rgba(197, 167, 116, 0.2);
    color: #C5A774;
    padding: 2px 10px;
    border-radius: 5px;
    border: 1px solid #C5A774;
    margin-right: 5px;
    margin-bottom: 5px;
    font-size: 14px;
    font-weight: 600;
}
</style>""", unsafe_allow_html=True)

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

@st.cache_data(ttl=300)
def load_data():
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A1:O").execute()
        values = res.get("values", [])
        if len(values) < 2: return pd.DataFrame(), "No Data"
        df = pd.DataFrame(values[1:], columns=values[0])
        for col in ["UV", "결제완료"]: df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
        df["CVR_num"] = df["CVR"].astype(str).str.replace("%", "").apply(lambda x: float(x) if x.strip() not in ["-", "", "0%"] else 0.0)
        df["결제금액_num"] = df["결제금액"].astype(str).str.replace("₩", "").str.replace(",", "").apply(lambda x: int(x) if str(x).isdigit() else 0)
        df["생성일_dt"] = pd.to_datetime(df["생성일"], format="mixed", dayfirst=False, errors="coerce")
        def parse_date(r):
            c = str(r.get("utm_content", ""))
            m6 = re.search(r'(\d{6})', c)
            if m6: return pd.to_datetime(m6.group(1), format="%y%m%d", errors='coerce')
            m4 = re.search(r'(\d{4})', c)
            if m4: return pd.to_datetime(f"{datetime.now().year}{m4.group(1)}", format="%Y%m%d", errors='coerce')
            return r["생성일_dt"]
        df["날짜_dt"] = df.apply(parse_date, axis=1)
        return df, None
    except Exception as e: return pd.DataFrame(), str(e)

def fmt_currency(v):
    if v >= 100_000_000: return f"{v/100_000_000:.1f}억원"
    if v >= 10_000: return f"{v/10_000:.0f}만원"
    return f"{v:,}원"

# ─────────────────────────────────────────
# Dashboard Logic
# ─────────────────────────────────────────
def render_dashboard(df):
    with st.expander("🔍 상세 필터", expanded=True):
        v_dates = df["날짜_dt"].dropna()
        min_d, max_d = (v_dates.min().date(), v_dates.max().date()) if not v_dates.empty else (datetime.now().date(), datetime.now().date())
        date_range = st.date_input("조회 기간", value=(min_d, max_d))
        c1, c2, c3, c4 = st.columns(4)
        sel_src = c1.selectbox("Source", ["전체"] + sorted(df["utm_source"].unique()))
        sel_med = c2.selectbox("Medium", ["전체"] + sorted(df["utm_medium"].unique()))
        sel_cam = c3.selectbox("Campaign", ["전체"] + sorted(df["utm_campaign"].unique()))
        sel_cre = c4.selectbox("생성자", ["전체"] + sorted(df["생성자"].unique()))

    fdf = df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        fdf = fdf[(fdf["날짜_dt"].dt.date >= date_range[0]) & (fdf["날짜_dt"].dt.date <= date_range[1])]
    if sel_src != "전체": fdf = fdf[fdf["utm_source"] == sel_src]
    if sel_med != "전체": fdf = fdf[fdf["utm_medium"] == sel_medium]
    if sel_campaign := sel_cam != "전체": fdf = fdf[fdf["utm_campaign"] == sel_cam]
    if sel_creator := sel_cre != "전체": fdf = fdf[fdf["생성자"] == sel_cre]

    k1, k2, k3, k4, k5 = st.columns(5)
    uv, pay = fdf["UV"].sum(), fdf["결제완료"].sum()
    k1.metric("Total UV", f"{uv:,}")
    k2.metric("Total 결제", f"{pay:,}")
    k3.metric("Avg CVR", f"{(pay/uv*100 if uv>0 else 0):.2f}%")
    k4.metric("Total 매출", fmt_currency(fdf["결제금액_num"].sum()))
    k5.metric("Active UTM", f"{(fdf['UV']>0).sum():,}")

    st.markdown('<div class="section-hd">UV & 전환 트렌드</div>', unsafe_allow_html=True)
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

    for i, r in grp.iterrows():
        if r["pay"] > 0:
            is_sel = r["g"] in st.session_state.selected_points
            fig.add_annotation(x=r["g"], y=r["pay"], yref="y2", text=f"<b>{int(r['pay'])}건</b>", showarrow=False, yshift=25, 
                               bgcolor="#FF3333" if not is_sel else "white", font=dict(color="white" if not is_sel else "#FF3333", size=13), borderpad=5)

    fig.update_layout(PLOTLY_LAYOUT, height=400, hovermode="x", clickmode="event+select", xaxis=dict(showgrid=False, tickangle=-45))
    fig.update_yaxes(secondary_y=False, range=[0, grp["UV"].max()*1.3])
    fig.update_yaxes(secondary_y=True, range=[0, grp["pay"].max()*2.5], showgrid=False)

    sel = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="main_chart")
    if sel and "selection" in sel and sel["selection"]["points"]:
        clicked_x = sel["selection"]["points"][0]["x"]
        if clicked_x in st.session_state.selected_points: st.session_state.selected_points.remove(clicked_x)
        else: st.session_state.selected_points.append(clicked_x)
        st.rerun()

    if st.session_state.selected_points:
        st.markdown(f'<div class="drilldown-box" style="background:rgba(197, 167, 116, 0.1); padding:20px; border-radius:15px; border:1px solid #C5A774;">', unsafe_allow_html=True)
        st.markdown("#### 🎯 선택된 기간 상세 성과")
        tag_cols = st.columns([0.15, 0.85])
        with tag_cols[0]:
            if st.button("선택 초기화 ✖️", key="clear_btn"): st.session_state.selected_points = []; st.rerun()
        with tag_cols[1]:
            tags_html = "".join([f'<span class="date-tag">{p}</span>' for p in st.session_state.selected_points])
            st.markdown(tags_html, unsafe_allow_html=True)
            
        detail_df = chart_df[chart_df["g"].isin(st.session_state.selected_points) & (chart_df["결제완료"] > 0)].sort_values("결제완료", ascending=False)
        if not detail_df.empty:
            sc1, sc2 = st.columns(2)
            with sc1: st.plotly_chart(px.bar(detail_df, x="utm_content", y="CVR_num", title="CVR (%)", color_discrete_sequence=["#C5A774"], text_auto=".1f").update_layout(PLOTLY_LAYOUT), use_container_width=True)
            with sc2: st.plotly_chart(px.bar(detail_df, x="utm_content", y="결제금액_num", title="매출액 (원)", color_discrete_sequence=["#891C21"]).update_layout(PLOTLY_LAYOUT), use_container_width=True)
            display_detail = detail_df.copy()
            display_detail["날짜"] = display_detail["날짜_dt"].dt.strftime("%Y-%m-%d")
            st.dataframe(display_detail[["날짜", "utm_content", "utm_campaign", "utm_source", "UV", "결제완료", "CVR", "결제금액", "결제품목"]].reset_index(drop=True), use_container_width=True, hide_index=True)
        else: st.info("선택한 날짜에 전환 데이터가 없습니다.")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    c_sub1, c_sub2 = st.columns([2, 1])
    with c_sub1:
        st.markdown('<div class="section-hd">캠페인 성과</div>', unsafe_allow_html=True)
        cp = fdf.groupby("utm_campaign").agg(UV=("UV","sum"), pay=("결제완료","sum")).reset_index().sort_values("UV")
        fig_cp = go.Figure()
        fig_cp.add_trace(go.Bar(y=cp["utm_campaign"], x=cp["UV"], orientation="h", marker_color="#C5A774", name="UV"))
        fig_cp.update_layout(PLOTLY_LAYOUT, height=max(300, len(cp)*30))
        st.plotly_chart(fig_cp, use_container_width=True)
    with c_sub2:
        st.markdown('<div class="section-hd">미디엄 비중</div>', unsafe_allow_html=True)
        md = fdf.groupby("utm_medium")["UV"].sum().reset_index()
        st.plotly_chart(px.pie(md, values="UV", names="utm_medium", hole=0.4, color_discrete_sequence=CHART_PALETTE).update_layout(PLOTLY_LAYOUT), use_container_width=True)

    st.markdown('<div class="section-hd">전체 UTM 데이터</div>', unsafe_allow_html=True)
    all_disp = fdf.copy()
    all_disp["날짜"] = all_disp["날짜_dt"].dt.strftime("%Y-%m-%d")
    
    # 🚨 수정됨: hidden 인자 대신 column_order를 사용하여 기본 노출 컬럼 제어 (버전 호환성 해결)
    all_cols = ["날짜", "생성자", "랜딩 URL", "utm_source", "utm_medium", "utm_campaign", "utm_content", "UV", "결제완료", "CVR", "결제금액", "결제품목", "완성 URL", "메모"]
    default_visible_cols = ["날짜", "랜딩 URL", "utm_source", "utm_medium", "utm_campaign", "utm_content", "UV", "결제완료", "CVR", "결제금액", "결제품목"]
    
    st.dataframe(
        all_disp.sort_values("날짜", ascending=False), 
        use_container_width=True, 
        hide_index=True, 
        height=420,
        column_order=default_visible_cols # 이 리스트에 포함된 열만 기본으로 노출됩니다.
    )

# ─────────────────────────────────────────
# UTM Generator
# ─────────────────────────────────────────
def render_gen():
    st.markdown('<div class="section-hd">UTM 링크 생성기</div>', unsafe_allow_html=True)
    with st.container(border=True):
        col1, col2 = st.columns(2)
        url = col1.text_input("랜딩 URL", "https://thesoo.co/")
        src = col1.selectbox("Source", ["kakao", "naver", "instagram", "facebook", "blog", "직접입력"])
        if src=="직접입력": src = col1.text_input("Source 입력")
        med = col2.selectbox("Medium", ["text", "image", "banner", "video", "instant", "직접입력"])
        if med=="직접입력": med = col2.text_input("Medium 입력")
        cam = col2.text_input("Campaign", placeholder="예: 2602_seolevent")
        cnt = st.text_input("Content", placeholder="예: 260226_kakao")
        memo = st.text_input("메모 (시트 전용)")
        
        if url and src and med and cam and cnt:
            params = {"utm_source": src, "utm_medium": med, "utm_campaign": cam, "utm_content": cnt}
            final_url = f"{url}{'&' if '?' in url else '?'}{urlencode(params)}"
            st.code(final_url, language=None)
            if st.button("🚀 구글 시트에 추가", use_container_width=True):
                row = [datetime.now().strftime("%Y. %m. %d"), "담당자", url, src, med, cam, cnt, 0, 0, "0%", "-", "-", final_url, "", memo]
                try:
                    creds = get_credentials()
                    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
                    service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A:A", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body={"values": [row]}).execute()
                    st.success("데이터가 성공적으로 전송되었습니다."); st.cache_data.clear()
                except Exception as e: st.error(f"저장 실패: {e}")

def main():
    df, err = load_data()
    if not df.empty:
        t1, t2 = st.tabs(["📊 Performance", "🔗 UTM Generator"])
        with t1: render_dashboard(df)
        with t2: render_gen()
    else: st.error(f"데이터 로드 실패: {err}")

if __name__ == "__main__": main()
