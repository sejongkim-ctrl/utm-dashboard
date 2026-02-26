#!/usr/bin/env python3
"""UTM Performance Dashboard

Segment_Full (Redash 전체 UTM) 우선 로드, UTM생성기 폴백.
- 인터랙티브 drill-down 차트
- 일간/주간/월간 트렌드 토글
- UTM 링크 생성 + Google Sheets 직접 저장
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import re
from urllib.parse import urlencode, unquote_plus
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
SPREADSHEET_ID = "1MTS1Aa8NmAbcvnpPs78LsQmAImSLbSHwEp5QbKE7JbI"
SEGMENT_SHEET_NAME = "Segment_Full"
GENERATOR_SHEET_NAME = "UTM생성기"

CHART_PALETTE = [
    "#C5A774", "#891C21", "#4ECDC4", "#45B7D1", "#D4636C",
    "#96648C", "#7BC67E", "#E5D4B0", "#FF9F43", "#6B1419",
    "#A68B5B", "#FF6B6B",
]
PLOTLY_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#ccc"),
    margin=dict(l=0, r=0, t=10, b=0),
)

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
#MainMenu, footer {visibility: hidden;}
.block-container {padding-top: 3rem; padding-bottom: 1rem;}

@keyframes fadeSlideUp { 0% { opacity: 0; transform: translateY(15px); } 100% { opacity: 1; transform: translateY(0); } }
.stPlotlyChart, .stDataFrame, [data-testid="stMetric"], .section-hd, .drilldown-box { animation: fadeSlideUp 0.5s ease-out forwards; }

[data-testid="stMetric"] { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border: 1px solid rgba(197, 167, 116, 0.2); border-radius: 10px; padding: 16px 20px; text-align: center; }
[data-testid="stMetricLabel"] { font-size: 12px !important; color: #888 !important; text-transform: uppercase; justify-content: center !important; }
[data-testid="stMetricValue"] { font-size: 26px !important; font-weight: 700 !important; justify-content: center !important; }

.section-hd { font-size: 15px; font-weight: 600; color: #C5A774; margin: 20px 0 8px; padding-bottom: 6px; border-bottom: 1px solid rgba(197, 167, 116, 0.15); }
.stDataFrame {font-size: 13px;}

.date-tag { display: inline-block; background-color: rgba(197, 167, 116, 0.2); color: #C5A774; padding: 2px 10px; border-radius: 5px; border: 1px solid #C5A774; margin-right: 5px; margin-bottom: 5px; font-size: 14px; font-weight: 600; }
.badge-segment { display: inline-block; padding: 2px 10px; border-radius: 4px; font-size: 11px; font-weight: 600; background: rgba(197, 167, 116, 0.2); color: #C5A774; }
.badge-sheet { display: inline-block; padding: 2px 10px; border-radius: 4px; font-size: 11px; font-weight: 600; background: rgba(137, 28, 33, 0.2); color: #D4636C; }
</style>""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Data Engine
# ─────────────────────────────────────────
def get_credentials():
    """Google OAuth2 인증: st.secrets → 환경변수 → 로컬 파일."""
    token_json = None

    try:
        token_json = st.secrets["GOOGLE_TOKEN_JSON"]
    except (KeyError, FileNotFoundError):
        pass
    except Exception as e:
        raise ValueError(f"st.secrets 읽기 실패: {e}")

    if not token_json:
        token_json = os.getenv("GOOGLE_TOKEN_JSON")

    if not token_json:
        for p in [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json"),
            os.path.expanduser("~/utm-tracker-updater/token.json"),
        ]:
            if os.path.exists(p):
                with open(p) as f:
                    token_json = f.read()
                break

    if not token_json:
        raise ValueError("TOKEN_NOT_FOUND")

    creds = Credentials.from_authorized_user_info(json.loads(token_json))
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def _read_sheet_tab(service, sheet_name: str):
    """시트 탭 읽기. 성공 시 DataFrame, 실패/미존재 시 None."""
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A1:Z")
            .execute()
        )
        values = result.get("values", [])
        if len(values) < 2:
            return None
        headers = values[0]
        rows = []
        for row in values[1:]:
            if not any(cell.strip() for cell in row if cell):
                continue
            padded = row + [""] * (len(headers) - len(row))
            rows.append(padded[: len(headers)])
        return pd.DataFrame(rows, columns=headers) if rows else None
    except Exception:
        return None


def _parse_date_from_content(row):
    """utm_content에서 날짜 추출 (예: 250226_kakao → 2025-02-26).
    4자리(MMDD) 패턴: 최초유입 연도 기준으로 파싱 (과거 UTM 오파싱 방지).
    """
    c = str(row.get("utm_content", ""))
    m6 = re.search(r"(\d{6})", c)
    if m6:
        return pd.to_datetime(m6.group(1), format="%y%m%d", errors="coerce")
    m4 = re.search(r"(\d{4})", c)
    if m4:
        base_dt = row.get("생성일_dt")
        year = base_dt.year if pd.notna(base_dt) else datetime.now().year
        return pd.to_datetime(
            f"{year}{m4.group(1)}", format="%Y%m%d", errors="coerce"
        )
    return row.get("생성일_dt")


def _convert_numerics(df: pd.DataFrame):
    """숫자형 컬럼 변환 (in-place)."""
    for col in ["UV", "결제완료"]:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""), errors="coerce"
                )
                .fillna(0)
                .astype(int)
            )
    if "CVR" in df.columns:
        df["CVR_num"] = (
            df["CVR"]
            .astype(str)
            .str.replace("%", "")
            .apply(lambda x: float(x) if x.strip() not in ["-", "", "0%"] else 0.0)
        )
    if "결제금액" in df.columns:
        df["결제금액_num"] = (
            df["결제금액"]
            .astype(str)
            .str.replace("₩", "")
            .str.replace(",", "")
            .apply(lambda x: int(x) if str(x).strip().isdigit() else 0)
        )


@st.cache_data(ttl=300)
def load_data():
    """Segment_Full 우선, UTM생성기 폴백.
    Returns: (DataFrame, data_source_label, error_or_None)
    """
    try:
        creds = get_credentials()
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)

        segment_df = _read_sheet_tab(service, SEGMENT_SHEET_NAME)
        generator_df = _read_sheet_tab(service, GENERATOR_SHEET_NAME)

        # ── 1) Segment_Full (Redash 전체 UTM) ──
        if segment_df is not None and len(segment_df) > 0:
            df = segment_df
            for col in ["utm_source", "utm_medium", "utm_campaign", "utm_content"]:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: unquote_plus(str(x)) if pd.notna(x) else ""
                    )

            _convert_numerics(df)

            if "최초유입" in df.columns:
                df["생성일"] = df["최초유입"]
            df["생성일_dt"] = pd.to_datetime(
                df["생성일"], format="mixed", errors="coerce"
            )
            df["날짜_dt"] = df.apply(_parse_date_from_content, axis=1)
            df["날짜_dt"] = df["날짜_dt"].fillna(df["생성일_dt"])

            # 생성자 병합
            if (
                generator_df is not None
                and "생성자" in generator_df.columns
                and "utm_content" in generator_df.columns
            ):
                gen_meta = generator_df[["utm_content", "생성자"]].drop_duplicates(
                    subset="utm_content"
                )
                df = df.merge(gen_meta, on="utm_content", how="left")
                df["생성자"] = df["생성자"].fillna("미등록")
            else:
                df["생성자"] = "미등록"

            return df, "Segment_Full", None

        # ── 2) UTM생성기 폴백 ──
        if generator_df is not None and len(generator_df) > 0:
            df = generator_df
            _convert_numerics(df)
            df["생성일_dt"] = pd.to_datetime(
                df["생성일"], format="mixed", dayfirst=False, errors="coerce"
            )
            df["날짜_dt"] = df.apply(_parse_date_from_content, axis=1)
            df["날짜_dt"] = df["날짜_dt"].fillna(df["생성일_dt"])
            return df, "UTM생성기", None

        return pd.DataFrame(), "", "시트에 데이터가 없습니다."
    except Exception as e:
        return pd.DataFrame(), "", str(e)


def fmt_currency(v):
    if v >= 100_000_000:
        return f"{v / 100_000_000:.1f}억원"
    if v >= 10_000:
        return f"{v / 10_000:.0f}만원"
    return f"{v:,}원"


# ─────────────────────────────────────────
# Dashboard Logic
# ─────────────────────────────────────────
def render_dashboard(df, data_source):
    with st.expander("🔍 상세 필터", expanded=True):
        v_dates = df["날짜_dt"].dropna()
        min_d, max_d = (
            (v_dates.min().date(), v_dates.max().date())
            if not v_dates.empty
            else (datetime.now().date(), datetime.now().date())
        )
        date_range = st.date_input("조회 기간", value=(min_d, max_d))
        c1, c2, c3, c4 = st.columns(4)
        sel_src = c1.selectbox("Source", ["전체"] + sorted(df["utm_source"].dropna().unique().tolist()))
        sel_med = c2.selectbox("Medium", ["전체"] + sorted(df["utm_medium"].dropna().unique().tolist()))
        sel_cam = c3.selectbox("Campaign", ["전체"] + sorted(df["utm_campaign"].dropna().unique().tolist()))
        sel_cre = c4.selectbox("생성자", ["전체"] + sorted(df["생성자"].dropna().unique().tolist()))

    fdf = df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        fdf = fdf[
            (fdf["날짜_dt"].dt.date >= date_range[0])
            & (fdf["날짜_dt"].dt.date <= date_range[1])
        ]
    if sel_src != "전체":
        fdf = fdf[fdf["utm_source"] == sel_src]
    if sel_med != "전체":
        fdf = fdf[fdf["utm_medium"] == sel_med]
    if sel_cam != "전체":
        fdf = fdf[fdf["utm_campaign"] == sel_cam]
    if sel_cre != "전체":
        fdf = fdf[fdf["생성자"] == sel_cre]

    # ── KPI Cards ──
    uv, pay = int(fdf["UV"].sum()), int(fdf["결제완료"].sum())
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total UV", f"{uv:,}")
    k2.metric("Total 결제", f"{pay:,}")
    k3.metric("Avg CVR", f"{(pay / uv * 100 if uv > 0 else 0):.2f}%")
    k4.metric("Total 매출", fmt_currency(int(fdf["결제금액_num"].sum())))
    k5.metric("Active UTM", f"{(fdf['UV'] > 0).sum():,}")

    # ── UV & 전환 트렌드 ──
    st.markdown(
        '<div class="section-hd">UV & 전환 트렌드</div>', unsafe_allow_html=True
    )
    t_unit = st.radio(
        "단위", ["일간", "주간", "월간"], index=0, horizontal=True,
        label_visibility="collapsed",
    )

    # 시간 단위 변경 시 선택 초기화 (일간↔주간↔월간 g값 불일치 방지)
    if st.session_state.get("_prev_t_unit") != t_unit:
        st.session_state["_prev_t_unit"] = t_unit
        st.session_state.selected_points = []
        st.session_state["_chart_sel_xs"] = []

    chart_df = fdf.copy()
    if t_unit == "일간":
        chart_df["g"] = chart_df["날짜_dt"].dt.strftime("%y.%m.%d")
    elif t_unit == "월간":
        chart_df["g"] = (
            chart_df["날짜_dt"]
            .dt.to_period("M")
            .dt.start_time.dt.strftime("%y년 %m월")
        )
    else:
        chart_df["g"] = (
            chart_df["날짜_dt"]
            .dt.to_period("W")
            .dt.start_time.dt.strftime("%y.%m.%d")
            + "(주)"
        )

    grp = (
        chart_df.groupby("g")
        .agg(UV=("UV", "sum"), pay=("결제완료", "sum"))
        .reset_index()
        .sort_values("g")
    )

    if "selected_points" not in st.session_state:
        st.session_state.selected_points = []

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    # 투명 바 (y축 범위 보정)
    fig.add_trace(
        go.Bar(
            x=grp["g"],
            y=[grp["UV"].max() * 1.2] * len(grp),
            marker_color="rgba(0,0,0,0)",
            hoverinfo="skip",
            showlegend=False,
        ),
        secondary_y=False,
    )
    colors = [
        "#C5A774" if x not in st.session_state.selected_points else "#E5D4B0"
        for x in grp["g"]
    ]
    fig.add_trace(
        go.Bar(
            x=grp["g"], y=grp["UV"], name="UV",
            marker_color=colors, text=grp["UV"], textposition="outside",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=grp["g"], y=grp["pay"], name="전환",
            mode="lines+markers",
            line=dict(color="#FF3333", width=3),
            marker=dict(size=12, color="#FF3333", line=dict(color="white", width=2)),
        ),
        secondary_y=True,
    )

    for _, r in grp.iterrows():
        if r["pay"] > 0:
            is_sel = r["g"] in st.session_state.selected_points
            fig.add_annotation(
                x=r["g"], y=r["pay"], yref="y2",
                text=f"<b>{int(r['pay'])}건</b>", showarrow=False, yshift=25,
                bgcolor="#FF3333" if not is_sel else "white",
                font=dict(
                    color="white" if not is_sel else "#FF3333", size=13
                ),
                borderpad=5,
            )

    fig.update_layout(
        **PLOTLY_LAYOUT,
        height=400,
        hovermode="x",
        clickmode="event+select",
        xaxis=dict(showgrid=False, tickangle=-45),
    )
    fig.update_yaxes(secondary_y=False, range=[0, grp["UV"].max() * 1.3])
    fig.update_yaxes(
        secondary_y=True,
        range=[0, max(grp["pay"].max(), 1) * 2.5],
        showgrid=False,
    )

    sel = st.plotly_chart(
        fig, use_container_width=True, on_select="rerun", key="main_chart"
    )

    # ── 선택 상태 관리 (이전 선택과 비교하여 중복 토글 방지) ──
    cur_sel_xs = sorted(
        set(p.get("x") for p in sel.get("selection", {}).get("points", []))
    ) if sel else []
    prev_sel_xs = st.session_state.get("_chart_sel_xs", [])

    if cur_sel_xs and cur_sel_xs != prev_sel_xs:
        st.session_state["_chart_sel_xs"] = cur_sel_xs
        for px in cur_sel_xs:
            if px not in prev_sel_xs:
                if px in st.session_state.selected_points:
                    st.session_state.selected_points.remove(px)
                else:
                    st.session_state.selected_points.append(px)
        st.rerun()
    elif not cur_sel_xs and prev_sel_xs:
        st.session_state["_chart_sel_xs"] = []

    # ── Drill-down 상세 ──
    if st.session_state.selected_points:
        st.markdown(
            '<div class="drilldown-box" style="background:rgba(197, 167, 116, 0.1);'
            " padding:20px; border-radius:15px; border:1px solid #C5A774;"
            '">',
            unsafe_allow_html=True,
        )
        st.markdown("#### 선택된 기간 상세 성과")
        tag_cols = st.columns([0.15, 0.85])
        with tag_cols[0]:
            if st.button("선택 초기화", key="clear_btn"):
                st.session_state.selected_points = []
                st.session_state["_chart_sel_xs"] = []
                st.rerun()
        with tag_cols[1]:
            tags_html = "".join(
                [
                    f'<span class="date-tag">{p}</span>'
                    for p in st.session_state.selected_points
                ]
            )
            st.markdown(tags_html, unsafe_allow_html=True)

        # 선택 기간의 집계 KPI (grp 기반 — 차트와 동일한 데이터 소스)
        sel_grp = grp[grp["g"].isin(st.session_state.selected_points)]
        if not sel_grp.empty:
            s_uv = int(sel_grp["UV"].sum())
            s_pay = int(sel_grp["pay"].sum())
            s_cvr = (s_pay / s_uv * 100) if s_uv > 0 else 0
            sk1, sk2, sk3 = st.columns(3)
            sk1.metric("기간 UV", f"{s_uv:,}")
            sk2.metric("기간 전환", f"{s_pay:,}건")
            sk3.metric("기간 CVR", f"{s_cvr:.2f}%")

        # 해당 기간 전체 UTM (전환 여부 무관) — 전환 있는 행 우선 정렬
        period_df = chart_df[
            chart_df["g"].isin(st.session_state.selected_points)
        ].sort_values("결제완료", ascending=False)

        # 전환 있는 UTM만 별도 추출 (차트용)
        conv_df = period_df[period_df["결제완료"] > 0]

        if not conv_df.empty:
            sc1, sc2 = st.columns(2)
            with sc1:
                fig_cvr = px.bar(
                    conv_df, x="utm_content", y="CVR_num",
                    title="CVR (%)", color_discrete_sequence=["#C5A774"],
                    text_auto=".1f",
                )
                fig_cvr.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig_cvr, use_container_width=True)
            with sc2:
                fig_rev = px.bar(
                    conv_df, x="utm_content", y="결제금액_num",
                    title="매출액 (원)", color_discrete_sequence=["#891C21"],
                )
                fig_rev.update_layout(**PLOTLY_LAYOUT)
                st.plotly_chart(fig_rev, use_container_width=True)

        if not period_df.empty:
            display_detail = period_df.copy()
            display_detail["날짜"] = display_detail["날짜_dt"].dt.strftime(
                "%Y-%m-%d"
            )
            detail_cols = [
                "날짜", "utm_content", "utm_campaign", "utm_source",
                "UV", "결제완료", "CVR", "결제금액", "결제품목",
            ]
            show_detail = [c for c in detail_cols if c in display_detail.columns]
            st.dataframe(
                display_detail[show_detail].reset_index(drop=True),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("선택한 기간에 UTM 데이터가 없습니다.")
        st.markdown("</div>", unsafe_allow_html=True)

    # ── 캠페인 + 미디엄 ──
    st.markdown("<br>", unsafe_allow_html=True)
    c_sub1, c_sub2 = st.columns([2, 1])

    with c_sub1:
        st.markdown(
            '<div class="section-hd">캠페인 성과</div>', unsafe_allow_html=True
        )
        cp = (
            fdf.groupby("utm_campaign")
            .agg(UV=("UV", "sum"), pay=("결제완료", "sum"))
            .reset_index()
            .sort_values("UV")
        )
        if not cp.empty:
            fig_cp = go.Figure()
            fig_cp.add_trace(
                go.Bar(
                    y=cp["utm_campaign"], x=cp["UV"],
                    orientation="h", marker_color="#C5A774", name="UV",
                )
            )
            fig_cp.update_layout(**PLOTLY_LAYOUT, height=max(300, len(cp) * 30))
            st.plotly_chart(fig_cp, use_container_width=True)
        else:
            st.info("데이터 없음")

    with c_sub2:
        st.markdown(
            '<div class="section-hd">미디엄 비중</div>', unsafe_allow_html=True
        )
        md = fdf.groupby("utm_medium")["UV"].sum().reset_index()
        md = md[md["UV"] > 0]
        if not md.empty:
            fig_md = px.pie(
                md, values="UV", names="utm_medium",
                hole=0.4, color_discrete_sequence=CHART_PALETTE,
            )
            fig_md.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig_md, use_container_width=True)
        else:
            st.info("데이터 없음")

    # ── 전체 UTM 데이터 테이블 ──
    st.markdown(
        '<div class="section-hd">전체 UTM 데이터</div>', unsafe_allow_html=True
    )
    all_disp = fdf.copy()
    all_disp["날짜"] = all_disp["날짜_dt"].dt.strftime("%Y-%m-%d")

    if data_source == "Segment_Full":
        default_cols = [
            "날짜", "생성자", "utm_content", "utm_source", "utm_medium",
            "UV", "결제완료", "CVR", "결제금액", "결제품목",
        ]
    else:
        default_cols = [
            "날짜", "랜딩 URL", "utm_content", "UV", "결제완료",
            "CVR", "결제금액", "결제품목",
        ]

    visible_cols = [c for c in default_cols if c in all_disp.columns]
    st.dataframe(
        all_disp.sort_values("날짜", ascending=False),
        use_container_width=True, hide_index=True, height=420,
        column_order=visible_cols,
    )


# ─────────────────────────────────────────
# UTM Generator
# ─────────────────────────────────────────
def render_gen():
    st.markdown(
        '<div class="section-hd">UTM 링크 생성기</div>', unsafe_allow_html=True
    )
    with st.container(border=True):
        col1, col2 = st.columns(2)
        url = col1.text_input("랜딩 URL", "https://thesoo.co/")
        src = col1.selectbox(
            "Source", ["kakao", "naver", "instagram", "facebook", "blog", "직접입력"]
        )
        if src == "직접입력":
            src = col1.text_input("Source 입력")
        med = col2.selectbox(
            "Medium", ["text", "image", "banner", "video", "instant", "직접입력"]
        )
        if med == "직접입력":
            med = col2.text_input("Medium 입력")
        cam = col2.text_input("Campaign", placeholder="예: 2602_seolevent")
        cnt = st.text_input("Content", placeholder="예: 260226_kakao")
        memo = st.text_input("메모 (시트 전용)")

        if url and src and med and cam and cnt:
            params = {
                "utm_source": src,
                "utm_medium": med,
                "utm_campaign": cam,
                "utm_content": cnt,
            }
            final_url = f"{url}{'&' if '?' in url else '?'}{urlencode(params)}"
            st.code(final_url, language=None)

            if st.button("구글 시트에 추가", use_container_width=True):
                row = [
                    datetime.now().strftime("%Y. %m. %d"),
                    "담당자", url, src, med, cam, cnt,
                    0, 0, "0%", "-", "-", final_url, "", memo,
                ]
                try:
                    creds = get_credentials()
                    svc = build(
                        "sheets", "v4", credentials=creds, cache_discovery=False
                    )
                    svc.spreadsheets().values().append(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f"'{GENERATOR_SHEET_NAME}'!A:A",
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={"values": [row]},
                    ).execute()
                    st.success("데이터가 성공적으로 전송되었습니다.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"저장 실패: {e}")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
def main():
    df, data_source, err = load_data()

    if df.empty:
        st.error("데이터를 불러올 수 없습니다.")
        if err == "TOKEN_NOT_FOUND":
            st.markdown("""
**Streamlit Cloud**: Settings > Secrets에 `GOOGLE_TOKEN_JSON` 추가.
**로컬 실행**: `token.json`을 프로젝트 폴더에 복사하거나 환경변수 설정.
            """)
        elif err:
            st.warning(f"상세 오류: {err}")
        return

    # Header
    badge_cls = "badge-segment" if data_source == "Segment_Full" else "badge-sheet"
    badge_txt = "Segment Full" if data_source == "Segment_Full" else "UTM생성기 Only"

    update_info = ""
    if data_source == "Segment_Full" and "업데이트" in df.columns:
        latest = df["업데이트"].dropna().unique()
        if len(latest) > 0:
            update_info = f"  |  동기화: {latest[0]}"

    st.markdown(
        f'## UTM Performance Dashboard\n'
        f'<span class="{badge_cls}">{badge_txt}</span> '
        f'<span style="color:#888; font-size:13px;">'
        f"데이터: {len(df)}건  |  "
        f'{datetime.now().strftime("%Y-%m-%d %H:%M")} 기준'
        f"{update_info}</span>",
        unsafe_allow_html=True,
    )

    # Tabs
    t1, t2 = st.tabs(["📊 Performance", "🔗 UTM Generator"])
    with t1:
        render_dashboard(df, data_source)
    with t2:
        render_gen()


if __name__ == "__main__":
    main()
