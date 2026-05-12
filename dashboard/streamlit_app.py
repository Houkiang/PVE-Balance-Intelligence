"""Streamlit dashboard for PVE balance analysis results."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DATASET_NAMES = [
    "hero_balance_report",
    "map_grid_heatmap_report",
    "card_strength_report",
    "card_weapon_growth_curve_report",
    "build_combination_report",
]


@st.cache_data(show_spinner=False)
def load_dataset(base_dir: str, dataset_name: str) -> pd.DataFrame:
    base = Path(base_dir)
    parquet_path = base / f"{dataset_name}.parquet"
    csv_path = base / f"{dataset_name}.csv"

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def render_kpi_row(hero_df: pd.DataFrame) -> None:
    if hero_df.empty:
        st.warning("hero_balance_report 数据缺失。")
        return

    top = hero_df.sort_values("balance_score", ascending=False).head(1)
    avg_success = hero_df["success_50_rate"].mean()
    avg_wave = hero_df["avg_survival_wave"].mean()

    c1, c2, c3 = st.columns(3)
    c1.metric("职业数量", int(hero_df["hero_id"].nunique()))
    c2.metric("全职业平均 50 波达成率", f"{avg_success * 100:.2f}%")
    c3.metric("全职业平均存活轮次", f"{avg_wave:.2f}")

    if not top.empty:
        st.caption(f"当前平衡评分第一职业：{top.iloc[0]['hero_name']} ({top.iloc[0]['balance_score']:.4f})")


def page_data_overview(data: dict[str, pd.DataFrame]) -> None:
    st.subheader("数据概览")
    rows = []
    for name in DATASET_NAMES:
        df = data[name]
        rows.append({"dataset": name, "rows": int(len(df)), "columns": int(df.shape[1]) if not df.empty else 0})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    hero_df = data["hero_balance_report"]
    render_kpi_row(hero_df)


def page_hero_balance(hero_df: pd.DataFrame) -> None:
    st.subheader("职业平衡")
    if hero_df.empty:
        st.warning("hero_balance_report 数据缺失。")
        return

    hero_df = hero_df.sort_values("balance_score", ascending=False)

    chart1 = px.bar(
        hero_df,
        x="hero_name",
        y="success_50_rate",
        color="role_type",
        title="职业 50 波达成率",
        text=hero_df["success_50_rate"].map(lambda x: f"{x * 100:.1f}%"),
    )
    chart1.update_layout(yaxis_tickformat=".0%")

    chart2 = px.bar(
        hero_df,
        x="hero_name",
        y="avg_survival_wave",
        color="role_type",
        title="职业平均存活轮次",
    )

    chart3 = px.bar(
        hero_df,
        x="hero_name",
        y="avg_kill_per_wave",
        color="role_type",
        title="职业每轮平均击杀",
    )

    chart4 = px.bar(
        hero_df,
        x="hero_name",
        y="high_performance_rate",
        color="role_type",
        title="职业高表现率",
        text=hero_df["high_performance_rate"].map(lambda x: f"{x * 100:.1f}%"),
    )
    chart4.update_layout(yaxis_tickformat=".0%")

    st.plotly_chart(chart1, use_container_width=True)
    st.plotly_chart(chart2, use_container_width=True)
    st.plotly_chart(chart3, use_container_width=True)
    st.plotly_chart(chart4, use_container_width=True)

    st.dataframe(
        hero_df[
            [
                "balance_rank",
                "hero_name",
                "role_type",
                "use_count",
                "success_50_rate",
                "avg_survival_wave",
                "avg_kill_per_wave",
                "high_performance_rate",
                "balance_score",
            ]
        ],
        use_container_width=True,
    )


def page_map_heatmap(map_df: pd.DataFrame) -> None:
    st.subheader("地图难点区域")
    if map_df.empty:
        st.warning("map_grid_heatmap_report 数据缺失。")
        return

    map_options = sorted(map_df["map_id"].dropna().unique().tolist())
    selected_map = st.selectbox("选择地图", map_options)
    top_n = st.slider("Top 危险区域数量", 10, 200, 40, 10)

    sub = map_df[map_df["map_id"] == selected_map].copy()

    heat = px.density_heatmap(
        sub,
        x="display_grid_x",
        y="display_grid_y",
        z="danger_score",
        histfunc="avg",
        nbinsx=max(int(sub["display_grid_x"].max() + 1), 10),
        nbinsy=max(int(sub["display_grid_y"].max() + 1), 10),
        title="危险系数热力图 (聚合网格)",
        color_continuous_scale="YlOrRd",
    )
    heat.update_yaxes(autorange="reversed")
    st.plotly_chart(heat, use_container_width=True)

    top_df = sub.sort_values("danger_score", ascending=False).head(top_n)
    st.dataframe(
        top_df[
            [
                "display_grid_x",
                "display_grid_y",
                "danger_score",
                "danger_level",
                "death_count",
                "enemy_spawn_count",
                "stay_duration",
            ]
        ],
        use_container_width=True,
    )


def page_card_strength(card_df: pd.DataFrame, curve_df: pd.DataFrame) -> None:
    st.subheader("卡牌强度")
    if card_df.empty:
        st.warning("card_strength_report 数据缺失。")
        return

    card_df = card_df.sort_values("strength_score", ascending=False)

    top_k = st.slider("排行展示数量", 10, 50, 20, 5)
    sort_metric = st.selectbox("排序指标", ["strength_score", "pick_rate", "success_50_lift", "wave_lift"])
    view_df = card_df.sort_values(sort_metric, ascending=False).head(top_k)

    chart1 = px.bar(
        view_df,
        x="card_name",
        y="pick_rate",
        color="card_type",
        title="卡牌选取率排行",
    )
    chart1.update_layout(yaxis_tickformat=".0%")

    chart2 = px.bar(
        view_df,
        x="card_name",
        y="success_50_lift",
        color="card_type",
        title="50 波达成率提升",
    )

    chart3 = px.bar(
        view_df,
        x="card_name",
        y="wave_lift",
        color="card_type",
        title="平均轮次提升",
    )

    st.plotly_chart(chart1, use_container_width=True)
    st.plotly_chart(chart2, use_container_width=True)
    st.plotly_chart(chart3, use_container_width=True)

    if not curve_df.empty:
        st.markdown("#### 武器升级卡成长曲线")
        curve_options = curve_df["card_name"].dropna().unique().tolist()
        selected_curve = st.selectbox("选择升级卡", sorted(curve_options))
        row = curve_df[curve_df["card_name"] == selected_curve].head(1)
        if not row.empty:
            row = row.iloc[0]
            line_df = pd.DataFrame(
                {
                    "pick_times": [1, 3, 5],
                    "value": [row["value_after_1_pick"], row["value_after_3_pick"], row["value_after_5_pick"]],
                }
            )
            line = px.line(
                line_df,
                x="pick_times",
                y="value",
                markers=True,
                title=f"{selected_curve} - {row['upgrade_attr']} 成长曲线",
            )
            st.plotly_chart(line, use_container_width=True)

    st.dataframe(
        card_df[
            [
                "card_name",
                "card_type",
                "rarity",
                "show_count",
                "effective_show_count",
                "pick_count",
                "pick_rate",
                "success_50_lift",
                "wave_lift",
                "strength_score",
                "is_overpowered_tag",
            ]
        ],
        use_container_width=True,
    )


def page_build_combinations(combo_df: pd.DataFrame) -> None:
    st.subheader("流派组合")
    if combo_df.empty:
        st.warning("build_combination_report 数据缺失。")
        return

    combo_types = sorted(combo_df["combination_type"].dropna().unique().tolist())
    selected_type = st.selectbox("组合类型", combo_types)
    top_k = st.slider("展示 Top N", 10, 100, 30, 10)

    sub = combo_df[combo_df["combination_type"] == selected_type].copy()
    sub = sub.sort_values(["success_50_rate", "support"], ascending=False).head(top_k)

    has_rule_metrics = (
        {"support", "confidence", "lift"}.issubset(sub.columns)
        and sub["confidence"].notna().any()
        and sub["lift"].notna().any()
    )
    if has_rule_metrics:
        chart = px.scatter(
            sub,
            x="support",
            y="confidence",
            color="success_50_rate",
            size="lift",
            hover_data=["items_text", "avg_final_wave", "sample_count"],
            title="支持度-置信度-提升度散点图",
            color_continuous_scale="Tealgrn",
        )
        st.plotly_chart(chart, use_container_width=True)

    st.dataframe(
        sub[
            [
                "combination_type",
                "items_text",
                "support",
                "confidence",
                "lift",
                "avg_final_wave",
                "success_50_rate",
                "sample_count",
            ]
        ],
        use_container_width=True,
    )


def inject_style() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(circle at 5% 10%, #fefce8 0%, #f8fafc 40%, #eef2ff 100%);
        }
        h1, h2, h3 {
            letter-spacing: 0.3px;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
        }
        [data-testid="stSidebar"] * {
            color: #e2e8f0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="PVE Balance Intelligence",
        page_icon="PI",
        layout="wide",
    )
    inject_style()

    st.title("PVE Balance Intelligence Dashboard")
    st.caption("离线 ADS 分析看板：职业平衡、地图难点、卡牌强度、流派组合")

    base_dir = st.sidebar.text_input("ADS 本地目录", value="data/ads_export")

    data = {name: load_dataset(base_dir, name) for name in DATASET_NAMES}

    page = st.sidebar.radio(
        "页面",
        ["数据概览", "职业平衡", "地图难点区域", "卡牌强度", "流派组合"],
    )

    if page == "数据概览":
        page_data_overview(data)
    elif page == "职业平衡":
        page_hero_balance(data["hero_balance_report"])
    elif page == "地图难点区域":
        page_map_heatmap(data["map_grid_heatmap_report"])
    elif page == "卡牌强度":
        page_card_strength(data["card_strength_report"], data["card_weapon_growth_curve_report"])
    else:
        page_build_combinations(data["build_combination_report"])


if __name__ == "__main__":
    main()
