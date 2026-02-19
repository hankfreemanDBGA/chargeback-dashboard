import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector

st.set_page_config(page_title="Agent Commission Analysis", layout="wide")
st.title("Agent Commission Analysis")


@st.cache_resource
def get_connection():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg["role"],
    )


@st.cache_data
def load_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH base AS (
            SELECT
                acp.*,
                p.POLICY_NUMBER
            FROM raw.ams.agent_commission_paid acp
            JOIN raw.ams.policies p ON acp.POLICY_ID = p.ID
        ),
        first_statement AS (
            SELECT
                POLICY_ID,
                POLICY_NUMBER,
                MIN(STATEMENT_DATE) AS FIRST_STATEMENT_DATE
            FROM base
            GROUP BY POLICY_ID, POLICY_NUMBER
        ),
        advance AS (
            SELECT
                b.POLICY_ID,
                SUM(b.PAID_OVERRIDE_AMOUNT) AS ADVANCE_AMOUNT
            FROM base b
            JOIN first_statement fs
                ON b.POLICY_ID = fs.POLICY_ID
                AND b.STATEMENT_DATE = fs.FIRST_STATEMENT_DATE
            WHERE b.PAID_OVERRIDE_AMOUNT > 0
            GROUP BY b.POLICY_ID
        ),
        chargebacks AS (
            SELECT
                POLICY_ID,
                SUM(PAID_OVERRIDE_AMOUNT) AS CHARGEBACK_AMOUNT
            FROM base
            WHERE PAID_OVERRIDE_AMOUNT < 0
            GROUP BY POLICY_ID
        )
        SELECT
            fs.POLICY_ID,
            fs.POLICY_NUMBER,
            TO_VARCHAR(fs.FIRST_STATEMENT_DATE, 'YYYY-MM-DD') AS FIRST_STATEMENT_DATE,
            COALESCE(a.ADVANCE_AMOUNT, 0)    AS ADVANCE_AMOUNT,
            COALESCE(c.CHARGEBACK_AMOUNT, 0) AS CHARGEBACK_AMOUNT
        FROM first_statement fs
        LEFT JOIN advance a      ON fs.POLICY_ID = a.POLICY_ID
        LEFT JOIN chargebacks c  ON fs.POLICY_ID = c.POLICY_ID
    """)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=cols)


def render_all(data):
    total = len(data)
    active_n = (data["STATUS"] == "Active").sum()
    cb_n = (data["STATUS"] == "Charged Back").sum()
    total_advanced = data["ADVANCE_AMOUNT"].sum()
    total_cb = data["CHARGEBACK_AMOUNT"].sum()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Policies", f"{total:,}")
    k2.metric("Active", f"{active_n:,}")
    k3.metric("Charged Back", f"{cb_n:,}")
    k4.metric("Total Advanced", f"${total_advanced:,.0f}")
    k5.metric("Total Chargebacks", f"${total_cb:,.0f}")

    st.divider()

    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("Policy Status")
        if total == 0:
            st.info("No data for this segment.")
        else:
            status_df = data["STATUS"].value_counts().reset_index()
            status_df.columns = ["Status", "Count"]
            donut = (
                alt.Chart(status_df)
                .mark_arc(innerRadius=60)
                .encode(
                    theta=alt.Theta("Count:Q"),
                    color=alt.Color(
                        "Status:N",
                        scale=alt.Scale(
                            domain=["Active", "Charged Back"],
                            range=["#2ecc71", "#e74c3c"],
                        ),
                    ),
                    tooltip=["Status", "Count"],
                )
                .properties(height=280)
            )
            st.altair_chart(donut, use_container_width=True)

    with col_right:
        st.subheader("Distribution of Months Paid (Charged-Back Policies)")
        cb_data = data[data["STATUS"] == "Charged Back"]
        if cb_data.empty:
            st.info("No charged-back policies in this segment.")
        else:
            month_dist = cb_data["MONTHS_PAID"].value_counts().reset_index()
            month_dist.columns = ["Months Paid", "Count"]
            month_dist = month_dist.sort_values("Months Paid")
            bar = (
                alt.Chart(month_dist)
                .mark_bar(color="#e67e22")
                .encode(
                    x=alt.X("Months Paid:O", title="Months Paid Before Chargeback"),
                    y=alt.Y("Count:Q"),
                    tooltip=["Months Paid", "Count"],
                )
                .properties(height=280)
            )
            st.altair_chart(bar, use_container_width=True)

    st.divider()
    st.subheader("Advanced vs Charged Back Over Time")

    agg_opt = st.radio("Aggregate by", ["Day", "Week", "Month"], horizontal=True, key="agg_all")
    freq_map = {"Day": "D", "Week": "W", "Month": "MS"}
    freq = freq_map[agg_opt]
    x_format = "%B %Y" if agg_opt == "Month" else "%b %d, %Y"

    if data.empty:
        st.info("No data to display.")
    else:
        ts = (
            data.set_index("FIRST_STATEMENT_DATE")
            .resample(freq)[["ADVANCE_AMOUNT", "CHARGEBACK_AMOUNT"]]
            .sum()
            .reset_index()
        )
        ts.columns = ["Date", "Advanced", "Chargeback"]
        ts["Chargeback"] = ts["Chargeback"].abs()
        ts_long = ts.melt("Date", var_name="Type", value_name="Amount")

        line = (
            alt.Chart(ts_long)
            .mark_line(point=True)
            .encode(
                x=alt.X("Date:T", title="Statement Date", axis=alt.Axis(format=x_format)),
                y=alt.Y("Amount:Q", title="Amount ($)"),
                color=alt.Color(
                    "Type:N",
                    scale=alt.Scale(
                        domain=["Advanced", "Chargeback"],
                        range=["#3498db", "#e74c3c"],
                    ),
                ),
                tooltip=["Date:T", "Type:N", alt.Tooltip("Amount:Q", format="$,.0f")],
            )
            .properties(height=350)
            .interactive()
        )
        st.altair_chart(line, use_container_width=True)

    st.divider()
    st.subheader("Monthly Chargeback Mix — Months Paid Distribution")

    cb_data = data[data["STATUS"] == "Charged Back"]
    if cb_data.empty:
        st.info("No charged-back policies in this segment.")
    else:
        cb_data = cb_data.copy()
        cb_data["YearMonth"] = cb_data["FIRST_STATEMENT_DATE"].dt.to_period("M").astype(str)
        heatmap_df = (
            cb_data.groupby(["YearMonth", "MONTHS_PAID"])
            .size()
            .reset_index(name="Count")
        )
        heatmap = (
            alt.Chart(heatmap_df)
            .mark_rect()
            .encode(
                x=alt.X("YearMonth:O", title="Month"),
                y=alt.Y("MONTHS_PAID:O", title="Months Paid", sort="descending"),
                color=alt.Color("Count:Q", scale=alt.Scale(scheme="orangered")),
                tooltip=["YearMonth", "MONTHS_PAID", "Count"],
            )
            .properties(height=320)
        )
        st.altair_chart(heatmap, use_container_width=True)

    st.divider()
    with st.expander("View raw policy data"):
        display_cols = ["POLICY_NUMBER", "POLICY_ID", "FIRST_STATEMENT_DATE",
                        "ADVANCE_AMOUNT", "CHARGEBACK_AMOUNT", "MONTHS_PAID", "STATUS"]
        st.dataframe(data[display_cols].sort_values("FIRST_STATEMENT_DATE", ascending=False),
                     use_container_width=True)


def render_segment(data, label, show_avg_months=False):
    total = len(data)
    total_cb = data["CHARGEBACK_AMOUNT"].sum()
    avg_cb = (total_cb / total) if total > 0 else 0
    avg_months = data["MONTHS_PAID"].mean() if total > 0 else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Policies", f"{total:,}")
    k2.metric("Total Chargebacks", f"${total_cb:,.0f}")
    k3.metric("Avg Chargeback per Policy", f"${avg_cb:,.0f}")
    k4.metric("Avg Months Paid", f"{avg_months:.1f}" if show_avg_months else "0")

    st.divider()
    st.subheader("Chargebacks Over Time")

    agg_opt = st.radio("Aggregate by", ["Day", "Week", "Month"], horizontal=True, key=f"agg_{label}")
    freq_map = {"Day": "D", "Week": "W", "Month": "MS"}
    freq = freq_map[agg_opt]
    x_format = "%B %Y" if agg_opt == "Month" else "%b %d, %Y"

    if data.empty:
        st.info("No data to display.")
    else:
        ts = (
            data.set_index("FIRST_STATEMENT_DATE")
            .resample(freq)
            .agg(
                Count=("POLICY_ID", "count"),
                Chargeback=("CHARGEBACK_AMOUNT", "sum"),
                Avg_Months=("MONTHS_PAID", "mean"),
            )
            .reset_index()
        )
        ts["Chargeback"] = ts["Chargeback"].abs()

        c1 = (
            alt.Chart(ts)
            .mark_line(point=True, color="#e74c3c")
            .encode(
                x=alt.X("FIRST_STATEMENT_DATE:T", title="Date", axis=alt.Axis(format=x_format)),
                y=alt.Y("Count:Q", title="# Policies Charged Back"),
                tooltip=["FIRST_STATEMENT_DATE:T", alt.Tooltip("Count:Q")],
            )
            .properties(height=250, title="Count of Charged-Back Policies")
            .interactive()
        )

        c2 = (
            alt.Chart(ts)
            .mark_line(point=True, color="#8e44ad")
            .encode(
                x=alt.X("FIRST_STATEMENT_DATE:T", title="Date", axis=alt.Axis(format=x_format)),
                y=alt.Y("Chargeback:Q", title="Total Chargeback ($)"),
                tooltip=["FIRST_STATEMENT_DATE:T", alt.Tooltip("Chargeback:Q", format="$,.0f")],
            )
            .properties(height=250, title="Total Chargeback Amount")
            .interactive()
        )

        c3 = (
            alt.Chart(ts)
            .mark_line(point=True, color="#e67e22")
            .encode(
                x=alt.X("FIRST_STATEMENT_DATE:T", title="Date", axis=alt.Axis(format=x_format)),
                y=alt.Y("Avg_Months:Q", title="Avg Months Paid"),
                tooltip=["FIRST_STATEMENT_DATE:T", alt.Tooltip("Avg_Months:Q", format=".1f")],
            )
            .properties(height=250, title="Average Months Paid Before Chargeback")
            .interactive()
        )

        st.altair_chart(c1, use_container_width=True)
        st.altair_chart(c2, use_container_width=True)
        if show_avg_months:
            st.altair_chart(c3, use_container_width=True)

    st.divider()
    st.subheader("Months Paid Distribution")
    if data.empty:
        st.info("No data to display.")
    else:
        month_dist = data["MONTHS_PAID"].value_counts().reset_index()
        month_dist.columns = ["Months Paid", "Count"]
        month_dist = month_dist.sort_values("Months Paid")
        bar = (
            alt.Chart(month_dist)
            .mark_bar(color="#e67e22")
            .encode(
                x=alt.X("Months Paid:O", title="Months Paid Before Chargeback"),
                y=alt.Y("Count:Q"),
                tooltip=["Months Paid", "Count"],
            )
            .properties(height=280)
        )
        st.altair_chart(bar, use_container_width=True)

    st.divider()
    with st.expander("View raw policy data"):
        display_cols = ["POLICY_NUMBER", "POLICY_ID", "FIRST_STATEMENT_DATE",
                        "ADVANCE_AMOUNT", "CHARGEBACK_AMOUNT", "MONTHS_PAID", "STATUS"]
        st.dataframe(data[display_cols].sort_values("FIRST_STATEMENT_DATE", ascending=False),
                     use_container_width=True)


# ── Load & clean ───────────────────────────────────────────────────────────────
df = load_data()

df["FIRST_STATEMENT_DATE"] = pd.to_datetime(df["FIRST_STATEMENT_DATE"], errors="coerce")
bad = df["FIRST_STATEMENT_DATE"].isna().sum()
if bad:
    st.warning(f"Dropped {bad} row(s) with invalid/out-of-bounds dates.")
df = df.dropna(subset=["FIRST_STATEMENT_DATE"])

df["CB_FRACTION"] = df.apply(
    lambda r: min(abs(r["CHARGEBACK_AMOUNT"]) / r["ADVANCE_AMOUNT"], 1.0)
    if r["ADVANCE_AMOUNT"] > 0 else 0,
    axis=1,
)
df["MONTHS_PAID"] = df["CB_FRACTION"].apply(lambda x: round(12 * (1 - x)))
df["STATUS"] = df["CHARGEBACK_AMOUNT"].apply(
    lambda x: "Active" if x == 0 else "Charged Back"
)

# ── Sidebar date filter ────────────────────────────────────────────────────────
min_date = df["FIRST_STATEMENT_DATE"].min().date()
max_date = df["FIRST_STATEMENT_DATE"].max().date()
default_start = max(min_date, pd.Timestamp("2024-01-01").date())

date_start, date_end = st.sidebar.date_input(
    "Date range (First Statement Date)",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date,
)
df = df[
    (df["FIRST_STATEMENT_DATE"].dt.date >= date_start) &
    (df["FIRST_STATEMENT_DATE"].dt.date <= date_end)
]

# ── Segment dataframes ─────────────────────────────────────────────────────────
df_ip  = df[(df["STATUS"] == "Charged Back") & (df["MONTHS_PAID"] == 0)]
df_nip = df[(df["STATUS"] == "Charged Back") & (df["MONTHS_PAID"] > 0)]

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_all, tab_ip, tab_nip = st.tabs(["All Policies", "IP Returns (Month 0)", "Non-IP Returns (Month 1+)"])

with tab_all:
    render_all(df)

with tab_ip:
    st.caption("Policies that were fully charged back with **0 months paid** (In-Policy returns).")
    render_segment(df_ip, "ip", show_avg_months=False)

with tab_nip:
    st.caption("Policies that were charged back after **1 or more months paid**.")
    render_segment(df_nip, "nip", show_avg_months=True)
