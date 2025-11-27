import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt

# -------------------------------------------------------------------
# 1. KONFIGURASI KONEKSI GOOGLE SHEETS
# -------------------------------------------------------------------

SPREADSHEET_URL_ROFO = "https://docs.google.com/spreadsheets/d/1dcVqGq6wjOtimpw_IDq_BKCxWhyO_z9W5pNBR3FO43Y/edit?usp=sharing"
SPREADSHEET_URL_PO = "https://docs.google.com/spreadsheets/d/17sBiMYXomOSj5SnLwUIoUkVpLTeB4IxNJhUWyxh5DlE/edit?usp=sharing"
SPREADSHEET_URL_SALES = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit?usp=sharing"

SHEET_NAME = "Sheet1"

@st.cache_data(ttl=3600)
def get_service_account():
    """Load Service Account dari secrets.toml"""
    creds = st.secrets["gcp_service_account"]
    gc = gspread.service_account_from_dict(dict(creds))
    return gc

@st.cache_data(ttl=3600)
def load_data_from_gsheet(url: str, sheet_name: str):
    """Load data dari Google Sheets (sheet tertentu)."""
    gc = get_service_account()
    try:
        sh = gc.open_by_url(url)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except Exception as e:
        st.error(f"❌ Error load sheet: {url} | Sheet: {sheet_name} | Error: {e}")
        return pd.DataFrame()

# -------------------------------------------------------------------
# 2. PROSES DATA UTAMA ROFO – PO – SALES
# -------------------------------------------------------------------

@st.cache_data
def process_data(df_rofo, df_po, df_sales):

    # ---------- 2.1. ROFO Wide → Long ----------
    rofo_id_cols = ['SKU GOA', 'Product Name']
    rofo_date_cols = [col for col in df_rofo.columns if col.startswith('202')]

    df_rofo_long = df_rofo.melt(
        id_vars=rofo_id_cols,
        value_vars=rofo_date_cols,
        var_name="Date",
        value_name="ROFO Quantity"
    ).rename(columns={'SKU GOA': 'SKU'})

    df_rofo_long["Date"] = pd.to_datetime(df_rofo_long["Date"], errors="coerce").dt.to_period("M")
    df_rofo_long["ROFO Quantity"] = pd.to_numeric(df_rofo_long["ROFO Quantity"], errors="coerce").fillna(0).clip(lower=0)
    df_rofo_long.dropna(subset=["Date"], inplace=True)

    # ---------- 2.2. PO Processing ----------
    df_po["Date"] = pd.to_datetime(df_po["Delivery Date"], errors="coerce").dt.to_period("M")
    df_po["Actual Quantity"] = pd.to_numeric(df_po["Confirm Quantity"], errors="coerce").fillna(0).clip(lower=0)

    df_po_long = df_po.groupby(["Material", "Date"])["Actual Quantity"].sum().reset_index()
    df_po_long = df_po_long.rename(columns={"Material": "SKU"})

    # ---------- 2.3. MERGE ----------
    df_merged = pd.merge(
        df_rofo_long[["SKU", "Date", "ROFO Quantity"]],
        df_po_long[["SKU", "Date", "Actual Quantity"]],
        on=["SKU", "Date"],
        how="outer"
    ).fillna(0)

    df_merged = df_merged[(df_merged["ROFO Quantity"] > 0) | (df_merged["Actual Quantity"] > 0)]
    df_merged["Date"] = df_merged["Date"].astype(str)

    # ---------- 2.4. FAR & Accuracy ----------
    def calculate_far(row):
        rofo = row["ROFO Quantity"]
        actual = row["Actual Quantity"]
        if rofo == 0:
            return 10.0 if actual > 0 else 1.0
        return actual / rofo

    df_merged["FAR"] = df_merged.apply(calculate_far, axis=1)

    df_merged["Accuracy Status"] = np.where(
        (df_merged["FAR"] >= 0.8) & (df_merged["FAR"] <= 1.2),
        "Accurate", "Non-Accurate"
    )

    df_merged["Bias"] = df_merged["ROFO Quantity"] - df_merged["Actual Quantity"]

    return df_merged

# -------------------------------------------------------------------
# 3. DASHBOARD UTAMA
# -------------------------------------------------------------------

def create_dashboard(df, df_sales):

    st.header("📊 KPI Utama Forecasting")

    # ---------- Filter SKU ----------
    all_sku = ["All"] + sorted(df["SKU"].unique().tolist())
    selected_sku = st.sidebar.selectbox("Filter SKU:", all_sku)

    df_filtered = df if selected_sku == "All" else df[df["SKU"] == selected_sku]

    if df_filtered.empty:
        st.warning("❗ Data kosong untuk SKU tersebut.")
        return

    # ---------- KPI ----------
    total_rofo = df_filtered["ROFO Quantity"].sum()
    total_actual = df_filtered["Actual Quantity"].sum()
    total_bias = total_rofo - total_actual

    far_overall = total_actual / total_rofo if total_rofo > 0 else 0

    acc_count = (df_filtered["Accuracy Status"] == "Accurate").sum()
    total_count = len(df_filtered)
    accuracy_ratio = acc_count / total_count if total_count > 0 else 0

    col1, col2, col3 = st.columns(3)

    col1.metric("Overall FAR", f"{far_overall:.1%}", "Target 80%–120%", delta_color="off")
    col2.metric("Accuracy by Count", f"{accuracy_ratio:.1%}", f"{acc_count} of {total_count}", delta_color="off")
    col3.metric("Total Bias", f"{total_bias:,.0f}", "Positif = Over forecast", delta_color="off")

    st.markdown("---")

    # ---------- Trend Chart ----------
    st.subheader("📈 Tren ROFO vs PO Submitted (Actual)")

    df_monthly = df_filtered.groupby("Date").agg(
        ROFO=("ROFO Quantity", "sum"),
        Actual=("Actual Quantity", "sum")
    ).reset_index()

    df_trend = df_monthly.melt("Date", var_name="Type", value_name="Quantity")

    chart = alt.Chart(df_trend).mark_line(point=True).encode(
        x=alt.X("Date:O", title="Bulan"),
        y=alt.Y("Quantity:Q", title="Qty"),
        color="Type",
        tooltip=["Date", "Type", alt.Tooltip("Quantity:Q", format=",")]
    ).properties(height=400)

    st.altair_chart(chart, use_container_width=True)

    # ---------- Top 10 Bias ----------
    st.subheader("🔥 Top 10 SKU Berdasarkan Bias (ROFO - Actual)")

    df_bias = df_filtered.groupby("SKU")["Bias"].sum().reset_index().sort_values("Bias", ascending=False).head(10)

    chart_bias = alt.Chart(df_bias).mark_bar().encode(
        x=alt.X("Bias:Q", title="Bias"),
        y=alt.Y("SKU:N", sort="-x"),
        color=alt.condition(
            alt.datum.Bias > 0,
            alt.value("red"),
            alt.value("green")
        ),
        tooltip=["SKU", alt.Tooltip("Bias:Q", format=",")]
    ).properties(height=300)

    st.altair_chart(chart_bias, use_container_width=True)

    # ---------- Data Table ----------
    st.subheader("📄 Detail Data FAR")
    st.dataframe(df_filtered.head(100))

    # ---------- Sales Display ----------
    st.header("📦 Data Sales (End Customer)")
    st.dataframe(df_sales.head())

# -------------------------------------------------------------------
# 4. MAIN EXECUTION
# -------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Forecast Accuracy Dashboard", layout="wide")

    st.sidebar.title("📌 Forecast Dashboard")
    st.title("📊 Forecast Accuracy & Achievement Dashboard")

    with st.spinner("Memuat data dari Google Sheets..."):
        df_rofo = load_data_from_gsheet(SPREADSHEET_URL_ROFO, SHEET_NAME)
        df_po = load_data_from_gsheet(SPREADSHEET_URL_PO, SHEET_NAME)
        df_sales = load_data_from_gsheet(SPREADSHEET_URL_SALES, SHEET_NAME)

    if df_rofo.empty or df_po.empty:
        st.error("❌ Gagal load data ROFO atau PO. Cek izin share & secrets.toml.")
        return

    df_processed = process_data(df_rofo, df_po, df_sales)
    create_dashboard(df_processed, df_sales)


if __name__ == "__main__":
    main()
