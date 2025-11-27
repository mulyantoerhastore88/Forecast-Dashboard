import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt
from google.oauth2.service_account import Credentials

# -------------------------------------------------------------------
# 1. KONFIGURASI KONEKSI GOOGLE SHEETS
# -------------------------------------------------------------------

SPREADSHEET_URL_ROFO = "https://docs.google.com/spreadsheets/d/17sBIMYXomOSjSSnLwUJoJlWpLTeB4ixNJhJWyxh5DIE/edit?usp=sharing"
SPREADSHEET_URL_PO = "https://docs.google.com/spreadsheets/d/1PuolI49N-IWOaNO8fSMYGwuVFfIT68_Kez30WN9q8Ds/edit?usp=sharing"
SPREADSHEET_URL_SALES = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit?usp=sharing"

SHEET_NAME = "Sheet1"

@st.cache_data(ttl=3600)
def get_service_account():
    """Load Service Account dari secrets.toml"""
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        gc = gspread.service_account_from_dict(creds_dict)
        return gc
    except Exception as e:
        st.error(f"Error in service account: {e}")
        return None

@st.cache_data(ttl=3600)
def load_data_from_gsheet(url: str, sheet_name: str):
    """Load data dari Google Sheets (sheet tertentu)"""
    try:
        gc = get_service_account()
        if gc is None:
            st.error("❌ Gagal menginisialisasi koneksi Google Sheets")
            return pd.DataFrame()
            
        sh = gc.open_by_url(url)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_values()
        
        if len(data) == 0:
            st.warning(f"⚠️ Sheet {sheet_name} kosong")
            return pd.DataFrame()
            
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
        
    except gspread.SpreadsheetNotFound:
        st.error(f"❌ Spreadsheet tidak ditemukan: {url}")
        return pd.DataFrame()
    except gspread.WorksheetNotFound:
        st.error(f"❌ Worksheet '{sheet_name}' tidak ditemukan")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error load sheet: {url} | Sheet: {sheet_name} | Error: {e}")
        return pd.DataFrame()

# -------------------------------------------------------------------
# 2. PROSES DATA UTAMA ROFO – PO – SALES
# -------------------------------------------------------------------

@st.cache_data
def process_data(df_rofo, df_po, df_sales):
    
    # Validasi data input
    if df_rofo.empty or df_po.empty:
        st.error("Data ROFO atau PO kosong, tidak dapat memproses")
        return pd.DataFrame()

    try:
        # ---------- 2.1. ROFO Wide → Long ----------
        
        # Cari kolom yang sesuai
        sku_col = None
        for col in df_rofo.columns:
            if 'sku' in col.lower() or 'SKU' in col:
                sku_col = col
                break
        
        if sku_col is None:
            # Jika tidak ada kolom SKU, gunakan kolom pertama sebagai default
            sku_col = df_rofo.columns[0]
            st.warning(f"⚠️ Kolom SKU tidak ditemukan, menggunakan kolom pertama: {sku_col}")
        
        rofo_id_cols = [sku_col]
        # Coba tambahkan Product Name jika ada
        if 'Product Name' in df_rofo.columns:
            rofo_id_cols.append('Product Name')
        
        # Hanya ambil kolom yang ada di dataframe
        rofo_id_cols = [col for col in rofo_id_cols if col in df_rofo.columns]
        
        rofo_date_cols = [col for col in df_rofo.columns if col.startswith('202')]
        
        if not rofo_date_cols:
            st.error("❌ Kolom tanggal (dimulai dengan 202) tidak ditemukan di ROFO")
            st.info(f"Kolom yang tersedia: {list(df_rofo.columns)}")
            return pd.DataFrame()

        df_rofo_long = df_rofo.melt(
            id_vars=rofo_id_cols,
            value_vars=rofo_date_cols,
            var_name="Date",
            value_name="ROFO Quantity"
        ).rename(columns={sku_col: 'SKU'})

        df_rofo_long["Date"] = pd.to_datetime(df_rofo_long["Date"], errors="coerce").dt.to_period("M")
        df_rofo_long["ROFO Quantity"] = pd.to_numeric(df_rofo_long["ROFO Quantity"], errors="coerce").fillna(0).clip(lower=0)
        df_rofo_long.dropna(subset=["Date"], inplace=True)

        # ---------- 2.2. PO Processing ----------
        
        # Cari kolom yang sesuai di PO data
        date_col = None
        qty_col = None
        sku_col_po = None
        
        # Cari kolom berdasarkan pattern
        for col in df_po.columns:
            col_lower = col.lower()
            if 'date' in col_lower or 'delivery' in col_lower:
                date_col = col
            elif 'qty' in col_lower or 'quantity' in col_lower or 'confirm' in col_lower:
                qty_col = col
            elif 'material' in col_lower or 'sku' in col_lower:
                sku_col_po = col
        
        # Default values jika tidak ditemukan
        if date_col is None: date_col = df_po.columns[0]
        if qty_col is None: qty_col = df_po.columns[1] if len(df_po.columns) > 1 else df_po.columns[0]
        if sku_col_po is None: sku_col_po = df_po.columns[2] if len(df_po.columns) > 2 else df_po.columns[0]

        df_po["Date"] = pd.to_datetime(df_po[date_col], errors="coerce").dt.to_period("M")
        df_po["Actual Quantity"] = pd.to_numeric(df_po[qty_col], errors="coerce").fillna(0).clip(lower=0)

        df_po_long = df_po.groupby([sku_col_po, "Date"])["Actual Quantity"].sum().reset_index()
        df_po_long = df_po_long.rename(columns={sku_col_po: "SKU"})

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

    except Exception as e:
        st.error(f"❌ Error dalam proses data: {e}")
        return pd.DataFrame()

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

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total ROFO", f"{total_rofo:,.0f}")
    col2.metric("Total Actual", f"{total_actual:,.0f}")
    col3.metric("Overall FAR", f"{far_overall:.1%}", "Target 80%–120%", delta_color="off")
    col4.metric("Accuracy Ratio", f"{accuracy_ratio:.1%}", f"{acc_count} of {total_count}")

    col5, col6, col7 = st.columns(3)
    col5.metric("Total Bias", f"{total_bias:,.0f}", "Positif = Over forecast", delta_color="inverse")
    
    # Hitung additional metrics
    over_forecast = len(df_filtered[df_filtered["Bias"] > 0])
    under_forecast = len(df_filtered[df_filtered["Bias"] < 0])
    perfect_forecast = len(df_filtered[df_filtered["Bias"] == 0])
    
    col6.metric("Over Forecast", f"{over_forecast}", "SKU dengan Bias > 0")
    col7.metric("Under Forecast", f"{under_forecast}", "SKU dengan Bias < 0")

    st.markdown("---")

    # ---------- Trend Chart ----------
    st.subheader("📈 Tren ROFO vs PO Submitted (Actual)")

    df_monthly = df_filtered.groupby("Date").agg(
        ROFO=("ROFO Quantity", "sum"),
        Actual=("Actual Quantity", "sum"),
        FAR=("FAR", "mean")
    ).reset_index()

    df_trend = df_monthly.melt("Date", value_vars=["ROFO", "Actual"], var_name="Type", value_name="Quantity")

    chart = alt.Chart(df_trend).mark_line(point=True).encode(
        x=alt.X("Date:O", title="Bulan"),
        y=alt.Y("Quantity:Q", title="Qty"),
        color=alt.Color("Type", scale=alt.Scale(domain=['ROFO', 'Actual'], range=['#1f77b4', '#ff7f0e'])),
        strokeDash=alt.StrokeDash("Type", scale=alt.Scale(domain=['ROFO', 'Actual'], range=[[1, 0], [5, 5]])),
        tooltip=["Date", "Type", alt.Tooltip("Quantity:Q", format=",")]
    ).properties(height=400, title="Tren ROFO vs Actual Quantity per Bulan")

    st.altair_chart(chart, use_container_width=True)

    # ---------- FAR Trend Chart ----------
    st.subheader("📊 Tren FAR (Forecast Accuracy Ratio) per Bulan")

    chart_far = alt.Chart(df_monthly).mark_line(point=True, color='red').encode(
        x=alt.X("Date:O", title="Bulan"),
        y=alt.Y("FAR:Q", title="FAR Ratio", scale=alt.Scale(domain=[0, max(2, df_monthly['FAR'].max())])),
        tooltip=["Date", alt.Tooltip("FAR:Q", format=".2%")]
    ).properties(height=300, title="Tren FAR per Bulan")
    
    # Add reference lines
    rule_upper = alt.Chart(pd.DataFrame({'y': [1.2]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y:Q')
    rule_lower = alt.Chart(pd.DataFrame({'y': [0.8]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y:Q')
    rule_target = alt.Chart(pd.DataFrame({'y': [1.0]})).mark_rule(color='blue', strokeDash=[3,3]).encode(y='y:Q')
    
    st.altair_chart(chart_far + rule_upper + rule_lower + rule_target, use_container_width=True)

    # ---------- Top 10 Bias ----------
    st.subheader("🔥 Top 10 SKU Berdasarkan Bias (ROFO - Actual)")

    df_bias = df_filtered.groupby("SKU").agg({
        "Bias": "sum",
        "ROFO Quantity": "sum",
        "Actual Quantity": "sum"
    }).reset_index().sort_values("Bias", ascending=False).head(10)

    chart_bias = alt.Chart(df_bias).mark_bar().encode(
        x=alt.X("Bias:Q", title="Bias"),
        y=alt.Y("SKU:N", sort="-x", title="SKU"),
        color=alt.condition(
            alt.datum.Bias > 0,
            alt.value("red"),   # Over forecast
            alt.value("green")  # Under forecast
        ),
        tooltip=["SKU", 
                alt.Tooltip("Bias:Q", format=","),
                alt.Tooltip("ROFO Quantity:Q", format=",", title="ROFO"),
                alt.Tooltip("Actual Quantity:Q", format=",", title="Actual")]
    ).properties(height=400, title="Top 10 SKU dengan Bias Tertinggi")

    st.altair_chart(chart_bias, use_container_width=True)

    # ---------- Accuracy Distribution ----------
    st.subheader("📋 Distribusi Akurasi Forecast")

    accuracy_dist = df_filtered["Accuracy Status"].value_counts().reset_index()
    accuracy_dist.columns = ["Status", "Count"]
    
    chart_pie = alt.Chart(accuracy_dist).mark_arc().encode(
        theta=alt.Theta(field="Count", type="quantitative"),
        color=alt.Color(field="Status", type="nominal", 
                       scale=alt.Scale(domain=['Accurate', 'Non-Accurate'], 
                                      range=['green', 'red'])),
        tooltip=["Status", "Count"]
    ).properties(height=300, title="Distribusi Status Akurasi")
    
    st.altair_chart(chart_pie, use_container_width=True)

    # ---------- Data Table ----------
    st.subheader("📄 Detail Data FAR")
    
    # Format dataframe untuk display
    df_display = df_filtered.copy()
    df_display["FAR"] = df_display["FAR"].apply(lambda x: f"{x:.1%}")
    df_display["ROFO Quantity"] = df_display["ROFO Quantity"].apply(lambda x: f"{x:,.0f}")
    df_display["Actual Quantity"] = df_display["Actual Quantity"].apply(lambda x: f"{x:,.0f}")
    df_display["Bias"] = df_display["Bias"].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(df_display.head(100), use_container_width=True)
    
    # Download button
    csv = df_filtered.to_csv(index=False)
    st.download_button(
        label="📥 Download Data sebagai CSV",
        data=csv,
        file_name="forecast_accuracy_data.csv",
        mime="text/csv",
    )

    # ---------- Sales Display ----------
    if not df_sales.empty:
        st.header("📦 Data Sales (End Customer)")
        st.dataframe(df_sales.head(50), use_container_width=True)

# -------------------------------------------------------------------
# 4. MAIN EXECUTION
# -------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Forecast Accuracy Dashboard", 
        layout="wide",
        page_icon="📊"
    )

    st.sidebar.title("📌 Forecast Dashboard")
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Dashboard Features:**
    - Forecast Accuracy Analysis
    - ROFO vs Actual Comparison  
    - Bias Analysis
    - Trend Visualization
    """)
    
    st.title("📊 Forecast Accuracy & Achievement Dashboard")

    # Load data section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Loading Status")
    
    with st.spinner("Memuat data dari Google Sheets..."):
        df_rofo = load_data_from_gsheet(SPREADSHEET_URL_ROFO, SHEET_NAME)
        df_po = load_data_from_gsheet(SPREADSHEET_URL_PO, SHEET_NAME)
        df_sales = load_data_from_gsheet(SPREADSHEET_URL_SALES, SHEET_NAME)

    # Display data info in sidebar
    st.sidebar.write(f"📁 ROFO Data: {len(df_rofo)} rows")
    st.sidebar.write(f"📁 PO Data: {len(df_po)} rows") 
    st.sidebar.write(f"📁 Sales Data: {len(df_sales)} rows")

    if df_rofo.empty or df_po.empty:
        st.error("""
        ❌ Gagal load data ROFO atau PO. 
        
        **Kemungkinan penyebab:**
        1. Izin akses Google Sheets belum diberikan ke service account
        2. Format secrets.toml tidak sesuai
        3. URL spreadsheet salah
        4. Struktur data tidak sesuai
        
        **Solusi:**
        - Pastikan file Google Sheets sudah di-share ke email service account
        - Cek format secrets.toml di folder .streamlit/
        """)
        
        # Show available columns for debugging
        if not df_rofo.empty:
            st.write("**Kolom ROFO yang tersedia:**", list(df_rofo.columns))
        if not df_po.empty:
            st.write("**Kolom PO yang tersedia:**", list(df_po.columns))
            
        return

    # Process data
    with st.spinner("Memproses data..."):
        df_processed = process_data(df_rofo, df_po, df_sales)

    if not df_processed.empty:
        st.sidebar.success(f"✅ Data processed: {len(df_processed)} rows")
        create_dashboard(df_processed, df_sales)
    else:
        st.error("Gagal memproses data. Periksa struktur data sumber.")

if __name__ == "__main__":
    main()
