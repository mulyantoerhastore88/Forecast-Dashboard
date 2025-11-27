import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt
from datetime import datetime
import traceback

# -------------------------------------------------------------------
# 1. KONFIGURASI KONEKSI GOOGLE SHEETS - FIXED VERSION
# -------------------------------------------------------------------

SPREADSHEET_URL_ROFO = "https://docs.google.com/spreadsheets/d/17sBIMYXomOSjSSnLwUJoJlWpLTeB4ixNJhJWyxh5DIE/edit?usp=sharing"
SPREADSHEET_URL_PO = "https://docs.google.com/spreadsheets/d/1PuolI49N-IWOaNO8fSMYGwuVFfIT68_Kez30WN9q8Ds/edit?usp=sharing"
SPREADSHEET_URL_SALES = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit?usp=sharing"

SHEET_NAME = "Sheet1"

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'df_processed' not in st.session_state:
    st.session_state.df_processed = None
if 'df_sales' not in st.session_state:
    st.session_state.df_sales = None

def get_service_account():
    """Load Service Account dari secrets.toml - FIXED VERSION"""
    try:
        # Method 1: Try direct service account from dict
        if 'gcp_service_account' in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            gc = gspread.service_account_from_dict(creds_dict)
            return gc
        else:
            st.error("❌ gcp_service_account tidak ditemukan di secrets.toml")
            return None
    except Exception as e:
        st.error(f"❌ Error in service account: {str(e)}")
        st.info("🔧 Pastikan konfigurasi secrets.toml sudah benar")
        return None

def load_data_from_gsheet(url: str, sheet_name: str):
    """Load data dari Google Sheets dengan error handling yang lebih baik"""
    try:
        gc = get_service_account()
        if gc is None:
            return pd.DataFrame()
            
        # Open spreadsheet
        sh = gc.open_by_url(url)
        
        # Get worksheet
        worksheet = sh.worksheet(sheet_name)
        
        # Get all data
        data = worksheet.get_all_values()
        
        if not data or len(data) <= 1:
            st.warning(f"⚠️ Data kosong atau hanya header di {sheet_name}")
            return pd.DataFrame()
        
        # Create DataFrame
        headers = data[0]
        rows = data[1:]
        
        df = pd.DataFrame(rows, columns=headers)
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        st.success(f"✅ Berhasil load: {len(df)} rows, {len(df.columns)} columns")
        return df
        
    except gspread.SpreadsheetNotFound:
        st.error(f"❌ Spreadsheet tidak ditemukan: {url}")
        return pd.DataFrame()
    except gspread.WorksheetNotFound:
        st.error(f"❌ Worksheet '{sheet_name}' tidak ditemukan di {url}")
        return pd.DataFrame()
    except gspread.APIError as e:
        st.error(f"❌ API Error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Unexpected error loading {url}: {str(e)}")
        return pd.DataFrame()

# -------------------------------------------------------------------
# 2. PROSES DATA UTAMA - FIXED VERSION
# -------------------------------------------------------------------

def process_data(df_rofo, df_po, df_sales):
    """Process data dengan error handling yang lebih baik"""
    try:
        # Validasi data input
        if df_rofo.empty:
            st.error("❌ Data ROFO kosong")
            return pd.DataFrame()
        if df_po.empty:
            st.error("❌ Data PO kosong") 
            return pd.DataFrame()

        st.info("🔄 Memproses data ROFO...")
        
        # ---------- ROFO Processing ----------
        # Find SKU column
        sku_col = None
        for col in df_rofo.columns:
            if any(keyword in col.lower() for keyword in ['sku', 'material', 'kode', 'code', 'item']):
                sku_col = col
                break
        
        if sku_col is None:
            sku_col = df_rofo.columns[0]  # Use first column as fallback
            st.warning(f"⚠️ Kolom SKU tidak terdeteksi, menggunakan: {sku_col}")

        # Find date columns (starting with 202 or containing date)
        date_cols = []
        for col in df_rofo.columns:
            if col.startswith('202') or 'date' in col.lower() or 'month' in col.lower():
                date_cols.append(col)
        
        if not date_cols:
            st.error("❌ Kolom tanggal tidak ditemukan di data ROFO")
            st.info(f"Kolom yang tersedia: {list(df_rofo.columns)}")
            return pd.DataFrame()

        # Prepare ID columns for melt
        id_cols = [sku_col]
        if 'Product Name' in df_rofo.columns:
            id_cols.append('Product Name')
        elif 'Description' in df_rofo.columns:
            id_cols.append('Description')

        # Melt ROFO data
        df_rofo_long = df_rofo.melt(
            id_vars=id_cols,
            value_vars=date_cols,
            var_name="Date",
            value_name="ROFO Quantity"
        ).rename(columns={sku_col: 'SKU'})

        # Clean and convert ROFO data
        df_rofo_long["Date"] = pd.to_datetime(df_rofo_long["Date"], errors="coerce").dt.to_period("M")
        df_rofo_long["ROFO Quantity"] = pd.to_numeric(
            df_rofo_long["ROFO Quantity"], errors="coerce"
        ).fillna(0).clip(lower=0)
        df_rofo_long = df_rofo_long.dropna(subset=["Date"])

        st.info("🔄 Memproses data PO...")
        
        # ---------- PO Processing ----------
        # Find relevant columns in PO data
        date_col_po = None
        qty_col_po = None  
        sku_col_po = None
        
        for col in df_po.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['date', 'delivery', 'tanggal']):
                date_col_po = col
            elif any(keyword in col_lower for keyword in ['qty', 'quantity', 'confirm', 'actual']):
                qty_col_po = col
            elif any(keyword in col_lower for keyword in ['material', 'sku', 'kode', 'item']):
                sku_col_po = col
        
        # Set defaults if not found
        if not date_col_po: date_col_po = df_po.columns[0]
        if not qty_col_po: qty_col_po = df_po.columns[1] if len(df_po.columns) > 1 else df_po.columns[0]
        if not sku_col_po: sku_col_po = df_po.columns[2] if len(df_po.columns) > 2 else df_po.columns[0]

        # Process PO data
        df_po_clean = df_po.copy()
        df_po_clean["Date"] = pd.to_datetime(df_po_clean[date_col_po], errors="coerce").dt.to_period("M")
        df_po_clean["Actual Quantity"] = pd.to_numeric(
            df_po_clean[qty_col_po], errors="coerce"
        ).fillna(0).clip(lower=0)

        df_po_long = df_po_clean.groupby([sku_col_po, "Date"])["Actual Quantity"].sum().reset_index()
        df_po_long = df_po_long.rename(columns={sku_col_po: "SKU"})

        st.info("🔄 Menggabungkan data...")
        
        # ---------- Merge Data ----------
        df_merged = pd.merge(
            df_rofo_long[["SKU", "Date", "ROFO Quantity"]],
            df_po_long[["SKU", "Date", "Actual Quantity"]],
            on=["SKU", "Date"],
            how="outer"
        ).fillna(0)

        # Filter only rows with data
        df_merged = df_merged[
            (df_merged["ROFO Quantity"] > 0) | 
            (df_merged["Actual Quantity"] > 0)
        ]
        df_merged["Date"] = df_merged["Date"].astype(str)

        # ---------- Calculate Metrics ----------
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

        st.success(f"✅ Data processing selesai: {len(df_merged)} baris data")
        return df_merged

    except Exception as e:
        st.error(f"❌ Error dalam proses data: {str(e)}")
        st.code(traceback.format_exc())
        return pd.DataFrame()

# -------------------------------------------------------------------
# 3. DASHBOARD UTAMA - FIXED VERSION
# -------------------------------------------------------------------

def create_dashboard(df, df_sales):
    """Create dashboard dengan data yang sudah diproses"""
    
    st.header("📊 KPI Utama Forecasting")

    # ---------- Filters ----------
    col_filter1, col_filter2 = st.columns([2, 1])
    
    with col_filter1:
        all_sku = ["All"] + sorted(df["SKU"].unique().tolist())
        selected_sku = st.selectbox("Filter by SKU:", all_sku, key="sku_filter")

    with col_filter2:
        date_range = sorted(df["Date"].unique().tolist())
        selected_date = st.selectbox("Filter by Date:", ["All"] + date_range, key="date_filter")

    # Apply filters
    df_filtered = df.copy()
    if selected_sku != "All":
        df_filtered = df_filtered[df_filtered["SKU"] == selected_sku]
    if selected_date != "All":
        df_filtered = df_filtered[df_filtered["Date"] == selected_date]

    if df_filtered.empty:
        st.warning("📭 Tidak ada data untuk filter yang dipilih")
        return

    # ---------- KPI Cards ----------
    total_rofo = df_filtered["ROFO Quantity"].sum()
    total_actual = df_filtered["Actual Quantity"].sum()
    total_bias = total_rofo - total_actual
    
    far_overall = total_actual / total_rofo if total_rofo > 0 else 0
    
    acc_count = (df_filtered["Accuracy Status"] == "Accurate").sum()
    total_count = len(df_filtered)
    accuracy_ratio = acc_count / total_count if total_count > 0 else 0

    # Display KPI cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total ROFO", 
            f"{total_rofo:,.0f}",
            help="Total Forecast Quantity"
        )
    
    with col2:
        st.metric(
            "Total Actual", 
            f"{total_actual:,.0f}",
            help="Total Actual PO Quantity"
        )
    
    with col3:
        st.metric(
            "Overall FAR", 
            f"{far_overall:.1%}",
            delta="Target: 80%-120%",
            delta_color="off",
            help="Forecast Accuracy Ratio"
        )
    
    with col4:
        st.metric(
            "Accuracy Rate", 
            f"{accuracy_ratio:.1%}",
            help=f"{acc_count} dari {total_count} data accurate"
        )

    col5, col6, col7 = st.columns(3)
    
    with col5:
        bias_color = "normal"
        if total_bias > 0:
            bias_color = "inverse"
        st.metric(
            "Total Bias",
            f"{total_bias:,.0f}",
            delta="Positif = Over forecast" if total_bias > 0 else "Negatif = Under forecast",
            delta_color=bias_color
        )
    
    with col6:
        over_forecast = len(df_filtered[df_filtered["Bias"] > 0])
        st.metric("Over Forecast", f"{over_forecast}")
    
    with col7:
        under_forecast = len(df_filtered[df_filtered["Bias"] < 0])
        st.metric("Under Forecast", f"{under_forecast}")

    st.markdown("---")

    # ---------- Charts ----------
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Trend", "🔥 Top Bias", "📊 Accuracy", "📋 Data"])

    with tab1:
        st.subheader("Tren ROFO vs Actual per Bulan")
        
        df_monthly = df_filtered.groupby("Date").agg({
            "ROFO Quantity": "sum",
            "Actual Quantity": "sum",
            "FAR": "mean"
        }).reset_index()

        df_trend = df_monthly.melt(
            id_vars=["Date"], 
            value_vars=["ROFO Quantity", "Actual Quantity"],
            var_name="Type", 
            value_name="Quantity"
        )

        chart_trend = alt.Chart(df_trend).mark_line(point=True).encode(
            x=alt.X("Date:O", title="Bulan"),
            y=alt.Y("Quantity:Q", title="Quantity", axis=alt.Axis(format="~s")),
            color=alt.Color("Type:N", scale=alt.Scale(
                domain=['ROFO Quantity', 'Actual Quantity'],
                range=['#1f77b4', '#ff7f0e']
            )),
            strokeDash=alt.StrokeDash("Type:N", scale=alt.Scale(
                domain=['ROFO Quantity', 'Actual Quantity'],
                range=[[1, 0], [5, 5]]
            )),
            tooltip=["Date", "Type", alt.Tooltip("Quantity:Q", format=",")]
        ).properties(height=400)
        
        st.altair_chart(chart_trend, use_container_width=True)

    with tab2:
        st.subheader("Top 10 SKU dengan Bias Tertinggi")
        
        df_bias = df_filtered.groupby("SKU").agg({
            "Bias": "sum",
            "ROFO Quantity": "sum", 
            "Actual Quantity": "sum"
        }).reset_index().nlargest(10, "Bias")

        chart_bias = alt.Chart(df_bias).mark_bar().encode(
            x=alt.X("Bias:Q", title="Bias", axis=alt.Axis(format="~s")),
            y=alt.Y("SKU:N", sort="-x", title="SKU"),
            color=alt.condition(
                alt.datum.Bias > 0,
                alt.value("#ff4b4b"),  # Red for over forecast
                alt.value("#4caf50")   # Green for under forecast
            ),
            tooltip=[
                "SKU", 
                alt.Tooltip("Bias:Q", format=",", title="Bias"),
                alt.Tooltip("ROFO Quantity:Q", format=",", title="ROFO"),
                alt.Tooltip("Actual Quantity:Q", format=",", title="Actual")
            ]
        ).properties(height=400)
        
        st.altair_chart(chart_bias, use_container_width=True)

    with tab3:
        st.subheader("Distribusi Akurasi")
        
        accuracy_dist = df_filtered["Accuracy Status"].value_counts().reset_index()
        accuracy_dist.columns = ["Status", "Count"]
        
        chart_pie = alt.Chart(accuracy_dist).mark_arc().encode(
            theta=alt.Theta("Count:Q", title="Count"),
            color=alt.Color("Status:N", scale=alt.Scale(
                domain=['Accurate', 'Non-Accurate'],
                range=['#4caf50', '#ff4b4b']
            )),
            tooltip=["Status", "Count"]
        ).properties(height=300)
        
        st.altair_chart(chart_pie, use_container_width=True)

    with tab4:
        st.subheader("Detail Data")
        
        # Format display data
        df_display = df_filtered.copy()
        df_display["FAR"] = df_display["FAR"].apply(lambda x: f"{x:.1%}")
        df_display["ROFO Quantity"] = df_display["ROFO Quantity"].apply(lambda x: f"{x:,.0f}")
        df_display["Actual Quantity"] = df_display["Actual Quantity"].apply(lambda x: f"{x:,.0f}")
        df_display["Bias"] = df_display["Bias"].apply(lambda x: f"{x:,.0f}")
        
        st.dataframe(df_display, use_container_width=True)
        
        # Download button
        csv = df_filtered.to_csv(index=False)
        st.download_button(
            label="📥 Download Data sebagai CSV",
            data=csv,
            file_name=f"forecast_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

    # ---------- Sales Data ----------
    if not df_sales.empty:
        st.markdown("---")
        st.header("📦 Data Sales")
        st.dataframe(df_sales.head(20), use_container_width=True)

# -------------------------------------------------------------------
# 4. MAIN APP - FIXED VERSION
# -------------------------------------------------------------------

def main():
    # Page configuration
    st.set_page_config(
        page_title="Forecast Accuracy Dashboard",
        layout="wide",
        page_icon="📊",
        initial_sidebar_state="expanded"
    )

    # Sidebar
    with st.sidebar:
        st.title("📊 Forecast Dashboard")
        st.markdown("---")
        
        st.subheader("Data Status")
        
        if st.button("🔄 Load/Refresh Data", type="primary"):
            st.session_state.data_loaded = False
            st.rerun()

    # Main content
    st.title("📈 Forecast Accuracy & Achievement Dashboard")
    st.markdown("Monitor dan analisis performa forecasting vs actual PO")

    # Load data section
    if not st.session_state.data_loaded:
        st.info("📥 Memuat data dari Google Sheets...")
        
        with st.spinner("Menghubungkan ke Google Sheets..."):
            df_rofo = load_data_from_gsheet(SPREADSHEET_URL_ROFO, SHEET_NAME)
            df_po = load_data_from_gsheet(SPREADSHEET_URL_PO, SHEET_NAME)  
            df_sales = load_data_from_gsheet(SPREADSHEET_URL_SALES, SHEET_NAME)

        # Display data status
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("ROFO Data", f"{len(df_rofo)} rows")
        with col2:
            st.metric("PO Data", f"{len(df_po)} rows")
        with col3:
            st.metric("Sales Data", f"{len(df_sales)} rows")

        if df_rofo.empty or df_po.empty:
            st.error("""
            ❌ Gagal memuat data utama. Periksa:
            - **Izin akses**: Pastikan Google Sheets sudah di-share ke service account
            - **URL**: Pastikan URL spreadsheet benar
            - **Secrets**: Pastikan konfigurasi secrets.toml sudah benar
            """)
            
            # Debug info
            with st.expander("🔧 Debug Information"):
                if not df_rofo.empty:
                    st.write("**ROFO Columns:**", list(df_rofo.columns))
                    st.write("**ROFO Sample:**")
                    st.dataframe(df_rofo.head(3))
                if not df_po.empty:
                    st.write("**PO Columns:**", list(df_po.columns))
                    st.write("**PO Sample:**")
                    st.dataframe(df_po.head(3))
            
            return

        # Process data
        with st.spinner("Memproses dan menganalisis data..."):
            df_processed = process_data(df_rofo, df_po, df_sales)

        if not df_processed.empty:
            st.session_state.df_processed = df_processed
            st.session_state.df_sales = df_sales
            st.session_state.data_loaded = True
            st.success("✅ Data berhasil dimuat dan diproses!")
            st.rerun()
        else:
            st.error("❌ Gagal memproses data. Periksa struktur data.")
            return
    else:
        # Display dashboard with cached data
        create_dashboard(st.session_state.df_processed, st.session_state.df_sales)

if __name__ == "__main__":
    main()
