import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt
from datetime import datetime
import re

# -------------------------------------------------------------------
# 1. KONFIGURASI & UTILS
# -------------------------------------------------------------------

st.set_page_config(page_title="Advanced Forecast Dashboard", layout="wide", page_icon="📈")

# Link Google Sheet Utama
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit"

# Nama Tab di Google Sheet (Sesuaikan jika beda)
SHEET_CONFIG = {
    'rofo': 'Rofo',
    'po': 'PO',
    'sales': 'Sales'
}

@st.cache_resource
def get_service_account():
    """Load Service Account dengan error handling yang lebih baik."""
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("❌ Secrets 'gcp_service_account' tidak ditemukan!")
            return None
        creds = st.secrets["gcp_service_account"]
        gc = gspread.service_account_from_dict(dict(creds))
        return gc
    except Exception as e:
        st.error(f"❌ Gagal koneksi ke Google Cloud: {e}")
        return None

@st.cache_data(ttl=600) # Cache 10 menit biar gak lemot
def load_gsheet_data(url, sheet_name):
    """Load data dari GSheet dengan retry logic sederhana."""
    gc = get_service_account()
    if not gc: return pd.DataFrame()
    
    try:
        sh = gc.open_by_url(url)
        ws = sh.worksheet(sheet_name)
        data = ws.get_all_values()
        if not data: return pd.DataFrame()
        
        # Ambil header dari baris pertama
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except gspread.WorksheetNotFound:
        st.warning(f"⚠️ Tab '{sheet_name}' tidak ditemukan di Google Sheet.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error loading {sheet_name}: {e}")
        return pd.DataFrame()

def extract_date_columns(columns):
    """
    Fungsi CANGGIH untuk mendeteksi kolom tanggal secara otomatis.
    Mendeteksi format YYYY-MM-DD, DD-MM-YYYY, atau format lain yang mengandung angka tahun.
    """
    date_cols = []
    for col in columns:
        # Cek apakah kolom mengandung pola tanggal (misal: 2024, 2025)
        if re.search(r'202\d', col): 
            date_cols.append(col)
    return date_cols

# -------------------------------------------------------------------
# 2. DATA PROCESSING ENGINE
# -------------------------------------------------------------------

@st.cache_data
def process_data(df_rofo, df_po, df_sales):
    # --- A. PROSES ROFO (FORECAST) ---
    # Cari kolom tanggal otomatis
    rofo_date_cols = extract_date_columns(df_rofo.columns)
    
    if not rofo_date_cols:
        st.error("❌ Tidak dapat menemukan kolom tanggal di data Rofo (cari kolom dengan tahun 202x).")
        return pd.DataFrame(), pd.DataFrame()

    # Identifikasi kolom ID (selain tanggal)
    rofo_id_cols = [c for c in df_rofo.columns if c not in rofo_date_cols]
    
    # Melt data (Wide to Long)
    df_rofo_long = df_rofo.melt(
        id_vars=rofo_id_cols, 
        value_vars=rofo_date_cols,
        var_name='Date_Raw', 
        value_name='ROFO_Qty'
    )
    
    # Cleaning & Formatting
    df_rofo_long['Month'] = pd.to_datetime(df_rofo_long['Date_Raw'], errors='coerce').dt.to_period('M')
    df_rofo_long['ROFO_Qty'] = pd.to_numeric(df_rofo_long['ROFO_Qty'], errors='coerce').fillna(0)
    
    # Pastikan ada kolom 'SKU SAP'
    sku_col = 'SKU SAP' if 'SKU SAP' in df_rofo.columns else df_rofo.columns[0]
    df_rofo_final = df_rofo_long.groupby([sku_col, 'Month'])['ROFO_Qty'].sum().reset_index().rename(columns={sku_col: 'SKU'})

    # --- B. PROSES PO (ACTUAL INBOUND) ---
    # Pastikan kolom tanggal dan qty ada
    po_date_col = 'Document Date' if 'Document Date' in df_po.columns else df_po.columns[0] # Fallback
    po_qty_col = 'Quantity' if 'Quantity' in df_po.columns else ('Order Quantity' if 'Order Quantity' in df_po.columns else df_po.columns[-1])
    po_sku_col = 'SKU SAP' if 'SKU SAP' in df_po.columns else 'Material'

    df_po['Month'] = pd.to_datetime(df_po[po_date_col], errors='coerce').dt.to_period('M')
    df_po['Actual_Qty'] = pd.to_numeric(df_po[po_qty_col], errors='coerce').fillna(0)
    
    df_po_final = df_po.groupby([po_sku_col, 'Month'])['Actual_Qty'].sum().reset_index().rename(columns={po_sku_col: 'SKU'})

    # --- C. PROSES SALES (SECONDARY METRIC) ---
    sales_date_cols = extract_date_columns(df_sales.columns)
    sales_id_cols = [c for c in df_sales.columns if c not in sales_date_cols]
    sales_sku_col = 'SKU SAP' if 'SKU SAP' in df_sales.columns else df_sales.columns[0]

    df_sales_long = df_sales.melt(id_vars=sales_id_cols, value_vars=sales_date_cols, var_name='Date_Raw', value_name='Sales_Qty')
    df_sales_long['Month'] = pd.to_datetime(df_sales_long['Date_Raw'], errors='coerce').dt.to_period('M')
    df_sales_long['Sales_Qty'] = pd.to_numeric(df_sales_long['Sales_Qty'], errors='coerce').fillna(0)
    
    df_sales_final = df_sales_long.groupby([sales_sku_col, 'Month'])['Sales_Qty'].sum().reset_index().rename(columns={sales_sku_col: 'SKU'})

    # --- D. MERGING (THE MAGIC HAPPENS HERE) ---
    # Full Outer Join Rofo & PO
    df_merged = pd.merge(df_rofo_final, df_po_final, on=['SKU', 'Month'], how='outer').fillna(0)
    
    # Left Join dengan Sales (Sales cuma pelengkap)
    df_merged = pd.merge(df_merged, df_sales_final, on=['SKU', 'Month'], how='left').fillna(0)

    # Filter data kosong (Rofo 0 AND Actual 0 AND Sales 0) - Buang sampah
    df_merged = df_merged[(df_merged['ROFO_Qty'] != 0) | (df_merged['Actual_Qty'] != 0) | (df_merged['Sales_Qty'] != 0)]
    
    # --- E. METRICS CALCULATION ---
    df_merged['Bias'] = df_merged['ROFO_Qty'] - df_merged['Actual_Qty']
    
    # Logic FAR (Forecast Achievement Ratio)
    def calc_far(row):
        if row['ROFO_Qty'] == 0:
            return 0.0 # Atau infinite, tapi 0 lebih aman buat chart
        return row['Actual_Qty'] / row['ROFO_Qty']
    
    df_merged['FAR'] = df_merged.apply(calc_far, axis=1)
    
    # Logic Accuracy Status (80% - 120%)
    df_merged['Status'] = np.where(
        (df_merged['FAR'] >= 0.8) & (df_merged['FAR'] <= 1.2), 'Accurate',
        np.where(df_merged['FAR'] < 0.8, 'Under-Delivery (Risk)', 'Over-Delivery (Overstock)')
    )
    # Handle kasus khusus
    df_merged.loc[(df_merged['ROFO_Qty'] > 0) & (df_merged['Actual_Qty'] == 0), 'Status'] = 'Missed (No Supply)'
    df_merged.loc[(df_merged['ROFO_Qty'] == 0) & (df_merged['Actual_Qty'] > 0), 'Status'] = 'Unforecasted Demand'

    # Convert Period to String for Altair
    df_merged['Month_Str'] = df_merged['Month'].astype(str)

    return df_merged

# -------------------------------------------------------------------
# 3. DASHBOARD UI
# -------------------------------------------------------------------

def main():
    st.title("🚀 Forecast Accuracy Dashboard (Gembel Premium Edition)")
    st.markdown("Dashboard ini membandingkan **Forecast (Rofo)** vs **Actual (PO)** vs **Sales Out**.")

    with st.spinner("Sedang menarik data terbaru dari Google Sheets..."):
        df_rofo = load_gsheet_data(SPREADSHEET_URL, SHEET_CONFIG['rofo'])
        df_po = load_gsheet_data(SPREADSHEET_URL, SHEET_CONFIG['po'])
        df_sales = load_gsheet_data(SPREADSHEET_URL, SHEET_CONFIG['sales'])

    if df_rofo.empty or df_po.empty:
        st.stop()

    # Process Data
    df_main = process_data(df_rofo, df_po, df_sales)

    # --- SIDEBAR FILTERS ---
    st.sidebar.header("🔍 Filter Data")
    
    # Filter SKU
    all_skus = ['All'] + sorted(df_main['SKU'].astype(str).unique().tolist())
    selected_sku = st.sidebar.selectbox("Pilih SKU:", all_skus)
    
    # Filter Bulan
    all_months = ['All'] + sorted(df_main['Month_Str'].unique().tolist())
    selected_month = st.sidebar.selectbox("Pilih Bulan:", all_months)

    # Filter Status
    all_statuses = ['All'] + sorted(df_main['Status'].unique().tolist())
    selected_status = st.sidebar.selectbox("Filter Status Akurasi:", all_statuses)

    # Apply Filters
    df_view = df_main.copy()
    if selected_sku != 'All':
        df_view = df_view[df_view['SKU'] == selected_sku]
    if selected_month != 'All':
        df_view = df_view[df_view['Month_Str'] == selected_month]
    if selected_status != 'All':
        df_view = df_view[df_view['Status'] == selected_status]

    if df_view.empty:
        st.warning("Data tidak ditemukan dengan filter tersebut.")
        st.stop()

    # --- KPI SECTION ---
    st.markdown("### 📊 Performa Global")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    total_rofo = df_view['ROFO_Qty'].sum()
    total_actual = df_view['Actual_Qty'].sum()
    total_sales = df_view['Sales_Qty'].sum()
    avg_far = (total_actual / total_rofo) if total_rofo > 0 else 0
    
    kpi1.metric("Total Forecast (Rofo)", f"{total_rofo:,.0f}")
    kpi2.metric("Total Actual (PO)", f"{total_actual:,.0f}", delta=f"{total_actual-total_rofo:,.0f} vs Rofo")
    kpi3.metric("Achievement (Actual vs Rofo)", f"{avg_far:.1%}", help="Target: 80% - 120%")
    kpi4.metric("Total Sales Out", f"{total_sales:,.0f}", delta=f"Ratio: {(total_sales/total_actual if total_actual>0 else 0):.1%} of PO")

    st.divider()

    # --- CHART SECTION ---
    tab_trend, tab_bias, tab_scatter = st.tabs(["📈 Monthly Trend", "⚖️ Top Bias Analysis", "🎯 Scatter Plot"])

    with tab_trend:
        st.subheader("Tren Forecast vs Actual vs Sales")
        # Prepare data for Altair
        chart_data = df_view.groupby('Month_Str')[['ROFO_Qty', 'Actual_Qty', 'Sales_Qty']].sum().reset_index()
        chart_data = chart_data.melt('Month_Str', var_name='Metric', value_name='Qty')
        
        # Custom Color Scheme
        colors = alt.Scale(domain=['ROFO_Qty', 'Actual_Qty', 'Sales_Qty'], range=['#3498db', '#2ecc71', '#e74c3c'])
        
        chart = alt.Chart(chart_data).mark_line(point=True).encode(
            x=alt.X('Month_Str', title='Bulan'),
            y=alt.Y('Qty', title='Quantity'),
            color=alt.Color('Metric', scale=colors, legend=alt.Legend(title="Metrik")),
            tooltip=['Month_Str', 'Metric', alt.Tooltip('Qty', format=',.0f')]
        ).properties(height=400).interactive()
        
        st.altair_chart(chart, use_container_width=True)

    with tab_bias:
        st.subheader("Top 10 SKU dengan Bias Tertinggi (Forecast Error)")
        # Hitung Absolute Bias untuk ranking
        bias_df = df_view.groupby('SKU')[['Bias', 'ROFO_Qty', 'Actual_Qty']].sum().reset_index()
        bias_df['Abs_Bias'] = bias_df['Bias'].abs()
        top_bias = bias_df.sort_values('Abs_Bias', ascending=False).head(10)
        
        bias_chart = alt.Chart(top_bias).mark_bar().encode(
            x=alt.X('Bias', title='Total Bias (Rofo - Actual)'),
            y=alt.Y('SKU', sort='-x'),
            color=alt.condition(alt.datum.Bias > 0, alt.value("#e74c3c"), alt.value("#27ae60")), # Merah jika Overforecast, Hijau jika Underforecast
            tooltip=['SKU', 'Bias', 'ROFO_Qty', 'Actual_Qty']
        ).properties(height=400)
        st.altair_chart(bias_chart, use_container_width=True)
        st.caption("*Merah = Forecast Ketinggian (Barang kurang), Hijau = Forecast Kerendahan (Barang kelebihan)*")

    with tab_scatter:
        st.subheader("Korelasi Forecast vs Actual")
        scatter = alt.Chart(df_view).mark_circle(size=60).encode(
            x=alt.X('ROFO_Qty', title='Forecast Qty'),
            y=alt.Y('Actual_Qty', title='Actual Qty'),
            color='Status',
            tooltip=['SKU', 'Month_Str', 'ROFO_Qty', 'Actual_Qty', 'Status']
        ).properties(height=400).interactive()
        
        line = alt.Chart(pd.DataFrame({'x': [0, df_view['ROFO_Qty'].max()], 'y': [0, df_view['ROFO_Qty'].max()]})).mark_rule(color='gray', strokeDash=[5,5]).encode(x='x', y='y')
        
        st.altair_chart(scatter + line, use_container_width=True)

    # --- DATA DETAIL ---
    with st.expander("📋 Lihat Detail Data (Tabel)"):
        st.dataframe(
            df_view[['SKU', 'Month_Str', 'ROFO_Qty', 'Actual_Qty', 'Sales_Qty', 'FAR', 'Bias', 'Status']]
            .sort_values(['Month_Str', 'SKU'])
            .style.format({
                'ROFO_Qty': '{:,.0f}', 
                'Actual_Qty': '{:,.0f}', 
                'Sales_Qty': '{:,.0f}',
                'FAR': '{:.1%}',
                'Bias': '{:,.0f}'
            }), 
            use_container_width=True
        )

if __name__ == "__main__":
    main()
