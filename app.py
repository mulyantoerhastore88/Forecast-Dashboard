# File: app.py

import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt

# --- 1. SETUP KONEKSI GOOGLE SHEETS & URLs ---

# URL DARI FILE GOOGLE SHEETS ANDA (SUDAH DIISI DENGAN URL YANG ANDA BERIKAN)
SPREADSHEET_URL_ROFO = "https://docs.google.com/spreadsheets/d/1dcVqGq6wjOtimpw_IDq_BKCxWhyO_z9W5pNBR3FO43Y/edit?usp=sharing" 
SPREADSHEET_URL_PO = "https://docs.google.com/spreadsheets/d/17sBiMYXomOSj5SnLwUIoUkVpLTeB4IxNJhUWyxh5DlE/edit?usp=sharing" 
SPREADSHEET_URL_SALES = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit?usp=sharing" 

# Nama Sheet sudah dikonfirmasi, yaitu 'Sheet1' untuk ketiga file
SHEET_NAME = "Sheet1" 

@st.cache_data(ttl=3600) 
def get_service_account():
    """Mengambil kredensial dari st.secrets dan menginisiasi gspread."""
    # Pastikan kredensial sudah diatur di Streamlit Cloud Secrets!
    creds = st.secrets["gcp_service_account"]
    gc = gspread.service_account_from_dict(dict(creds))
    return gc

@st.cache_data(ttl=3600)
def load_data_from_gsheet(spreadsheet_url: str, sheet_name: str):
    """Fungsi untuk memuat data dari Sheet tertentu di Google Sheets."""
    gc = get_service_account()
    try:
        sh = gc.open_by_url(spreadsheet_url)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except Exception as e:
        st.error(f"Error memuat Sheet dari URL: {spreadsheet_url} (Sheet: {sheet_name}). Error: {e}. Cek izin berbagi.")
        return pd.DataFrame()

# --- 2. TRANSFORMATION & CALCULATION ---

@st.cache_data
def process_data(df_rofo, df_po, df_sales):
    
    # --- 2.1. Transformasi ROFO (Wide ke Long) ---
    rofo_id_cols = ['SKU GOA', 'Product Name']
    rofo_date_cols = [col for col in df_rofo.columns if col.startswith('202')]
    df_rofo_long = df_rofo.melt(
        id_vars=rofo_id_cols, 
        value_vars=rofo_date_cols, 
        var_name='Date', 
        value_name='ROFO Quantity'
    ).rename(columns={'SKU GOA': 'SKU'})
    
    # Cleaning ROFO
    df_rofo_long['Date'] = pd.to_datetime(df_rofo_long['Date'], errors='coerce').dt.to_period('M')
    df_rofo_long['ROFO Quantity'] = pd.to_numeric(df_rofo_long['ROFO Quantity'], errors='coerce').fillna(0).clip(lower=0)
    df_rofo_long.dropna(subset=['Date'], inplace=True)

    # --- 2.2. Transformasi PO (Transactional ke Long/Bulanan) ---
    # PO: Menggunakan 'Material' sebagai SKU, 'Confirm Quantity' sebagai kuantitas
    df_po['Date'] = pd.to_datetime(df_po['Delivery Date'], errors='coerce').dt.to_period('M')
    df_po['Actual Quantity'] = pd.to_numeric(df_po['Confirm Quantity'], errors='coerce').fillna(0).clip(lower=0)
    
    # Agregasi PO ke bulanan
    df_po_long = df_po.groupby(['Material', 'Date'])['Actual Quantity'].sum().reset_index()
    df_po_long = df_po_long.rename(columns={'Material': 'SKU'})

    # --- 2.3. Merging ROFO (Forecast) vs PO (Actual) ---
    df_merged = pd.merge(
        df_rofo_long[['SKU', 'Date', 'ROFO Quantity']],
        df_po_long[['SKU', 'Date', 'Actual Quantity']],
        on=['SKU', 'Date'],
        how='outer'
    ).fillna(0)
    
    # Hapus baris yang ROFO dan Actual-nya sama-sama 0
    df_merged = df_merged[(df_merged['ROFO Quantity'] > 0) | (df_merged['Actual Quantity'] > 0)]
    
    # --- 2.4. Custom Metric Calculation (FAR & Accuracy Status) ---
    
    def calculate_far(row):
        rofo = row['ROFO Quantity']
        actual = row['Actual Quantity']
        if rofo == 0:
            return 10.0 if actual > 0 else 1.0 
        return actual / rofo

    df_merged['FAR'] = df_merged.apply(calculate_far, axis=1)
    
    # Logika Akurasi Kustom 80% - 120%
    df_merged['Accuracy Status'] = np.where(
        (df_merged['FAR'] >= 0.8) & (df_merged['FAR'] <= 1.2),
        'Accurate',
        'Non-Accurate'
    )
    
    df_merged['Bias'] = df_merged['ROFO Quantity'] - df_merged['Actual Quantity']
    df_merged['Date'] = df_merged['Date'].astype(str) 
    
    return df_merged

# --- 3. VISUALISASI UTAMA (Fungsi tetap sama seperti sebelumnya) ---

def create_dashboard(df):
    # ... (Isi fungsi create_dashboard sama persis seperti kode sebelumnya) ...
    # Masukkan kode create_dashboard Anda di sini
    
    st.header("Metrik Kinerja Utama (KPI)")
    
    # 3.1. Filter Interaktif
    all_sku = ['All'] + sorted(df['SKU'].unique().tolist())
    selected_sku = st.sidebar.selectbox("Filter SKU:", all_sku)

    if selected_sku != 'All':
        df_filtered = df[df['SKU'] == selected_sku]
    else:
        df_filtered = df
    
    if df_filtered.empty:
        st.warning("Data kosong setelah filter SKU.")
        return

    # 3.2. Perhitungan KPI Global
    total_rofo = df_filtered['ROFO Quantity'].sum()
    total_actual = df_filtered['Actual Quantity'].sum()
    total_bias = total_rofo - total_actual 

    overall_far = (total_actual / total_rofo) if total_rofo > 0 else 0
    
    total_accurate_count = (df_filtered['Accuracy Status'] == 'Accurate').sum()
    total_count = len(df_filtered)
    accuracy_by_count = (total_accurate_count / total_count) if total_count > 0 else 0

    col1, col2, col3 = st.columns(3)
    
    col1.metric(
        label="Overall Forecast Achievement Ratio (FAR)", 
        value=f"{overall_far:.1%}", 
        delta=f"Range 80%-120%", 
        delta_color="off" 
    )
    col2.metric(
        label="Accuracy by Count (SKU-Month)", 
        value=f"{accuracy_by_count:.1%}", 
        delta=f"{total_accurate_count} of {total_count}",
        delta_color="off"
    )
    col3.metric(
        label="Total Bias (ROFO - Actual)", 
        value=f"{total_bias:,.0f}", 
        delta="Positif: Over-forecast, Negatif: Under-forecast", 
        delta_color="off"
    )

    st.markdown("---")
    
    # 3.3. Chart Tren ROFO vs. Actual
    st.subheader("Tren ROFO (Forecast) vs. PO Submitted (Actual) per Bulan")
    
    df_monthly = df_filtered.groupby('Date').agg(
        ROFO_Qty=('ROFO Quantity', 'sum'),
        Actual_Qty=('Actual Quantity', 'sum')
    ).reset_index().rename(columns={'ROFO_Qty': 'ROFO', 'Actual_Qty': 'PO Submitted'})
    
    df_monthly_chart = df_monthly.melt('Date', var_name='Type', value_name='Quantity')

    chart_trend = alt.Chart(df_monthly_chart).mark_line().encode(
        x=alt.X('Date:O', title='Bulan'),
        y=alt.Y('Quantity:Q', title='Kuantitas (Unit)'),
        color='Type:N',
        tooltip=['Date', 'Type', alt.Tooltip('Quantity:Q', format=',.0f')]
    ).properties(
        height=400
    ).interactive()
    
    st.altair_chart(chart_trend, use_container_width=True)

    # 3.4. Chart Akurasi per SKU (Top 10 Bias)
    st.subheader("Top 10 SKU Berdasarkan Total Bias (ROFO - PO)")
    
    df_sku_bias = df_filtered.groupby('SKU').agg(
        Total_Bias=('Bias', 'sum')
    ).reset_index().sort_values(by='Total_Bias', ascending=False).head(10)

    chart_bias = alt.Chart(df_sku_bias).mark_bar().encode(
        x=alt.X('Total_Bias:Q', title='Total Bias (ROFO - PO)'),
        y=alt.Y('SKU:N', sort='-x', title='SKU'),
        color=alt.condition(
            alt.datum.Total_Bias > 0,
            alt.value("red"),  # Over-forecast (ROFO > PO)
            alt.value("green") # Under-forecast (ROFO < PO)
        ),
        tooltip=['SKU', alt.Tooltip('Total_Bias:Q', format=',.0f')]
    ).properties(
        height=300
    )
    st.altair_chart(chart_bias, use_container_width=True)
    
    st.subheader("Detail Data Forecast Achievement Ratio (FAR)")
    st.dataframe(df_filtered.head(100))
    
    # Data Sales (hanya ditampilkan, tidak dihitung)
    st.header("Data Sales (End Customer Performance)")
    st.dataframe(df_sales.head())
    # ... (End of create_dashboard) ...

# --- 4. MAIN APP EXECUTION ---
def main():
    st.set_page_config(
        page_title="Forecast Accuracy Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.sidebar.title("Forecast Dashboard")
    st.sidebar.markdown("Filter dan Pilihan Utama")

    st.title("Forecast Accuracy & Achievement Dashboard")
    st.markdown("Dashboard ini membandingkan **ROFO** dengan **PO Submitted (Actual)**. Akurasi dihitung berdasarkan rasio 80% - 120%.")

    # 4.1. Load Data
    with st.spinner('Memuat dan memproses data dari Google Sheets...'):
        df_rofo = load_data_from_gsheet(SPREADSHEET_URL_ROFO, SHEET_NAME)
        df_po = load_data_from_gsheet(SPREADSHEET_URL_PO, SHEET_NAME)
        df_sales = load_data_from_gsheet(SPREADSHEET_URL_SALES, SHEET_NAME) # Hanya untuk display

    # 4.2. Run Processing
    if not df_rofo.empty and not df_po.empty:
        df_processed = process_data(df_rofo, df_po, df_sales)
        create_dashboard(df_processed)
    else:
        st.warning("Tidak dapat menampilkan dashboard karena gagal memuat data ROFO atau PO. Periksa pesan error di atas.")

if __name__ == "__main__":
    main()
