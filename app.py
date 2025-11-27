# File: app.py

import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt

# --- 1. SETUP KONEKSI GOOGLE SHEETS ---

# Ganti ini dengan URL Google Sheets Anda
SPREADSHEET_URL = "GANTI DENGAN URL GOOGLE SHEETS ANDA" 

# Nama Sheet dari file CSV yang Anda berikan
SHEET_ROFO = "Data-Forecast(Rofo)" 
SHEET_SALES = "Data-Sales" 

@st.cache_data(ttl=3600) # Data di-cache selama 1 jam
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
        st.error(f"Error memuat Sheet '{sheet_name}': {e}. Pastikan URL Sheet dan nama Sheet benar, dan Service Account sudah diberi akses Viewer.")
        return pd.DataFrame()

# --- 2. TRANSFORMATION & CALCULATION ---

@st.cache_data
def process_data(df_rofo, df_sales):
    """
    Mengubah data dari Wide ke Long, membersihkan, menggabungkan, 
    dan menghitung metrik akurasi kustom.
    """
    
    # Identifikasi Kolom (Asumsi berdasarkan file CSV)
    rofo_id_cols = ['SKU GOA', 'Product Name']
    rofo_date_cols = [col for col in df_rofo.columns if col.startswith('202')]
    sales_id_cols = ['Current SKU', 'SKU Name']
    sales_date_cols = [col for col in df_sales.columns if col.startswith('202')]
    
    # 2.1. Melt ROFO (Wide ke Long)
    df_rofo_long = df_rofo.melt(
        id_vars=rofo_id_cols, 
        value_vars=rofo_date_cols, 
        var_name='Date', 
        value_name='ROFO Quantity'
    ).rename(columns={'SKU GOA': 'SKU'})
    
    # 2.2. Melt Sales/Actual (Wide ke Long)
    df_sales_long = df_sales.melt(
        id_vars=sales_id_cols, 
        value_vars=sales_date_cols, 
        var_name='Date', 
        value_name='Actual Quantity'
    ).rename(columns={'Current SKU': 'SKU'})
    
    # 2.3. Cleaning
    for df in [df_rofo_long, df_sales_long]:
        # Konversi Date (misalnya '2024-01-01') ke format bulan/tahun
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.to_period('M')
        # Konversi Kuantitas ke numerik, isi error/NaN dengan 0
        df['ROFO Quantity'] = pd.to_numeric(df['ROFO Quantity'], errors='coerce').fillna(0).clip(lower=0)
        df['Actual Quantity'] = pd.to_numeric(df['Actual Quantity'], errors='coerce').fillna(0).clip(lower=0)
        df.dropna(subset=['Date'], inplace=True)

    # 2.4. Merging
    df_merged = pd.merge(
        df_rofo_long[['SKU', 'Date', 'ROFO Quantity']],
        df_sales_long[['SKU', 'Date', 'Actual Quantity']],
        on=['SKU', 'Date'],
        how='outer'
    ).fillna(0)
    
    # Hapus baris yang ROFO dan Actual-nya sama-sama 0
    df_merged = df_merged[(df_merged['ROFO Quantity'] > 0) | (df_merged['Actual Quantity'] > 0)]
    
    # 2.5. Custom Metric Calculation (FAR & Accuracy Status)
    
    # Fungsi untuk menghitung FAR, menghindari pembagian dengan nol
    def calculate_far(row):
        rofo = row['ROFO Quantity']
        actual = row['Actual Quantity']
        
        if rofo == 0:
            # Jika ROFO 0 dan Aktual > 0, set ke nilai tinggi untuk status Non-Accurate (> 1.2)
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
    df_merged['Date'] = df_merged['Date'].astype(str) # Altair lebih suka string untuk periode
    
    return df_merged

# --- 3. VISUALISASI UTAMA ---

def create_dashboard(df):
    """Membuat visualisasi dan KPI utama."""
    
    st.header("Metrik Kinerja Utama (KPI)")
    
    # 3.1. Filter Interaktif
    all_sku = ['All'] + sorted(df['SKU'].unique().tolist())
    selected_sku = st.sidebar.selectbox("Filter SKU:", all_sku)

    if selected_sku != 'All':
        df_filtered = df[df['SKU'] == selected_sku]
    else:
        df_filtered = df
    
    # Pastikan data tidak kosong setelah filter
    if df_filtered.empty:
        st.warning("Data kosong setelah filter SKU.")
        return

    # 3.2. Perhitungan KPI Global
    total_rofo = df_filtered['ROFO Quantity'].sum()
    total_actual = df_filtered['Actual Quantity'].sum()
    total_bias = total_rofo - total_actual # Bias = Over/Under-forecast

    # Overall FAR (menghindari pembagian dengan nol)
    overall_far = (total_actual / total_rofo) if total_rofo > 0 else 0
    
    # % Accurate (berdasarkan jumlah SKU/Bulan yang akurat)
    total_accurate_count = (df_filtered['Accuracy Status'] == 'Accurate').sum()
    total_count = len(df_filtered)
    accuracy_by_count = (total_accurate_count / total_count) if total_count > 0 else 0

    col1, col2, col3 = st.columns(3)
    
    col1.metric(
        label="Overall Forecast Achievement Ratio (FAR)", 
        value=f"{overall_far:.1%}", 
        delta=f"Range 80%-120%", 
        delta_color="off" # Tidak perlu delta trend
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
    st.subheader("Tren ROFO vs. Actual per Bulan")
    
    # Agregasi Bulanan
    df_monthly = df_filtered.groupby('Date').agg(
        ROFO_Qty=('ROFO Quantity', 'sum'),
        Actual_Qty=('Actual Quantity', 'sum')
    ).reset_index().rename(columns={'ROFO_Qty': 'ROFO', 'Actual_Qty': 'Actual'})
    
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
    st.subheader("Top 10 SKU Berdasarkan Total Bias (ROFO - Actual)")
    
    df_sku_bias = df_filtered.groupby('SKU').agg(
        Total_Bias=('Bias', 'sum')
    ).reset_index().sort_values(by='Total_Bias', ascending=False).head(10)

    chart_bias = alt.Chart(df_sku_bias).mark_bar().encode(
        x=alt.X('Total_Bias:Q', title='Total Bias (ROFO - Actual)'),
        y=alt.Y('SKU:N', sort='-x', title='SKU'),
        color=alt.condition(
            alt.datum.Total_Bias > 0,
            alt.value("red"),  # Over-forecast (ROFO > Actual)
            alt.value("green") # Under-forecast (ROFO < Actual)
        ),
        tooltip=['SKU', alt.Tooltip('Total_Bias:Q', format=',.0f')]
    ).properties(
        height=300
    )
    st.altair_chart(chart_bias, use_container_width=True)
    
    # 3.5. Detail Data
    st.subheader("Detail Data Forecast Achievement Ratio (FAR)")
    st.write("Data ini adalah gabungan ROFO dan Actual, sudah di-pivot ke format bulanan.")
    st.dataframe(df_filtered.head(100))


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
    st.markdown("Dashboard ini membandingkan ROFO dengan Actual Quantity (Sales/PO) dan mengukur performa Akurasi kustom Anda.")

    # 4.1. Load Data
    with st.spinner('Memuat dan memproses data dari Google Sheets...'):
        df_rofo = load_data_from_gsheet(SPREADSHEET_URL, SHEET_ROFO)
        df_sales = load_data_from_gsheet(SPREADSHEET_URL, SHEET_SALES)

    # 4.2. Run Processing
    if not df_rofo.empty and not df_sales.empty:
        df_processed = process_data(df_rofo, df_sales)
        create_dashboard(df_processed)
    else:
        st.warning("Tidak dapat menampilkan dashboard karena gagal memuat data. Periksa pesan error di atas.")

if __name__ == "__main__":
    main()
