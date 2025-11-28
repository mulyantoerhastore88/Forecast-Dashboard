import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Dashboard Forecast Accuracy", layout="wide")

st.title("📊 Dashboard Forecast Accuracy & PO Absorption")
st.markdown("Upload data **Rofo (Forecast)**, **Sales**, dan **PO** untuk melihat analisa otomatis.")

# --- FUNGSI CLEANING DATA ---
def clean_currency(x):
    """Membersihkan format angka (hapus koma, ubah strip jadi 0)"""
    if isinstance(x, str):
        x = x.replace(',', '').replace('-', '0').strip()
        if x == '': return 0
    return pd.to_numeric(x, errors='coerce')

def process_file(uploaded_file, value_name):
    """Membaca file, melt tanggal, dan cleaning"""
    if uploaded_file is None:
        return None
    
    df = pd.read_csv(uploaded_file)
    
    # Identifikasi kolom tanggal (asumsi kolom yang mengandung '202')
    date_cols = [col for col in df.columns if '202' in col or '202' in str(col)]
    id_vars = [col for col in df.columns if col not in date_cols]
    
    # Unpivot / Melt
    df_melted = df.melt(id_vars=id_vars, value_vars=date_cols, var_name='Date_Raw', value_name=value_name)
    
    # Cleaning Value
    df_melted[value_name] = df_melted[value_name].apply(clean_currency).fillna(0)
    
    # Standardisasi Tanggal ke Awal Bulan (biar bisa di-join)
    # Contoh: 2025-01-25 -> 2025-01-01
    df_melted['Date'] = pd.to_datetime(df_melted['Date_Raw'], errors='coerce').dt.to_period('M').dt.to_timestamp()
    
    # Pastikan ada kolom SKU SAP (Standardisasi nama kolom)
    # Kita cari kolom yang mirip 'SKU' dan 'SAP'
    sku_col = [c for c in df_melted.columns if 'SKU' in c and 'SAP' in c]
    if sku_col:
        df_melted = df_melted.rename(columns={sku_col[0]: 'SKU SAP'})
    else:
        # Fallback: cari kolom yang cuma 'SKU' atau kolom pertama
        sku_col_fallback = [c for c in df_melted.columns if 'SKU' in c]
        if sku_col_fallback:
             df_melted = df_melted.rename(columns={sku_col_fallback[0]: 'SKU SAP'})

    # Khusus PO, kadang SKU ada prefix "FG-", kita bersihkan biar match
    if value_name == 'PO_Qty':
        df_melted['SKU SAP'] = df_melted['SKU SAP'].astype(str).str.replace('FG-', '')

    return df_melted[['SKU SAP', 'Date', value_name]]

# --- SIDEBAR: UPLOAD DATA ---
with st.sidebar:
    st.header("📂 Upload Data")
    file_rofo = st.file_uploader("Upload Rofo (Forecast)", type=['csv', 'xlsx'])
    file_sales = st.file_uploader("Upload Sales (Actual)", type=['csv', 'xlsx'])
    file_po = st.file_uploader("Upload PO", type=['csv', 'xlsx'])

# --- LOGIKA UTAMA ---
if file_rofo and file_sales and file_po:
    
    # 1. Load Data
    df_rofo = process_file(file_rofo, 'Forecast_Qty')
    df_sales = process_file(file_sales, 'Sales_Qty')
    df_po = process_file(file_po, 'PO_Qty')

    # Ambil info Brand/Product dari file Rofo untuk filter (opsional tapi berguna)
    # Kita baca ulang file rofo mentah untuk ambil master datanya
    file_rofo.seek(0)
    master_rofo = pd.read_csv(file_rofo)
    # Cari kolom Brand
    brand_col = [c for c in master_rofo.columns if 'Brand' in c][0]
    product_col = [c for c in master_rofo.columns if 'Product' in c][0]
    sku_col_master = [c for c in master_rofo.columns if 'SKU' in c and 'SAP' in c][0]
    
    master_sku = master_rofo[[sku_col_master, brand_col, product_col]].rename(columns={sku_col_master: 'SKU SAP', brand_col: 'Brand', product_col: 'Product Name'})
    master_sku['SKU SAP'] = master_sku['SKU SAP'].astype(str)

    # 2. Merging Data
    # Merge Forecast & Sales
    df_merge = pd.merge(df_rofo, df_sales, on=['SKU SAP', 'Date'], how='outer').fillna(0)
    # Merge dengan PO
    df_merge = pd.merge(df_merge, df_po, on=['SKU SAP', 'Date'], how='outer').fillna(0)
    # Merge dengan Master Brand
    df_merge['SKU SAP'] = df_merge['SKU SAP'].astype(str)
    df_final = pd.merge(df_merge, master_sku, on='SKU SAP', how='left')
    
    # Isi Brand kosong dengan 'Unknown'
    df_final['Brand'] = df_final['Brand'].fillna('Unknown')

    # --- FILTER SIDEBAR TAMBAHAN ---
    st.sidebar.divider()
    selected_brand = st.sidebar.multiselect("Filter Brand", options=df_final['Brand'].unique(), default=df_final['Brand'].unique())
    
    # Filter Dataframe
    df_filtered = df_final[df_final['Brand'].isin(selected_brand)]

    # --- HITUNG METRICS ---
    # Metric 1: Forecast Accuracy Status
    def get_status(row):
        fc = row['Forecast_Qty']
        act = row['Sales_Qty']
        if act == 0:
            return "Accurate" if fc == 0 else "Over Forecast"
        
        acc = fc / act
        if 0.8 <= acc <= 1.2:
            return "Accurate"
        elif acc < 0.8:
            return "Under Forecast"
        else:
            return "Over Forecast"

    df_filtered['Status_Accuracy'] = df_filtered.apply(get_status, axis=1)
    
    # Metric 2: Absorption (PO / Forecast)
    # Hati-hati pembagian nol
    df_filtered['Absorption_Pct'] = df_filtered.apply(lambda x: (x['PO_Qty'] / x['Forecast_Qty'] * 100) if x['Forecast_Qty'] > 0 else 0, axis=1)

    # --- DASHBOARD TABS ---
    tab1, tab2, tab3 = st.tabs(["🎯 Forecast Accuracy (Sales)", "📦 PO Absorption", "📄 Raw Data"])

    # === TAB 1: FORECAST VS SALES ===
    with tab1:
        st.subheader("Forecast vs Actual Sales Analysis")
        
        # KPI Cards
        col1, col2, col3 = st.columns(3)
        total_fc = df_filtered['Forecast_Qty'].sum()
        total_sales = df_filtered['Sales_Qty'].sum()
        avg_acc_rate = (total_fc / total_sales * 100) if total_sales > 0 else 0
        
        col1.metric("Total Forecast Qty", f"{total_fc:,.0f}")
        col2.metric("Total Sales Qty", f"{total_sales:,.0f}")
        col3.metric("Global Accuracy Rate", f"{avg_acc_rate:.1f}%")
        
        # Chart 1: Monthly Trend
        st.markdown("### Monthly Trend")
        monthly_grp = df_filtered.groupby('Date')[['Forecast_Qty', 'Sales_Qty']].sum().reset_index()
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=monthly_grp['Date'], y=monthly_grp['Forecast_Qty'], mode='lines+markers', name='Forecast'))
        fig_trend.add_trace(go.Scatter(x=monthly_grp['Date'], y=monthly_grp['Sales_Qty'], mode='lines+markers', name='Actual Sales', line=dict(dash='dot')))
        st.plotly_chart(fig_trend, use_container_width=True)
        
        # Chart 2: Accuracy Distribution
        st.markdown("### Accuracy Status Distribution")
        status_counts = df_filtered['Status_Accuracy'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        
        color_map = {'Accurate': '#2ca02c', 'Over Forecast': '#d62728', 'Under Forecast': '#ff7f0e'}
        fig_pie = px.pie(status_counts, values='Count', names='Status', color='Status', color_discrete_map=color_map)
        st.plotly_chart(fig_pie)

    # === TAB 2: PO ABSORPTION ===
    with tab2:
        st.subheader("Rofo vs PO (Absorption) Analysis")
        
        # Chart: Forecast vs PO by Month
        st.markdown("### Forecast vs PO Qty")
        po_grp = df_filtered.groupby('Date')[['Forecast_Qty', 'PO_Qty']].sum().reset_index()
        
        fig_po = go.Figure()
        fig_po.add_trace(go.Bar(x=po_grp['Date'], y=po_grp['Forecast_Qty'], name='Forecast (Plan)'))
        fig_po.add_trace(go.Bar(x=po_grp['Date'], y=po_grp['PO_Qty'], name='PO (Ordered)'))
        st.plotly_chart(fig_po, use_container_width=True)
        
        # Table: Low Absorption Alert
        st.markdown("### ⚠️ Low Absorption Alert (PO < 50% of Forecast)")
        low_abs = df_filtered[(df_filtered['Absorption_Pct'] < 50) & (df_filtered['Forecast_Qty'] > 100)] # Filter noise
        st.dataframe(low_abs[['Date', 'Brand', 'Product Name', 'Forecast_Qty', 'PO_Qty', 'Absorption_Pct']].sort_values('Absorption_Pct'))

    # === TAB 3: RAW DATA ===
    with tab3:
        st.markdown("### Data Detail")
        st.dataframe(df_filtered)
        
        # Download Button
        csv = df_filtered.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Processed Data as CSV",
            data=csv,
            file_name='processed_forecast_dashboard.csv',
            mime='text/csv',
        )

else:
    st.info("👋 Silakan upload file Rofo, Sales, dan PO di sidebar sebelah kiri untuk memulai.")
