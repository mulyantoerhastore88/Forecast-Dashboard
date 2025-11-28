import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Dashboard Forecast Accuracy", layout="wide")

st.title("📊 Dashboard Forecast Accuracy & PO Absorption")
st.markdown("Upload **1 File Excel** yang berisi sheet Rofo, Sales, dan PO.")

# --- FUNGSI CLEANING DATA ---
def clean_currency(x):
    """Membersihkan format angka (hapus koma, ubah strip jadi 0)"""
    if isinstance(x, str):
        x = x.replace(',', '').replace('-', '0').strip()
        if x == '': return 0
    return pd.to_numeric(x, errors='coerce')

def process_dataframe(df, value_name):
    """Memproses dataframe yang sudah di-load dari sheet"""
    # Identifikasi kolom tanggal (asumsi kolom yang mengandung '202' atau format tanggal)
    # Kita cari kolom yang bukan metadata (biasanya metadata itu string/object di awal)
    # Cara paling aman: cari kolom yang namanya mengandung tahun '202'
    date_cols = [col for col in df.columns if '202' in str(col)]
    
    # Jika tidak ketemu '202', coba deteksi datetime objects (kalau excel kadang auto convert)
    if not date_cols:
        # Fallback: ambil semua kolom kecuali beberapa kolom awal yang teks
        # Ini asumsi kasar, lebih aman pakai nama kolom tahun
        pass 

    id_vars = [col for col in df.columns if col not in date_cols]
    
    # Unpivot / Melt
    df_melted = df.melt(id_vars=id_vars, value_vars=date_cols, var_name='Date_Raw', value_name=value_name)
    
    # Cleaning Value
    df_melted[value_name] = df_melted[value_name].apply(clean_currency).fillna(0)
    
    # Standardisasi Tanggal ke Awal Bulan
    df_melted['Date'] = pd.to_datetime(df_melted['Date_Raw'], errors='coerce').dt.to_period('M').dt.to_timestamp()
    
    # Standardisasi nama kolom SKU
    sku_col = [c for c in df_melted.columns if 'SKU' in str(c) and 'SAP' in str(c)]
    if sku_col:
        df_melted = df_melted.rename(columns={sku_col[0]: 'SKU SAP'})
    else:
        # Fallback
        sku_col_fallback = [c for c in df_melted.columns if 'SKU' in str(c)]
        if sku_col_fallback:
             df_melted = df_melted.rename(columns={sku_col_fallback[0]: 'SKU SAP'})

    # Khusus PO, bersihkan prefix FG-
    if value_name == 'PO_Qty' and 'SKU SAP' in df_melted.columns:
        df_melted['SKU SAP'] = df_melted['SKU SAP'].astype(str).str.replace('FG-', '')

    return df_melted[['SKU SAP', 'Date', value_name]]

# --- SIDEBAR: UPLOAD DATA ---
with st.sidebar:
    st.header("📂 Upload Data")
    uploaded_file = st.file_uploader("Upload File Excel (.xlsx)", type=['xlsx'])

# --- LOGIKA UTAMA ---
if uploaded_file:
    # 1. Baca Nama Sheet
    xls = pd.ExcelFile(uploaded_file)
    sheet_names = xls.sheet_names
    
    st.success(f"File berhasil dibaca! Ditemukan {len(sheet_names)} sheet.")

    # 2. Mapping Sheet (User Pilih Mana Sheet yg Sesuai)
    with st.expander("⚙️ Konfigurasi Sheet", expanded=True):
        col_s1, col_s2, col_s3 = st.columns(3)
        
        # Coba auto-detect index berdasarkan nama
        idx_rofo = next((i for i, s in enumerate(sheet_names) if 'rofo' in s.lower() or 'forecast' in s.lower()), 0)
        idx_sales = next((i for i, s in enumerate(sheet_names) if 'sales' in s.lower()), 1 if len(sheet_names)>1 else 0)
        idx_po = next((i for i, s in enumerate(sheet_names) if 'po' in s.lower()), 2 if len(sheet_names)>2 else 0)

        sheet_rofo = col_s1.selectbox("Pilih Sheet Forecast/Rofo:", sheet_names, index=idx_rofo)
        sheet_sales = col_s2.selectbox("Pilih Sheet Sales:", sheet_names, index=idx_sales)
        sheet_po = col_s3.selectbox("Pilih Sheet PO:", sheet_names, index=idx_po)

    # Tombol Proses
    if st.button("🚀 Proses Dashboard"):
        
        with st.spinner('Sedang memproses data...'):
            # Load Data berdasarkan sheet yg dipilih
            raw_rofo = pd.read_excel(uploaded_file, sheet_name=sheet_rofo)
            raw_sales = pd.read_excel(uploaded_file, sheet_name=sheet_sales)
            raw_po = pd.read_excel(uploaded_file, sheet_name=sheet_po)

            # Process Data
            df_rofo = process_dataframe(raw_rofo, 'Forecast_Qty')
            df_sales = process_dataframe(raw_sales, 'Sales_Qty')
            df_po = process_dataframe(raw_po, 'PO_Qty')

            # Ambil Master Data (Brand/Product) dari Rofo
            # Asumsi kolom master ada di sheet Rofo
            brand_col = [c for c in raw_rofo.columns if 'Brand' in str(c)]
            product_col = [c for c in raw_rofo.columns if 'Product' in str(c)]
            sku_col_master = [c for c in raw_rofo.columns if 'SKU' in str(c) and 'SAP' in str(c)]
            
            # Handling kalau kolom tidak ditemukan dengan tepat
            if brand_col and product_col and sku_col_master:
                master_sku = raw_rofo[[sku_col_master[0], brand_col[0], product_col[0]]].copy()
                master_sku.columns = ['SKU SAP', 'Brand', 'Product Name']
                master_sku['SKU SAP'] = master_sku['SKU SAP'].astype(str)
                master_sku = master_sku.drop_duplicates(subset=['SKU SAP'])
            else:
                # Dummy master kalau kolom ga ketemu
                st.warning("Kolom Brand/Product tidak terdeteksi otomatis di sheet Rofo. Filter Brand mungkin tidak akurat.")
                master_sku = df_rofo[['SKU SAP']].drop_duplicates()
                master_sku['Brand'] = 'Unknown'
                master_sku['Product Name'] = master_sku['SKU SAP']

            # Merging
            df_merge = pd.merge(df_rofo, df_sales, on=['SKU SAP', 'Date'], how='outer').fillna(0)
            df_merge = pd.merge(df_merge, df_po, on=['SKU SAP', 'Date'], how='outer').fillna(0)
            
            df_merge['SKU SAP'] = df_merge['SKU SAP'].astype(str)
            df_final = pd.merge(df_merge, master_sku, on='SKU SAP', how='left')
            df_final['Brand'] = df_final['Brand'].fillna('Unknown')

            # --- PERHITUNGAN METRIC (Sama kayak sebelumnya) ---
            def get_status(row):
                fc = row['Forecast_Qty']
                act = row['Sales_Qty']
                if act == 0:
                    return "Accurate" if fc == 0 else "Over Forecast"
                acc = fc / act
                if 0.8 <= acc <= 1.2: return "Accurate"
                elif acc < 0.8: return "Under Forecast"
                else: return "Over Forecast"

            df_final['Status_Accuracy'] = df_final.apply(get_status, axis=1)
            df_final['Absorption_Pct'] = df_final.apply(lambda x: (x['PO_Qty'] / x['Forecast_Qty'] * 100) if x['Forecast_Qty'] > 0 else 0, axis=1)

            # Simpan ke session state biar ga ilang pas ganti filter
            st.session_state['df_final'] = df_final
            st.session_state['data_processed'] = True

# --- TAMPILAN DASHBOARD ---
if st.session_state.get('data_processed'):
    df_final = st.session_state['df_final']

    # Filter Sidebar
    st.sidebar.divider()
    all_brands = sorted(df_final['Brand'].astype(str).unique())
    selected_brand = st.sidebar.multiselect("Filter Brand", options=all_brands, default=all_brands)
    
    df_filtered = df_final[df_final['Brand'].isin(selected_brand)]

    # Tabs
    tab1, tab2, tab3 = st.tabs(["🎯 Forecast Accuracy", "📦 PO Absorption", "📄 Data Detail"])

    # TAB 1: SALES ACCURACY
    with tab1:
        st.subheader("Forecast vs Actual Sales")
        
        # Metric Cards
        col1, col2, col3 = st.columns(3)
        total_fc = df_filtered['Forecast_Qty'].sum()
        total_sales = df_filtered['Sales_Qty'].sum()
        avg_acc = (total_fc / total_sales * 100) if total_sales > 0 else 0
        
        col1.metric("Total Forecast", f"{total_fc:,.0f}")
        col2.metric("Total Sales", f"{total_sales:,.0f}")
        col3.metric("Accuracy Rate (Global)", f"{avg_acc:.1f}%")
        
        # Grafik Trend
        monthly_grp = df_filtered.groupby('Date')[['Forecast_Qty', 'Sales_Qty']].sum().reset_index()
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(x=monthly_grp['Date'], y=monthly_grp['Forecast_Qty'], name='Forecast'))
        fig_trend.add_trace(go.Scatter(x=monthly_grp['Date'], y=monthly_grp['Sales_Qty'], name='Sales', line=dict(dash='dot')))
        st.plotly_chart(fig_trend, use_container_width=True)

        # Grafik Pie Chart
        status_counts = df_filtered['Status_Accuracy'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        color_map = {'Accurate': '#2ca02c', 'Over Forecast': '#d62728', 'Under Forecast': '#ff7f0e'}
        fig_pie = px.pie(status_counts, values='Count', names='Status', color='Status', color_discrete_map=color_map, title='Distribusi Status Akurasi (per SKU)')
        st.plotly_chart(fig_pie)

    # TAB 2: PO ABSORPTION
    with tab2:
        st.subheader("Forecast vs PO (Absorption)")
        
        po_grp = df_filtered.groupby('Date')[['Forecast_Qty', 'PO_Qty']].sum().reset_index()
        fig_po = go.Figure()
        fig_po.add_trace(go.Bar(x=po_grp['Date'], y=po_grp['Forecast_Qty'], name='Forecast Plan'))
        fig_po.add_trace(go.Bar(x=po_grp['Date'], y=po_grp['PO_Qty'], name='PO Issued'))
        st.plotly_chart(fig_po, use_container_width=True)
        
        st.write("Daftar SKU dengan Absorpsi Rendah (<50%) bulan ini:")
        low_abs = df_filtered[(df_filtered['Absorption_Pct'] < 50) & (df_filtered['Forecast_Qty'] > 0)]
        st.dataframe(low_abs[['Date', 'Brand', 'Product Name', 'Forecast_Qty', 'PO_Qty', 'Absorption_Pct']].sort_values('Absorption_Pct'))

    # TAB 3: DATA
    with tab3:
        st.dataframe(df_filtered)
        csv = df_filtered.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", data=csv, file_name='processed_dashboard.csv', mime='text/csv')

else:
    st.info("Silakan upload file Excel dan klik 'Proses Dashboard' untuk melihat hasil.")
