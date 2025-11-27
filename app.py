import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt
from datetime import datetime
import re
import traceback

# -------------------------------------------------------------------
# 0. KONFIGURASI APLIKASI
# -------------------------------------------------------------------

# Set page config di awal
st.set_page_config(page_title="Advanced Forecast Dashboard", layout="wide", page_icon="📈")

# Link Single Google Sheet
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit"

# Nama Tab di Google Sheet (WAJIB SESUAIKAN JIKA BEDA)
SHEET_CONFIG = {
    'rofo': 'Rofo',
    'po': 'PO',
    'sales': 'Sales'
}

# -------------------------------------------------------------------
# 1. KONEKSI GOOGLE SHEETS
# -------------------------------------------------------------------

@st.cache_resource
def get_service_account():
    """Load Service Account dari secrets.toml"""
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("❌ Secrets 'gcp_service_account' tidak ditemukan! Silakan masukkan ke Streamlit Secrets.")
            return None
        creds_dict = dict(st.secrets["gcp_service_account"])
        gc = gspread.service_account_from_dict(creds_dict)
        return gc
    except Exception as e:
        st.error(f"❌ Error service account: Pastikan format TOML key sudah benar. Error: {str(e)}")
        return None

@st.cache_data(ttl=600) # Cache 10 menit
def load_sheet_data(url, sheet_name):
    """Load data dari sheet tertentu"""
    gc = get_service_account()
    if not gc: return pd.DataFrame()
    
    try:
        sh = gc.open_by_url(url)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_values()
        
        if not data or len(data) <= 1:
            st.warning(f"⚠️ Data kosong di tab: {sheet_name}")
            return pd.DataFrame()
            
        headers = [h.strip() for h in data[0]] # Clean header
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        
        st.success(f"✅ Loaded {sheet_name}: {len(df)} rows, {len(df.columns)} cols")
        return df
        
    except gspread.WorksheetNotFound:
        st.error(f"❌ Tab '{sheet_name}' tidak ditemukan! Cek nama tab di Google Sheet.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error load {sheet_name}: {str(e)}")
        return pd.DataFrame()

def extract_date_columns(columns):
    """Mendeteksi kolom tanggal secara fleksibel."""
    date_cols = []
    # Mencari pola 'YYYY-MM-DD', 'DD-MM-YYYY', 'YYYY', atau kata bulan (Jan/Feb)
    for col in columns:
        if re.search(r'(202\d)|(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', col): 
            date_cols.append(col)
    return date_cols

# -------------------------------------------------------------------
# 3. PROCESS DATA (CORE LOGIC)
# -------------------------------------------------------------------

@st.cache_data
def process_all_data():
    """Process data Rofo (horizontal) + PO (vertical) + Sales (horizontal)"""
    
    with st.spinner("🔄 Memuat dan memproses data dari Google Sheets..."):
        df_rofo = load_sheet_data(SPREADSHEET_URL, SHEET_CONFIG['rofo'])
        df_po = load_sheet_data(SPREADSHEET_URL, SHEET_CONFIG['po'])
        df_sales = load_sheet_data(SPREADSHEET_URL, SHEET_CONFIG['sales'])

    if df_rofo.empty or df_po.empty:
        st.error("❌ Data Rofo atau PO kosong, tidak bisa melanjutkan.")
        return None

    try:
        # ========== A. PROCESS ROFO (HORIZONTAL) ==========
        rofo_sku_col = 'SKU SAP'
        if rofo_sku_col not in df_rofo.columns:
            st.error(f"❌ Kolom '{rofo_sku_col}' tidak ditemukan di data Rofo.")
            return None
            
        month_cols = extract_date_columns(df_rofo.columns)
        if not month_cols:
            st.error("❌ Kolom bulan (tanggal) tidak ditemukan di data Rofo.")
            return None

        id_cols = [col for col in df_rofo.columns if col not in month_cols]
        df_rofo_long = df_rofo.melt(
            id_vars=id_cols,
            value_vars=month_cols,
            var_name="Month_Raw",
            value_name="ROFO_Qty"
        ).rename(columns={rofo_sku_col: 'SKU'})

        # Convert dan clean data Rofo
        df_rofo_long["ROFO_Qty"] = pd.to_numeric(
            df_rofo_long["ROFO_Qty"], errors="coerce"
        ).fillna(0).clip(lower=0)
        
        # Flexibel date conversion (diasumsikan format 2025-01-25)
        df_rofo_long["Date"] = pd.to_datetime(
             df_rofo_long["Month_Raw"], errors='coerce'
        ).dt.to_period("M")
        
        df_rofo_long = df_rofo_long.dropna(subset=["Date"])
        df_rofo_final = df_rofo_long.groupby(["SKU", "Date"])["ROFO_Qty"].sum().reset_index()


        # ========== B. PROCESS PO (VERTICAL) ==========
        po_required_cols = ['Document Date', 'SKU SAP', 'Quantity'] # Asumsi Quantity = Actual Qty
        missing_cols = [col for col in po_required_cols if col not in df_po.columns]
        
        if missing_cols:
             # Cek fallback Order Quantity
             if 'Order Quantity' in df_po.columns and 'Quantity' in missing_cols:
                 po_required_cols[-1] = 'Order Quantity'
                 st.info("ℹ️ Menggunakan kolom 'Order Quantity' sebagai Kuantitas PO.")
             else:
                 st.error(f"❌ Kolom PO tidak ditemukan: {missing_cols}")
                 return None

        df_po_clean = df_po.copy()
        df_po_clean["Actual_Qty"] = pd.to_numeric(
            df_po_clean[po_required_cols[-1]], errors="coerce"
        ).fillna(0).clip(lower=0)
        
        # Convert Document Date ke Period
        df_po_clean["Date"] = pd.to_datetime(
            df_po_clean["Document Date"], errors="coerce"
        ).dt.to_period("M")
        
        df_po_clean = df_po_clean.dropna(subset=["Date"])
        df_po_grouped = df_po_clean.groupby(["SKU SAP", "Date"])["Actual_Qty"].sum().reset_index().rename(columns={"SKU SAP": "SKU"})


        # ========== C. MERGE ROFO + PO & CALCULATE METRICS ==========
        df_merged = pd.merge(
            df_rofo_final,
            df_po_grouped,
            on=["SKU", "Date"],
            how="outer"
        ).fillna(0)
        
        df_merged = df_merged[(df_merged["ROFO_Qty"] > 0) | (df_merged["Actual_Qty"] > 0)]

        # --- Metrics ---
        def calculate_far(row):
            if row["ROFO_Qty"] == 0:
                return 0.0
            return row["Actual_Qty"] / row["ROFO_Qty"]

        df_merged["FAR"] = df_merged.apply(calculate_far, axis=1)
        df_merged["Bias"] = df_merged["ROFO_Qty"] - df_merged["Actual_Qty"]

        # Status Logic
        df_merged['Status'] = np.select(
            [
                (df_merged['ROFO_Qty'] == 0) & (df_merged['Actual_Qty'] > 0),
                (df_merged['ROFO_Qty'] > 0) & (df_merged['Actual_Qty'] == 0),
                (df_merged['FAR'] >= 0.8) & (df_merged['FAR'] <= 1.2),
                (df_merged['FAR'] < 0.8),
                (df_merged['FAR'] > 1.2)
            ],
            [
                "Unforecasted Demand", # Actual > 0, Forecast = 0
                "Missed (No Supply)",  # Forecast > 0, Actual = 0
                "Accurate (80-120%)", 
                "Under-Delivery",
                "Over-Delivery"
            ],
            default="No Activity"
        )
        
        df_merged["Month_Str"] = df_merged["Date"].astype(str)

        # ========== D. PROCESS SALES (OPTIONAL) ==========
        sales_data_dict = {}
        sales_sku_col = 'SKU SAP'
        if not df_sales.empty and sales_sku_col in df_sales.columns:
            sales_month_cols = extract_date_columns(df_sales.columns)
            if sales_month_cols:
                df_sales_long = df_sales.melt(
                    id_vars=[sales_sku_col],
                    value_vars=sales_month_cols,
                    var_name="Month_Raw",
                    value_name="Sales_Qty"
                ).rename(columns={sales_sku_col: 'SKU'})
                
                df_sales_long["Sales_Qty"] = pd.to_numeric(df_sales_long["Sales_Qty"], errors="coerce").fillna(0).clip(lower=0)
                df_sales_long["Date"] = pd.to_datetime(df_sales_long["Month_Raw"], errors='coerce').dt.to_period("M")
                df_sales_data = df_sales_long.groupby(["SKU", "Date"])["Sales_Qty"].sum().reset_index()
                sales_data_dict['sales_data'] = df_sales_data.rename(columns={'Sales_Qty': 'Sales_Qty_Ref'})


        # --- FINAL OUTPUT ---
        final_df = pd.merge(df_merged, sales_data_dict.get('sales_data', pd.DataFrame()), on=['SKU', 'Date'], how='left').fillna({'Sales_Qty_Ref': 0})
        
        st.success(f"✅ Processing selesai: {len(final_df)} baris data gabungan.")
        return final_df

    except Exception as e:
        st.error(f"❌ Error processing data: {str(e)}")
        st.code(traceback.format_exc())
        return None

# -------------------------------------------------------------------
# 4. DASHBOARD COMPONENTS
# -------------------------------------------------------------------

def create_dashboard(df):
    """Create dashboard dengan data yang sudah diproses"""
    
    st.header("📊 Forecast Accuracy Dashboard")
    
    # ========== FILTERS ==========
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        all_skus = ["All"] + sorted(df["SKU"].astype(str).unique().tolist())
        selected_sku = st.selectbox("Filter by SKU:", all_skus)
    
    with col2:
        all_months = ["All"] + sorted(df["Month_Str"].unique().tolist())
        selected_month = st.selectbox("Filter by Month:", all_months)
    
    with col3:
        all_statuses = ["All"] + sorted(df["Status"].unique().tolist())
        selected_status = st.selectbox("Filter by Status:", all_statuses)

    # Apply filters
    df_filtered = df.copy()
    if selected_sku != "All":
        df_filtered = df_filtered[df_filtered["SKU"] == selected_sku]
    if selected_month != "All":
        df_filtered = df_filtered[df_filtered["Month_Str"] == selected_month]
    if selected_status != "All":
        df_filtered = df_filtered[df_filtered["Status"] == selected_status]

    if df_filtered.empty:
        st.warning("📭 Tidak ada data untuk filter yang dipilih")
        return

    # ========== KPI CARDS ==========
    total_rofo = df_filtered["ROFO_Qty"].sum()
    total_actual = df_filtered["Actual_Qty"].sum()
    total_sales = df_filtered["Sales_Qty_Ref"].sum()
    total_bias = total_rofo - total_actual
    
    avg_far = total_actual / total_rofo if total_rofo > 0 else 0
    accuracy_rate = (df_filtered["Status"] == "Accurate (80-120%)").mean()
    
    st.subheader("🎯 Key Performance Indicators (PO vs Rofo)")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total ROFO", f"{total_rofo:,.0f}")
    with col2:
        st.metric("Total Actual (PO)", f"{total_actual:,.0f}")
    with col3:
        st.metric("Overall FAR", f"{avg_far:.1%}", help="Actual PO / ROFO. Target: 80% - 120%")
    with col4:
        st.metric("Accuracy Rate", f"{accuracy_rate:.1%}", help="Persentase periode SKU yang akurat (80%-120%)")
    with col5:
        st.metric("Total Bias", f"{total_bias:,.0f}", delta="Positif = Over Forecast (Terlalu Tinggi)")

    st.divider()
    
    # ========== CHARTS ==========
    tab_trend, tab_bias, tab_status = st.tabs(["📈 Monthly Trend", "🔥 Top Bias SKU", "📊 Status Distribution"])

    with tab_trend:
        st.subheader("Tren Rofo vs Actual vs Sales")
        chart_data = df_filtered.groupby("Month_Str").agg({
            "ROFO_Qty": "sum",
            "Actual_Qty": "sum",
            "Sales_Qty_Ref": "sum"
        }).reset_index()

        trend_chart = alt.Chart(chart_data).transform_fold(
            ['ROFO_Qty', 'Actual_Qty', 'Sales_Qty_Ref'],
            as_=['Metric', 'Quantity']
        ).mark_line(point=True).encode(
            x=alt.X('Month_Str:O', title='Bulan', sort=chart_data['Month_Str'].tolist()),
            y=alt.Y('Quantity:Q', title='Quantity', axis=alt.Axis(format='~s')),
            color=alt.Color('Metric:N', scale=alt.Scale(
                domain=['ROFO_Qty', 'Actual_Qty', 'Sales_Qty_Ref'],
                range=['#3498db', '#2ecc71', '#e74c3c']
            )),
            tooltip=['Month_Str', 'Metric', alt.Tooltip('Quantity:Q', format=',')]
        ).properties(height=400, title="Monthly Trend (Rofo, PO, Sales)")
        
        st.altair_chart(trend_chart, use_container_width=True)

    with tab_bias:
        st.subheader("Top 10 SKU dengan Bias Tertinggi (Absolute)")
        bias_df = df_filtered.groupby('SKU').agg(
            Bias=('Bias', 'sum'),
            ROFO_Qty=('ROFO_Qty', 'sum'),
            Actual_Qty=('Actual_Qty', 'sum')
        ).reset_index()
        bias_df['Abs_Bias'] = bias_df['Bias'].abs()
        top_bias = bias_df.nlargest(10, 'Abs_Bias')
        
        bias_chart = alt.Chart(top_bias).mark_bar().encode(
            x=alt.X('Bias:Q', title='Total Bias (Rofo - Actual)'),
            y=alt.Y('SKU:N', sort=alt.EncodingSortField(field="Abs_Bias", order="descending"), title='SKU'),
            color=alt.condition(
                alt.datum.Bias > 0,
                alt.value('#e74c3c'), # Over Forecast (Merah)
                alt.value('#27ae60') # Under Forecast (Hijau)
            ),
            tooltip=['SKU', alt.Tooltip('Bias:Q', format=',.0f'), alt.Tooltip('ROFO_Qty:Q', format=',.0f'), alt.Tooltip('Actual_Qty:Q', format=',.0f')]
        ).properties(height=400)
        
        st.altair_chart(bias_chart, use_container_width=True)

    with tab_status:
        st.subheader("Distribusi Status Akurasi (Per Periode SKU)")
        status_dist = df_filtered['Status'].value_counts().reset_index()
        status_dist.columns = ['Status', 'Count']
        
        status_colors = alt.Scale(domain=sorted(status_dist['Status'].tolist()), range=['#27ae60', '#f1c40f', '#e74c3c', '#8e44ad', '#34495e'])

        status_chart = alt.Chart(status_dist).mark_bar().encode(
            x=alt.X('Status:N', title='Status', sort='-y'),
            y=alt.Y('Count:Q', title='Jumlah Periode SKU'),
            color=alt.Color('Status:N', scale=status_colors),
            tooltip=['Status', 'Count']
        ).properties(height=400)
        
        st.altair_chart(status_chart, use_container_width=True)


    # --- DATA TABLE ---
    st.markdown("### 📋 Detail Data Gabungan")
    display_cols = ['SKU', 'Month_Str', 'ROFO_Qty', 'Actual_Qty', 'Sales_Qty_Ref', 'FAR', 'Bias', 'Status']
    
    st.dataframe(
        df_filtered[display_cols].sort_values(['Month_Str', 'SKU'])
        .style.format({
            'ROFO_Qty': '{:,.0f}', 
            'Actual_Qty': '{:,.0f}', 
            'Sales_Qty_Ref': '{:,.0f}',
            'FAR': '{:.1%}',
            'Bias': '{:,.0f}'
        }), 
        use_container_width=True
    )
    
    csv = df_filtered.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name=f"forecast_dashboard_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

# -------------------------------------------------------------------
# 5. MAIN APP EXECUTION
# -------------------------------------------------------------------

def main():
    st.title("📈 Forecast Accuracy & Performance Dashboard")
    st.markdown(f"Data Source: [Google Sheet]({SPREADSHEET_URL})")

    # Ambil data (atau dari cache)
    df_final = process_all_data()

    if df_final is not None and not df_final.empty:
        # Panggil dashboard dengan DataFrame yang sudah diproses
        create_dashboard(df_final)
    else:
        st.warning("⚠️ Dashboard tidak dapat ditampilkan. Periksa pesan error di atas dan pastikan data di Google Sheet sudah terisi.")


if __name__ == "__main__":
    main()
