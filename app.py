import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt
from datetime import datetime
import traceback

# -------------------------------------------------------------------
# 1. KONFIGURASI - SINGLE SOURCE
# -------------------------------------------------------------------

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit?gid=857579960#gid=857579960"

SHEET_NAMES = {
    'rofo': 'Rofo',
    'po': 'PO', 
    'sales': 'Sales'
}

# Session state untuk caching
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'df_processed' not in st.session_state:
    st.session_state.df_processed = None

# -------------------------------------------------------------------
# 2. KONEKSI GOOGLE SHEETS - OPTIMIZED
# -------------------------------------------------------------------

def get_service_account():
    """Load Service Account dari secrets.toml"""
    try:
        if 'gcp_service_account' in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            gc = gspread.service_account_from_dict(creds_dict)
            return gc
        else:
            st.error("❌ gcp_service_account tidak ditemukan di secrets.toml")
            return None
    except Exception as e:
        st.error(f"❌ Error service account: {str(e)}")
        return None

def load_sheet_data(sheet_name):
    """Load data dari sheet tertentu"""
    try:
        gc = get_service_account()
        if gc is None:
            return pd.DataFrame()
            
        sh = gc.open_by_url(SPREADSHEET_URL)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_values()
        
        if not data or len(data) <= 1:
            st.warning(f"⚠️ Data kosong di sheet {sheet_name}")
            return pd.DataFrame()
        
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        df.columns = df.columns.str.strip()
        
        st.success(f"✅ {sheet_name}: {len(df)} rows, {len(df.columns)} cols")
        return df
        
    except Exception as e:
        st.error(f"❌ Error load {sheet_name}: {str(e)}")
        return pd.DataFrame()

# -------------------------------------------------------------------
# 3. PROCESS DATA - SESUAI STRUKTUR ANDA
# -------------------------------------------------------------------

def process_all_data():
    """Process data Rofo (horizontal) + PO (vertical) + Sales (horizontal)"""
    try:
        # Load semua sheet
        with st.spinner("📥 Memuat data dari Google Sheets..."):
            df_rofo = load_sheet_data(SHEET_NAMES['rofo'])
            df_po = load_sheet_data(SHEET_NAMES['po'])
            df_sales = load_sheet_data(SHEET_NAMES['sales'])

        if df_rofo.empty or df_po.empty:
            st.error("❌ Data Rofo atau PO kosong")
            return None

        # ========== PROCESS ROFO (HORIZONTAL TIMESERIES) ==========
        st.info("🔄 Memproses data Rofo...")
        
        # Cari kolom SKU SAP
        sku_col = 'SKU SAP'
        if sku_col not in df_rofo.columns:
            st.error(f"❌ Kolom {sku_col} tidak ditemukan di data Rofo")
            st.info(f"Kolom Rofo: {list(df_rofo.columns)}")
            return None

        # Identifikasi kolom bulan (Jan-2025, Feb-2025, dst)
        month_cols = [col for col in df_rofo.columns if any(month in col for month in 
                     ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])]
        
        if not month_cols:
            st.error("❌ Kolom bulan tidak ditemukan di data Rofo")
            return None

        # Melt data Rofo dari wide to long
        id_cols = [sku_col]
        if 'Product Name' in df_rofo.columns:
            id_cols.append('Product Name')
        elif 'Description' in df_rofo.columns:
            id_cols.append('Description')

        df_rofo_long = df_rofo.melt(
            id_vars=id_cols,
            value_vars=month_cols,
            var_name="Month",
            value_name="ROFO Quantity"
        ).rename(columns={sku_col: 'SKU'})

        # Convert dan clean data Rofo
        df_rofo_long["ROFO Quantity"] = pd.to_numeric(
            df_rofo_long["ROFO Quantity"], errors="coerce"
        ).fillna(0).clip(lower=0)
        
        # Convert Month to datetime (format: Jan-2025 -> 2025-01)
        df_rofo_long["Date"] = pd.to_datetime(
            df_rofo_long["Month"], format='%b-%Y', errors='coerce'
        ).dt.to_period("M")
        
        df_rofo_long = df_rofo_long.dropna(subset=["Date"])
        df_rofo_long["Date"] = df_rofo_long["Date"].astype(str)

        # ========== PROCESS PO (VERTICAL TIMESERIES) ==========
        st.info("🔄 Memproses data PO...")
        
        # Cari kolom yang diperlukan di PO
        required_po_cols = ['Document Date', 'SKU SAP', 'Quantity']
        missing_cols = [col for col in required_po_cols if col not in df_po.columns]
        
        if missing_cols:
            st.error(f"❌ Kolom PO tidak ditemukan: {missing_cols}")
            st.info(f"Kolom PO yang tersedia: {list(df_po.columns)}")
            return None

        # Process PO data
        df_po_clean = df_po.copy()
        df_po_clean["Actual Quantity"] = pd.to_numeric(
            df_po_clean["Quantity"], errors="coerce"
        ).fillna(0).clip(lower=0)
        
        df_po_clean["Date"] = pd.to_datetime(
            df_po_clean["Document Date"], errors="coerce"
        ).dt.to_period("M")
        
        df_po_clean = df_po_clean.dropna(subset=["Date"])
        
        # Group by SKU dan Month
        df_po_grouped = df_po_clean.groupby(["SKU SAP", "Date"])["Actual Quantity"].sum().reset_index()
        df_po_grouped = df_po_grouped.rename(columns={"SKU SAP": "SKU"})
        df_po_grouped["Date"] = df_po_grouped["Date"].astype(str)

        # ========== MERGE ROFO + PO ==========
        st.info("🔄 Menggabungkan data Rofo + PO...")
        
        df_merged = pd.merge(
            df_rofo_long[["SKU", "Date", "Month", "ROFO Quantity"]],
            df_po_grouped[["SKU", "Date", "Actual Quantity"]],
            on=["SKU", "Date"],
            how="left"  # Keep all Rofo records
        ).fillna(0)

        # Filter hanya data yang memiliki ROFO > 0 atau Actual > 0
        df_merged = df_merged[
            (df_merged["ROFO Quantity"] > 0) | 
            (df_merged["Actual Quantity"] > 0)
        ]

        # ========== CALCULATE METRICS ==========
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
        df_merged["FAR Category"] = np.select(
            [
                df_merged["FAR"] == 0,
                df_merged["FAR"] < 0.8, 
                df_merged["FAR"] <= 1.2,
                df_merged["FAR"] > 1.2
            ],
            ["No Actual", "Under Forecast", "Accurate", "Over Forecast"],
            default="Unknown"
        )

        # ========== PROCESS SALES DATA (OPTIONAL) ==========
        sales_data = None
        if not df_sales.empty and 'SKU SAP' in df_sales.columns:
            st.info("🔄 Memproses data Sales...")
            
            # Process sales data (similar to Rofo)
            sales_month_cols = [col for col in df_sales.columns if any(month in col for month in 
                              ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])]
            
            if sales_month_cols:
                df_sales_long = df_sales.melt(
                    id_vars=['SKU SAP'],
                    value_vars=sales_month_cols,
                    var_name="Month",
                    value_name="Sales Quantity"
                ).rename(columns={'SKU SAP': 'SKU'})
                
                df_sales_long["Sales Quantity"] = pd.to_numeric(
                    df_sales_long["Sales Quantity"], errors="coerce"
                ).fillna(0).clip(lower=0)
                
                df_sales_long["Date"] = pd.to_datetime(
                    df_sales_long["Month"], format='%b-%Y', errors='coerce'
                ).dt.to_period("M").astype(str)
                
                sales_data = df_sales_long[["SKU", "Date", "Sales Quantity"]]

        st.success(f"✅ Processing selesai: {len(df_merged)} baris data")
        return {
            'forecast_data': df_merged,
            'sales_data': sales_data
        }

    except Exception as e:
        st.error(f"❌ Error processing data: {str(e)}")
        st.code(traceback.format_exc())
        return None

# -------------------------------------------------------------------
# 4. DASHBOARD COMPONENTS
# -------------------------------------------------------------------

def create_dashboard(data_dict):
    """Create dashboard dengan data yang sudah diproses"""
    
    df = data_dict['forecast_data']
    sales_data = data_dict.get('sales_data')
    
    st.header("📊 Forecast Accuracy Dashboard")
    
    # ========== FILTERS ==========
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        all_skus = ["All"] + sorted(df["SKU"].unique().tolist())
        selected_sku = st.selectbox("Filter by SKU:", all_skus)
    
    with col2:
        all_months = ["All"] + sorted(df["Month"].unique().tolist())
        selected_month = st.selectbox("Filter by Month:", all_months)
    
    with col3:
        far_categories = ["All"] + sorted(df["FAR Category"].unique().tolist())
        selected_category = st.selectbox("Filter by FAR Category:", far_categories)

    # Apply filters
    df_filtered = df.copy()
    if selected_sku != "All":
        df_filtered = df_filtered[df_filtered["SKU"] == selected_sku]
    if selected_month != "All":
        df_filtered = df_filtered[df_filtered["Month"] == selected_month]
    if selected_category != "All":
        df_filtered = df_filtered[df_filtered["FAR Category"] == selected_category]

    if df_filtered.empty:
        st.warning("📭 Tidak ada data untuk filter yang dipilih")
        return

    # ========== KPI CARDS ==========
    total_rofo = df_filtered["ROFO Quantity"].sum()
    total_actual = df_filtered["Actual Quantity"].sum()
    total_bias = total_rofo - total_actual
    
    far_overall = total_actual / total_rofo if total_rofo > 0 else 0
    
    accuracy_rate = (df_filtered["Accuracy Status"] == "Accurate").mean()
    accurate_count = (df_filtered["Accuracy Status"] == "Accurate").sum()
    total_count = len(df_filtered)

    st.subheader("🎯 Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total ROFO", f"{total_rofo:,.0f}")
    with col2:
        st.metric("Total Actual", f"{total_actual:,.0f}")
    with col3:
        st.metric("Overall FAR", f"{far_overall:.1%}")
    with col4:
        st.metric("Accuracy Rate", f"{accuracy_rate:.1%}")
    with col5:
        st.metric("Total Bias", f"{total_bias:,.0f}")

    # ========== CHARTS ==========
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Trend Analysis", "🔥 Bias Analysis", "📊 Accuracy Overview", "📋 Data Details"])

    with tab1:
        # Monthly Trend
        monthly_trend = df_filtered.groupby("Month").agg({
            "ROFO Quantity": "sum",
            "Actual Quantity": "sum"
        }).reset_index()

        trend_chart = alt.Chart(monthly_trend).transform_fold(
            ['ROFO Quantity', 'Actual Quantity'],
            as_=['Type', 'Quantity']
        ).mark_line(point=True).encode(
            x=alt.X('Month:O', title='Month', sort=monthly_trend['Month'].tolist()),
            y=alt.Y('Quantity:Q', title='Quantity', axis=alt.Axis(format='~s')),
            color=alt.Color('Type:N', scale=alt.Scale(
                domain=['ROFO Quantity', 'Actual Quantity'],
                range=['#1f77b4', '#ff7f0e']
            )),
            strokeDash=alt.StrokeDash('Type:N', scale=alt.Scale(
                domain=['ROFO Quantity', 'Actual Quantity'],
                range=[[1, 0], [5, 5]]
            )),
            tooltip=['Month', 'Type', alt.Tooltip('Quantity:Q', format=',')]
        ).properties(height=400, title="Monthly ROFO vs Actual Trend")
        
        st.altair_chart(trend_chart, use_container_width=True)

    with tab2:
        # Top 10 Bias by SKU
        sku_bias = df_filtered.groupby("SKU").agg({
            "Bias": "sum",
            "ROFO Quantity": "sum",
            "Actual Quantity": "sum"
        }).reset_index().nlargest(10, "Bias")

        bias_chart = alt.Chart(sku_bias).mark_bar().encode(
            y=alt.Y('SKU:N', sort='-x', title='SKU'),
            x=alt.X('Bias:Q', title='Bias', axis=alt.Axis(format='~s')),
            color=alt.condition(
                alt.datum.Bias > 0,
                alt.value('#ff4b4b'),
                alt.value('#4caf50')
            ),
            tooltip=['SKU', 'Bias:Q', 'ROFO Quantity:Q', 'Actual Quantity:Q']
        ).properties(height=400, title="Top 10 SKU by Bias")
        
        st.altair_chart(bias_chart, use_container_width=True)

    with tab3:
        col1, col2 = st.columns(2)
        
        with col1:
            # Accuracy Distribution
            accuracy_dist = df_filtered["Accuracy Status"].value_counts().reset_index()
            accuracy_pie = alt.Chart(accuracy_dist).mark_arc().encode(
                theta=alt.Theta('count:Q'),
                color=alt.Color('Accuracy Status:N', scale=alt.Scale(
                    domain=['Accurate', 'Non-Accurate'],
                    range=['#4caf50', '#ff4b4b']
                )),
                tooltip=['Accuracy Status', 'count']
            ).properties(height=300, title="Accuracy Distribution")
            
            st.altair_chart(accuracy_pie, use_container_width=True)
        
        with col2:
            # FAR Category Distribution
            far_dist = df_filtered["FAR Category"].value_counts().reset_index()
            far_bar = alt.Chart(far_dist).mark_bar().encode(
                x=alt.X('count:Q', title='Count'),
                y=alt.Y('FAR Category:N', sort='-x', title='FAR Category'),
                color=alt.Color('FAR Category:N', scale=alt.Scale(
                    domain=['Accurate', 'Under Forecast', 'Over Forecast', 'No Actual'],
                    range=['#4caf50', '#ffa500', '#ff4b4b', '#969696']
                )),
                tooltip=['FAR Category', 'count']
            ).properties(height=300, title="FAR Category Distribution")
            
            st.altair_chart(far_bar, use_container_width=True)

    with tab4:
        # Data Table
        st.subheader("Detailed Data")
        
        display_cols = ['SKU', 'Month', 'ROFO Quantity', 'Actual Quantity', 'FAR', 'Accuracy Status', 'Bias', 'FAR Category']
        available_cols = [col for col in display_cols if col in df_filtered.columns]
        
        df_display = df_filtered[available_cols].copy()
        df_display['ROFO Quantity'] = df_display['ROFO Quantity'].apply(lambda x: f"{x:,.0f}")
        df_display['Actual Quantity'] = df_display['Actual Quantity'].apply(lambda x: f"{x:,.0f}")
        df_display['Bias'] = df_display['Bias'].apply(lambda x: f"{x:,.0f}")
        df_display['FAR'] = df_display['FAR'].apply(lambda x: f"{x:.1%}")
        
        st.dataframe(df_display, use_container_width=True)
        
        # Download button
        csv = df_filtered[available_cols].to_csv(index=False)
        st.download_button(
            label="📥 Download Data as CSV",
            data=csv,
            file_name=f"forecast_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

    # ========== SALES COMPARISON (OPTIONAL) ==========
    if sales_data is not None:
        st.markdown("---")
        st.header("📦 Sales Performance Comparison")
        
        # Merge sales data dengan forecast data
        df_with_sales = pd.merge(
            df_filtered,
            sales_data,
            on=['SKU', 'Date'],
            how='left'
        ).fillna(0)
        
        if not df_with_sales.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Sales", f"{df_with_sales['Sales Quantity'].sum():,.0f}")
            
            with col2:
                sales_achievement = df_with_sales['Sales Quantity'].sum() / df_with_sales['Actual Quantity'].sum() if df_with_sales['Actual Quantity'].sum() > 0 else 0
                st.metric("Sales vs PO Achievement", f"{sales_achievement:.1%}")
            
            # Sales vs PO trend
            sales_trend = df_with_sales.groupby("Month").agg({
                "Actual Quantity": "sum",
                "Sales Quantity": "sum"
            }).reset_index()

            sales_chart = alt.Chart(sales_trend).transform_fold(
                ['Actual Quantity', 'Sales Quantity'],
                as_=['Type', 'Quantity']
            ).mark_line(point=True).encode(
                x=alt.X('Month:O', sort=sales_trend['Month'].tolist()),
                y=alt.Y('Quantity:Q', axis=alt.Axis(format='~s')),
                color='Type:N',
                tooltip=['Month', 'Type', alt.Tooltip('Quantity:Q', format=',')]
            ).properties(height=300, title="PO vs Sales Trend")
            
            st.altair_chart(sales_chart, use_container_width=True)

# -------------------------------------------------------------------
# 5. MAIN APP
# -------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="Forecast Accuracy Dashboard",
        layout="wide",
        page_icon="📊"
    )

    # Sidebar
    with st.sidebar:
        st.title("📊 Forecast Dashboard")
        st.markdown("---")
        
        st.subheader("Data Control")
        
        if st.button("🔄 Load/Refresh Data", type="primary", use_container_width=True):
            st.session_state.data_loaded = False
            st.rerun()
            
        st.markdown("---")
        st.info("""
        **Data Sources:**
        - 📋 Rofo: Forecast data (horizontal)
        - 📦 PO: Actual absorption (vertical)  
        - 🏪 Sales: End-customer sales (horizontal)
        
        **Primary Key:** SKU SAP
        """)

    # Main content
    st.title("📈 Forecast Accuracy & Performance Dashboard")
    
    if not st.session_state.data_loaded:
        data_dict = process_all_data()
        
        if data_dict is not None:
            st.session_state.df_processed = data_dict
            st.session_state.data_loaded = True
            st.success("✅ Data berhasil dimuat!")
            st.rerun()
        else:
            st.error("❌ Gagal memuat data. Periksa koneksi dan struktur data.")
    else:
        create_dashboard(st.session_state.df_processed)

if __name__ == "__main__":
    main()
