import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import gspread
from google.oauth2 import service_account

# Konfigurasi halaman
st.set_page_config(
    page_title="SKU Management Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS custom
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
    }
    .warning-card {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #ffc107;
    }
</style>
""", unsafe_allow_html=True)

class GoogleSheetsConnector:
    def __init__(self):
        self.credentials = self.get_credentials()
        self.client = gspread.authorize(self.credentials)
        
    def get_credentials(self):
        """Mendapatkan credentials dari secrets.toml"""
        try:
            # Create credentials from secrets
            credentials_dict = {
                "type": st.secrets["gcp_service_account"]["type"],
                "project_id": st.secrets["gcp_service_account"]["project_id"],
                "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
                "private_key": st.secrets["gcp_service_account"]["private_key"].replace('\\n', '\n'),
                "client_email": st.secrets["gcp_service_account"]["client_email"],
                "client_id": st.secrets["gcp_service_account"]["client_id"],
                "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
                "token_uri": st.secrets["gcp_service_account"]["token_uri"],
                "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
                "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
            }
            credentials = service_account.Credentials.from_service_account_info(
                credentials_dict,
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
            return credentials
        except Exception as e:
            st.error(f"Error loading credentials: {e}")
            return None
    
    def get_sheet_data(self, sheet_url, sheet_name):
        """Mendapatkan data dari sheet tertentu"""
        try:
            spreadsheet = self.client.open_by_url(sheet_url)
            worksheet = spreadsheet.worksheet(sheet_name)
            data = worksheet.get_all_records()
            return pd.DataFrame(data)
        except Exception as e:
            st.error(f"Error getting data from {sheet_name}: {e}")
            return pd.DataFrame()

class SKUAnalyzer:
    def __init__(self, master_data, rofo_data, sales_data):
        self.master_data = master_data
        self.rofo_data = rofo_data
        self.sales_data = sales_data
        self.prepare_data()
    
    def prepare_data(self):
        """Mempersiapkan dan membersihkan data"""
        try:
            # Clean master data
            if not self.master_data.empty:
                self.master_data.columns = ['Material', 'OLD_Material', 'SKU_SAP']
                self.master_data = self.master_data.dropna(subset=['Material'])
            
            # Clean Rofo data
            if not self.rofo_data.empty:
                # Get actual column names and map them
                rofo_columns = ['SKU_GOA', 'SKU_SAP', 'Product_Name', 'Brand', 'Notes', 
                               'Qty_Per_Box'] + [f'Forecast_{i:02d}' for i in range(1, 12)]
                
                # Use available columns
                available_cols = min(len(rofo_columns), len(self.rofo_data.columns))
                self.rofo_data.columns = rofo_columns[:available_cols] + list(self.rofo_data.columns[available_cols:])
                self.rofo_data = self.rofo_data[self.rofo_data['SKU_SAP'] != 'SKU SAP']
            
            # Clean Sales data
            if not self.sales_data.empty:
                sales_columns = ['Current_SKU', 'SKU_SAP', 'SKU_Old', 'SKU_Name', 'Brand', 
                               'Category', 'SKU_Tier'] + [f'Sales_{i:02d}' for i in range(1, 12)]
                
                available_cols = min(len(sales_columns), len(self.sales_data.columns))
                self.sales_data.columns = sales_columns[:available_cols] + list(self.sales_data.columns[available_cols:])
                self.sales_data = self.sales_data[~self.sales_data['Current_SKU'].astype(str).str.contains('Current SKU', na=False)]
            
            # Create mapping dictionary
            self.create_mappings()
            
        except Exception as e:
            st.error(f"Error preparing data: {e}")
    
    def create_mappings(self):
        """Membuat mapping dictionaries untuk akses cepat"""
        self.sku_mapping = {}
        if not self.master_data.empty:
            for _, row in self.master_data.iterrows():
                if pd.notna(row['Material']) and pd.notna(row['SKU_SAP']):
                    self.sku_mapping[row['Material']] = row['SKU_SAP']
                if pd.notna(row['OLD_Material']) and pd.notna(row['SKU_SAP']):
                    self.sku_mapping[row['OLD_Material']] = row['SKU_SAP']
    
    def get_sku_info(self, sku):
        """Mendapatkan informasi lengkap untuk sebuah SKU"""
        info = {
            'material': '',
            'old_material': '',
            'sku_sap': '',
            'product_name': '',
            'brand': '',
            'category': '',
            'found': False
        }
        
        if not self.master_data.empty:
            # Cari di master data
            master_match = self.master_data[
                (self.master_data['Material'] == sku) | 
                (self.master_data['OLD_Material'] == sku) |
                (self.master_data['SKU_SAP'] == sku)
            ]
            
            if not master_match.empty:
                info.update({
                    'material': master_match.iloc[0]['Material'],
                    'old_material': master_match.iloc[0]['OLD_Material'],
                    'sku_sap': master_match.iloc[0]['SKU_SAP'],
                    'found': True
                })
                
                # Cari info tambahan di Sales data
                if not self.sales_data.empty:
                    sales_match = self.sales_data[self.sales_data['SKU_SAP'] == info['sku_sap']]
                    if not sales_match.empty:
                        info.update({
                            'product_name': sales_match.iloc[0]['SKU_Name'],
                            'brand': sales_match.iloc[0]['Brand'],
                            'category': sales_match.iloc[0]['Category']
                        })
        
        return info
    
    def calculate_forecast_accuracy(self):
        """Menghitung akurasi forecast vs actual sales"""
        accuracy_data = []
        
        if self.rofo_data.empty or self.sales_data.empty:
            return pd.DataFrame()
        
        for _, rofo_row in self.rofo_data.iterrows():
            sku_sap = rofo_row['SKU_SAP']
            
            # Cari data sales yang sesuai
            sales_match = self.sales_data[self.sales_data['SKU_SAP'] == sku_sap]
            
            if not sales_match.empty:
                sales_row = sales_match.iloc[0]
                
                accuracy_row = {'SKU_SAP': sku_sap, 'Product_Name': rofo_row.get('Product_Name', '')}
                
                # Hitung accuracy per bulan
                for i in range(1, 12):
                    forecast_col = f'Forecast_{i:02d}'
                    sales_col = f'Sales_{i:02d}'
                    
                    if forecast_col in rofo_row and sales_col in sales_row:
                        forecast_val = rofo_row[forecast_col]
                        sales_val = sales_row[sales_col]
                        
                        if pd.notna(forecast_val) and pd.notna(sales_val) and forecast_val != 0:
                            try:
                                accuracy = (1 - abs(sales_val - forecast_val) / forecast_val) * 100
                                accuracy_row[f'Accuracy_{i:02d}'] = max(0, min(100, accuracy))
                            except:
                                accuracy_row[f'Accuracy_{i:02d}'] = None
                        else:
                            accuracy_row[f'Accuracy_{i:02d}'] = None
                    else:
                        accuracy_row[f'Accuracy_{i:02d}'] = None
                
                accuracy_data.append(accuracy_row)
        
        return pd.DataFrame(accuracy_data)
    
    def get_brand_performance(self):
        """Analisis performa per brand"""
        brand_data = []
        
        if self.sales_data.empty:
            return pd.DataFrame()
        
        for _, row in self.sales_data.iterrows():
            if pd.notna(row.get('Brand')) and row.get('Brand') != 'Brand':
                total_sales = 0
                sales_cols = [col for col in self.sales_data.columns if col.startswith('Sales_')]
                for col in sales_cols:
                    if col in row and pd.notna(row[col]):
                        total_sales += row[col]
                
                brand_data.append({
                    'Brand': row['Brand'],
                    'Category': row.get('Category', 'Unknown'),
                    'Total_Sales': total_sales,
                    'SKU_Count': 1
                })
        
        brand_df = pd.DataFrame(brand_data)
        if not brand_df.empty:
            brand_performance = brand_df.groupby(['Brand', 'Category']).agg({
                'Total_Sales': 'sum',
                'SKU_Count': 'count'
            }).reset_index()
            brand_performance['Avg_Sales_Per_SKU'] = brand_performance['Total_Sales'] / brand_performance['SKU_Count']
            return brand_performance.sort_values('Total_Sales', ascending=False)
        
        return pd.DataFrame()
    
    def find_data_issues(self):
        """Mencari issue dalam data"""
        issues = []
        
        if self.sales_data.empty or self.master_data.empty:
            issues.append("⚠️ Data tidak lengkap")
            return issues
        
        # SKU di Sales tapi tidak ada di Master Data
        sales_skus = set(self.sales_data['SKU_SAP'].dropna())
        master_skus = set(self.master_data['SKU_SAP'].dropna())
        missing_master = sales_skus - master_skus
        
        if missing_master:
            issues.append(f"🚨 {len(missing_master)} SKU di Sales tidak ada di Master Data")
        
        # SKU dengan forecast tapi tidak ada sales
        if not self.rofo_data.empty:
            rofo_skus = set(self.rofo_data['SKU_SAP'].dropna())
            no_sales_forecast = rofo_skus - sales_skus
            
            if no_sales_forecast:
                issues.append(f"⚠️ {len(no_sales_forecast)} SKU ada forecast tapi tidak ada sales data")
        
        # SKU dengan sales 0
        sales_cols = [col for col in self.sales_data.columns if col.startswith('Sales_')]
        if sales_cols:
            zero_sales = self.sales_data[sales_cols].sum(axis=1) == 0
            zero_sales_count = zero_sales.sum()
            
            if zero_sales_count > 0:
                issues.append(f"📉 {zero_sales_count} SKU memiliki total sales 0")
        
        return issues

def main():
    st.markdown('<h1 class="main-header">📊 SKU Management Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialize Google Sheets connector
    gs_connector = GoogleSheetsConnector()
    
    if gs_connector.credentials is None:
        st.error("❌ Gagal mengakses Google Sheets. Periksa konfigurasi credentials.")
        return
    
    # URL Google Sheets dari folder yang diberikan
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1nno4Y5thUux03dAGemvF7SQBggqUf8Pp"  # Ganti dengan URL actual
    
    # Load data
    with st.spinner("🔄 Memuat data dari Google Sheets..."):
        try:
            master_data = gs_connector.get_sheet_data(SHEET_URL, "master data")
            rofo_data = gs_connector.get_sheet_data(SHEET_URL, "Rofo")
            sales_data = gs_connector.get_sheet_data(SHEET_URL, "Sales")
            
            if master_data.empty or rofo_data.empty or sales_data.empty:
                st.error("❌ Gagal memuat data. Pastikan sheet URL benar dan memiliki akses.")
                return
            
            # Initialize analyzer
            analyzer = SKUAnalyzer(master_data, rofo_data, sales_data)
            
            st.success(f"✅ Data berhasil dimuat!")
            st.info(f"📊 Summary: {len(master_data)} SKU Master, {len(rofo_data)} Forecast, {len(sales_data)} Sales")
            
        except Exception as e:
            st.error(f"❌ Error memuat data: {e}")
            return
    
    # Sidebar navigation
    st.sidebar.header("🔍 Navigation")
    page = st.sidebar.radio("Pilih Halaman:", 
                          ["📈 Dashboard Overview", "🔍 SKU Search", "📊 Forecast Accuracy", 
                           "🏷️ Brand Analysis", "🚨 Data Issues", "📥 Export Data"])
    
    if page == "📈 Dashboard Overview":
        show_dashboard_overview(analyzer)
    elif page == "🔍 SKU Search":
        show_sku_search(analyzer)
    elif page == "📊 Forecast Accuracy":
        show_forecast_accuracy(analyzer)
    elif page == "🏷️ Brand Analysis":
        show_brand_analysis(analyzer)
    elif page == "🚨 Data Issues":
        show_data_issues(analyzer)
    elif page == "📥 Export Data":
        show_export_data(analyzer)

def show_dashboard_overview(analyzer):
    """Menampilkan dashboard overview"""
    st.header("📈 Dashboard Overview")
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_skus = len(analyzer.master_data) if not analyzer.master_data.empty else 0
        st.metric("Total SKUs", f"{total_skus:,}")
    
    with col2:
        if not analyzer.sales_data.empty:
            active_products = len(analyzer.sales_data[~analyzer.sales_data['Current_SKU'].astype(str).str.contains('Discontinue', na=False)])
        else:
            active_products = 0
        st.metric("Active Products", f"{active_products:,}")
    
    with col3:
        forecast_skus = len(analyzer.rofo_data) if not analyzer.rofo_data.empty else 0
        st.metric("SKUs with Forecast", f"{forecast_skus:,}")
    
    with col4:
        if not analyzer.sales_data.empty:
            sales_cols = [col for col in analyzer.sales_data.columns if col.startswith('Sales_')]
            total_sales = analyzer.sales_data[sales_cols].sum().sum() if sales_cols else 0
        else:
            total_sales = 0
        st.metric("Total Sales", f"${total_sales:,.0f}")
    
    # Data issues warning
    issues = analyzer.find_data_issues()
    if issues:
        st.markdown("---")
        st.subheader("🚨 Data Issues")
        for issue in issues:
            st.warning(issue)
    
    # Quick charts
    if not analyzer.sales_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Sales by Category
            if 'Category' in analyzer.sales_data.columns:
                category_sales = analyzer.sales_data.groupby('Category')[[col for col in analyzer.sales_data.columns 
                                                                    if col.startswith('Sales_')]].sum().sum(axis=1)
                if not category_sales.empty:
                    fig = px.pie(values=category_sales.values, names=category_sales.index, 
                                title="Sales Distribution by Category")
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Top products
            sales_data = analyzer.sales_data.copy()
            sales_cols = [col for col in sales_data.columns if col.startswith('Sales_')]
            if sales_cols:
                sales_data['Total_Sales'] = sales_data[sales_cols].sum(axis=1)
                top_products = sales_data.nlargest(10, 'Total_Sales')[['SKU_Name', 'Total_Sales']]
                
                if not top_products.empty:
                    fig = px.bar(top_products, x='Total_Sales', y='SKU_Name', 
                                orientation='h', title="Top 10 Products by Sales")
                    st.plotly_chart(fig, use_container_width=True)

def show_sku_search(analyzer):
    """Halaman pencarian SKU"""
    st.header("🔍 SKU Search & Analysis")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        search_term = st.text_input("Enter SKU (Material/OLD Material/SKU SAP):")
        
        if search_term:
            sku_info = analyzer.get_sku_info(search_term)
            
            if sku_info['found']:
                st.success("✅ SKU Found!")
                
                st.subheader("SKU Information")
                st.write(f"**Material:** {sku_info['material']}")
                st.write(f"**OLD Material:** {sku_info['old_material']}")
                st.write(f"**SKU SAP:** {sku_info['sku_sap']}")
                st.write(f"**Product Name:** {sku_info['product_name']}")
                st.write(f"**Brand:** {sku_info['brand']}")
                st.write(f"**Category:** {sku_info['category']}")
            else:
                st.error("❌ SKU Not Found")
    
    with col2:
        if search_term and sku_info['found']:
            # Tampilkan sales vs forecast data
            sku_sap = sku_info['sku_sap']
            
            # Get sales data
            if not analyzer.sales_data.empty:
                sales_match = analyzer.sales_data[analyzer.sales_data['SKU_SAP'] == sku_sap]
            else:
                sales_match = pd.DataFrame()
                
            if not analyzer.rofo_data.empty:
                rofo_match = analyzer.rofo_data[analyzer.rofo_data['SKU_SAP'] == sku_sap]
            else:
                rofo_match = pd.DataFrame()
            
            if not sales_match.empty and not rofo_match.empty:
                sales_row = sales_match.iloc[0]
                rofo_row = rofo_match.iloc[0]
                
                # Prepare data for chart
                months = [f'Month {i:02d}' for i in range(1, 12)]
                sales_values = []
                forecast_values = []
                
                for i in range(1, 12):
                    sales_col = f'Sales_{i:02d}'
                    forecast_col = f'Forecast_{i:02d}'
                    
                    sales_val = sales_row[sales_col] if sales_col in sales_row else 0
                    forecast_val = rofo_row[forecast_col] if forecast_col in rofo_row else 0
                    
                    sales_values.append(sales_val)
                    forecast_values.append(forecast_val)
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=months, y=sales_values, name='Actual Sales', line=dict(color='blue')))
                fig.add_trace(go.Scatter(x=months, y=forecast_values, name='Forecast', line=dict(color='red', dash='dash')))
                
                fig.update_layout(title=f"Sales vs Forecast - {sku_info['product_name']}",
                                xaxis_title="Month",
                                yaxis_title="Quantity")
                
                st.plotly_chart(fig, use_container_width=True)

def show_forecast_accuracy(analyzer):
    """Halaman analisis akurasi forecast"""
    st.header("📊 Forecast Accuracy Analysis")
    
    # Calculate accuracy
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if not accuracy_df.empty:
        # Overall accuracy
        accuracy_columns = [col for col in accuracy_df.columns if col.startswith('Accuracy_')]
        overall_accuracy = accuracy_df[accuracy_columns].mean().mean()
        
        st.metric("Overall Forecast Accuracy", f"{overall_accuracy:.1f}%")
        
        # Accuracy by month
        monthly_accuracy = accuracy_df[accuracy_columns].mean()
        
        fig = px.line(x=range(1, 12), y=monthly_accuracy.values,
                     labels={'x': 'Month', 'y': 'Accuracy %'},
                     title="Forecast Accuracy by Month")
        st.plotly_chart(fig, use_container_width=True)
        
        # Accuracy distribution
        fig = px.histogram(accuracy_df[accuracy_columns].mean(axis=1),
                          nbins=20, title="Distribution of SKU Accuracy")
        st.plotly_chart(fig, use_container_width=True)
        
        # Low accuracy SKUs
        accuracy_df['Avg_Accuracy'] = accuracy_df[accuracy_columns].mean(axis=1)
        low_accuracy = accuracy_df[accuracy_df['Avg_Accuracy'] < 70]
        
        if not low_accuracy.empty:
            st.subheader("🚨 SKUs with Low Accuracy (<70%)")
            st.dataframe(low_accuracy[['SKU_SAP', 'Product_Name', 'Avg_Accuracy']].sort_values('Avg_Accuracy'))
    
    else:
        st.warning("No accuracy data available")

def show_brand_analysis(analyzer):
    """Halaman analisis brand"""
    st.header("🏷️ Brand Performance Analysis")
    
    brand_performance = analyzer.get_brand_performance()
    
    if not brand_performance.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Sales by brand
            fig = px.bar(brand_performance, x='Brand', y='Total_Sales',
                        title="Total Sales by Brand")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # SKU count by brand
            fig = px.pie(brand_performance, values='SKU_Count', names='Brand',
                        title="SKU Distribution by Brand")
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed table
        st.subheader("Brand Performance Details")
        st.dataframe(brand_performance)
    
    else:
        st.warning("No brand performance data available")

def show_data_issues(analyzer):
    """Halaman issues data"""
    st.header("🚨 Data Quality Issues")
    
    issues = analyzer.find_data_issues()
    
    if issues:
        for issue in issues:
            st.error(issue)
        
        # Detailed analysis
        if not analyzer.sales_data.empty and not analyzer.master_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Missing Master Data Mappings")
                sales_skus = set(analyzer.sales_data['SKU_SAP'].dropna())
                master_skus = set(analyzer.master_data['SKU_SAP'].dropna())
                missing_master = sales_skus - master_skus
                
                if missing_master:
                    missing_df = analyzer.sales_data[analyzer.sales_data['SKU_SAP'].isin(missing_master)]
                    st.dataframe(missing_df[['Current_SKU', 'SKU_SAP', 'SKU_Name']].head(10))
            
            with col2:
                st.subheader("SKUs with No Sales")
                sales_cols = [col for col in analyzer.sales_data.columns if col.startswith('Sales_')]
                if sales_cols:
                    zero_sales = analyzer.sales_data[sales_cols].sum(axis=1) == 0
                    if zero_sales.any():
                        zero_sales_df = analyzer.sales_data[zero_sales]
                        st.dataframe(zero_sales_df[['Current_SKU', 'SKU_SAP', 'SKU_Name']].head(10))
    
    else:
        st.success("✅ No major data issues found!")

def show_export_data(analyzer):
    """Halaman export data"""
    st.header("📥 Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📋 Export Master Data"):
            csv = analyzer.master_data.to_csv(index=False)
            st.download_button(
                label="Download Master Data CSV",
                data=csv,
                file_name="master_data.csv",
                mime="text/csv"
            )
    
    with col2:
        if st.button("📈 Export Sales Data"):
            csv = analyzer.sales_data.to_csv(index=False)
            st.download_button(
                label="Download Sales Data CSV",
                data=csv,
                file_name="sales_data.csv",
                mime="text/csv"
            )
    
    with col3:
        if st.button("🎯 Export Accuracy Report"):
            accuracy_df = analyzer.calculate_forecast_accuracy()
            if not accuracy_df.empty:
                csv = accuracy_df.to_csv(index=False)
                st.download_button(
                    label="Download Accuracy Report CSV",
                    data=csv,
                    file_name="accuracy_report.csv",
                    mime="text/csv"
                )

if __name__ == "__main__":
    main()
