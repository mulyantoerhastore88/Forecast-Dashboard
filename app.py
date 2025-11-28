import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Konfigurasi halaman
st.set_page_config(
    page_title="Forecast Accuracy Dashboard",
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
        margin-bottom: 1rem;
    }
    .warning-card {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #ffc107;
        margin-bottom: 1rem;
    }
    .success-card {
        background-color: #d1ecf1;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #0c5460;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

class ForecastAccuracyAnalyzer:
    def __init__(self, rofo_data, sales_data, po_data=None):
        self.rofo_data = rofo_data
        self.sales_data = sales_data
        self.po_data = po_data
        self.prepare_data()
    
    def prepare_data(self):
        """Mempersiapkan dan membersihkan data untuk analisis forecast accuracy"""
        try:
            # Clean Rofo data (Forecast)
            if not self.rofo_data.empty:
                # Remove metadata rows and clean column names
                self.rofo_data = self.rofo_data.iloc[1:]  # Remove header row
                self.rofo_data.columns = ['SKU_GOA', 'SKU_SAP', 'Product_Name', 'Brand'] + [f'Forecast_{i:02d}' for i in range(1, 13)]
                
                # Convert forecast columns to numeric
                for col in self.rofo_data.columns[4:]:
                    self.rofo_data[col] = pd.to_numeric(self.rofo_data[col], errors='coerce')
            
            # Clean Sales data
            if not self.sales_data.empty:
                # Remove metadata rows and clean column names
                self.sales_data = self.sales_data.iloc[1:]  # Remove header row
                self.sales_data.columns = ['Current_SKU', 'SKU_SAP', 'SKU_Old', 'SKU_Name', 'Brand', 
                                         'Category', 'SKU_Tier'] + [f'Sales_{i:02d}' for i in range(1, 13)]
                
                # Convert sales columns to numeric
                for col in self.sales_data.columns[7:]:
                    self.sales_data[col] = pd.to_numeric(self.sales_data[col], errors='coerce')
            
            # Clean PO data if available
            if self.po_data is not None and not self.po_data.empty:
                self.po_data = self.po_data.iloc[1:]  # Remove header row
                self.po_data.columns = ['SKU_SAP'] + [f'PO_{i:02d}' for i in range(1, 12)]
                
                # Convert PO columns to numeric
                for col in self.po_data.columns[1:]:
                    self.po_data[col] = pd.to_numeric(self.po_data[col], errors='coerce')
            
        except Exception as e:
            st.error(f"Error preparing data: {e}")
    
    def calculate_forecast_accuracy(self):
        """Menghitung akurasi forecast vs actual sales dengan berbagai metrik"""
        accuracy_data = []
        
        if self.rofo_data.empty or self.sales_data.empty:
            return pd.DataFrame()
        
        for _, rofo_row in self.rofo_data.iterrows():
            sku_sap = rofo_row['SKU_SAP']
            
            # Cari data sales yang sesuai
            sales_match = self.sales_data[self.sales_data['SKU_SAP'] == sku_sap]
            
            if not sales_match.empty:
                sales_row = sales_match.iloc[0]
                
                accuracy_row = {
                    'SKU_SAP': sku_sap, 
                    'Product_Name': rofo_row.get('Product_Name', ''),
                    'Brand': rofo_row.get('Brand', ''),
                    'Total_Forecast': 0,
                    'Total_Sales': 0,
                    'Total_Absolute_Error': 0,
                    'Months_With_Data': 0
                }
                
                monthly_accuracy = []
                
                # Hitung accuracy per bulan
                for i in range(1, 12):  # 11 bulan sesuai data
                    forecast_col = f'Forecast_{i:02d}'
                    sales_col = f'Sales_{i:02d}'
                    
                    if forecast_col in rofo_row and sales_col in sales_row:
                        forecast_val = rofo_row[forecast_col] if pd.notna(rofo_row[forecast_col]) else 0
                        sales_val = sales_row[sales_col] if pd.notna(sales_row[sales_col]) else 0
                        
                        # Skip jika kedua nilai 0
                        if forecast_val == 0 and sales_val == 0:
                            continue
                            
                        accuracy_row['Total_Forecast'] += forecast_val
                        accuracy_row['Total_Sales'] += sales_val
                        accuracy_row['Total_Absolute_Error'] += abs(sales_val - forecast_val)
                        accuracy_row['Months_With_Data'] += 1
                        
                        # Hitung accuracy untuk bulan ini
                        if pd.notna(forecast_val) and pd.notna(sales_val) and forecast_val != 0:
                            accuracy_pct = (1 - abs(sales_val - forecast_val) / forecast_val) * 100
                            bias_pct = ((sales_val - forecast_val) / forecast_val) * 100 if forecast_val != 0 else 0
                        else:
                            accuracy_pct = None
                            bias_pct = None
                        
                        monthly_accuracy.append({
                            'Month': i,
                            'Forecast': forecast_val,
                            'Sales': sales_val,
                            'Accuracy_Pct': accuracy_pct,
                            'Bias_Pct': bias_pct,
                            'Absolute_Error': abs(sales_val - forecast_val)
                        })
                
                # Hitung metrik agregat
                if accuracy_row['Months_With_Data'] > 0:
                    if accuracy_row['Total_Forecast'] > 0:
                        accuracy_row['Overall_Accuracy'] = (1 - accuracy_row['Total_Absolute_Error'] / accuracy_row['Total_Forecast']) * 100
                        accuracy_row['Bias_Pct'] = ((accuracy_row['Total_Sales'] - accuracy_row['Total_Forecast']) / accuracy_row['Total_Forecast']) * 100
                    else:
                        accuracy_row['Overall_Accuracy'] = 0
                        accuracy_row['Bias_Pct'] = 0
                    
                    accuracy_row['MAPE'] = (accuracy_row['Total_Absolute_Error'] / accuracy_row['Total_Sales']) * 100 if accuracy_row['Total_Sales'] > 0 else 0
                    accuracy_row['MAE'] = accuracy_row['Total_Absolute_Error'] / accuracy_row['Months_With_Data']
                    
                    # Klasifikasi accuracy
                    if accuracy_row['Overall_Accuracy'] >= 90:
                        accuracy_row['Accuracy_Level'] = 'Excellent'
                    elif accuracy_row['Overall_Accuracy'] >= 80:
                        accuracy_row['Accuracy_Level'] = 'Good'
                    elif accuracy_row['Overall_Accuracy'] >= 70:
                        accuracy_row['Accuracy_Level'] = 'Fair'
                    else:
                        accuracy_row['Accuracy_Level'] = 'Poor'
                    
                    accuracy_row['Monthly_Details'] = monthly_accuracy
                    accuracy_data.append(accuracy_row)
        
        return pd.DataFrame(accuracy_data)
    
    def get_brand_accuracy_summary(self, accuracy_df):
        """Ringkasan akurasi per brand"""
        if accuracy_df.empty:
            return pd.DataFrame()
        
        brand_summary = accuracy_df.groupby('Brand').agg({
            'Overall_Accuracy': 'mean',
            'Bias_Pct': 'mean',
            'MAPE': 'mean',
            'SKU_SAP': 'count',
            'Total_Forecast': 'sum',
            'Total_Sales': 'sum'
        }).reset_index()
        
        brand_summary.columns = ['Brand', 'Avg_Accuracy', 'Avg_Bias', 'Avg_MAPE', 'SKU_Count', 'Total_Forecast', 'Total_Sales']
        brand_summary['Accuracy_Level'] = brand_summary['Avg_Accuracy'].apply(
            lambda x: 'Excellent' if x >= 90 else 'Good' if x >= 80 else 'Fair' if x >= 70 else 'Poor'
        )
        
        return brand_summary.sort_values('Avg_Accuracy', ascending=False)
    
    def get_monthly_accuracy_trend(self, accuracy_df):
        """Trend akurasi bulanan agregat"""
        if accuracy_df.empty:
            return pd.DataFrame()
        
        monthly_data = []
        for _, row in accuracy_df.iterrows():
            for month_data in row['Monthly_Details']:
                monthly_data.append({
                    'Month': month_data['Month'],
                    'Accuracy_Pct': month_data['Accuracy_Pct'] if month_data['Accuracy_Pct'] is not None else 0,
                    'Brand': row['Brand']
                })
        
        monthly_df = pd.DataFrame(monthly_data)
        monthly_summary = monthly_df.groupby('Month')['Accuracy_Pct'].agg(['mean', 'std', 'count']).reset_index()
        monthly_summary.columns = ['Month', 'Avg_Accuracy', 'Std_Deviation', 'Data_Points']
        
        return monthly_summary
    
    def identify_forecast_issues(self, accuracy_df):
        """Mengidentifikasi issue dalam forecasting"""
        issues = []
        
        if accuracy_df.empty:
            return issues
        
        # SKU dengan accuracy rendah
        low_accuracy = accuracy_df[accuracy_df['Overall_Accuracy'] < 70]
        if len(low_accuracy) > 0:
            issues.append(f"🚨 {len(low_accuracy)} SKU memiliki accuracy di bawah 70%")
        
        # SKU dengan bias tinggi (over-forecast)
        high_over_forecast = accuracy_df[accuracy_df['Bias_Pct'] < -20]
        if len(high_over_forecast) > 0:
            issues.append(f"📈 {len(high_over_forecast)} SKU over-forecast lebih dari 20%")
        
        # SKU dengan bias tinggi (under-forecast)
        high_under_forecast = accuracy_df[accuracy_df['Bias_Pct'] > 20]
        if len(high_under_forecast) > 0:
            issues.append(f"📉 {len(high_under_forecast)} SKU under-forecast lebih dari 20%")
        
        # SKU tanpa data sales
        no_sales_forecast = self.rofo_data[~self.rofo_data['SKU_SAP'].isin(self.sales_data['SKU_SAP'])]
        if len(no_sales_forecast) > 0:
            issues.append(f"⚠️ {len(no_sales_forecast)} SKU memiliki forecast tapi tidak ada data sales")
        
        return issues

def main():
    st.markdown('<h1 class="main-header">📊 Forecast Accuracy Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data langsung dari file yang diberikan
    try:
        with st.spinner("🔄 Memuat data dari file..."):
            # Gunakan data yang sudah disediakan
            rofo_data = pd.read_excel('Data (11).xlsx', sheet_name='Rofo(Forecast)')
            sales_data = pd.read_excel('Data (11).xlsx', sheet_name='Sales')
            po_data = pd.read_excel('Data (11).xlsx', sheet_name='PO')
            
            # Initialize analyzer
            analyzer = ForecastAccuracyAnalyzer(rofo_data, sales_data, po_data)
            
            st.success(f"✅ Data berhasil dimuat!")
            st.info(f"📊 Summary: {len(rofo_data)} Forecast SKUs, {len(sales_data)} Sales SKUs")
                
    except Exception as e:
        st.error(f"❌ Error memuat data: {str(e)}")
        return
    
    # Navigation
    st.sidebar.header("🔍 Navigation")
    page = st.sidebar.radio("Pilih Halaman:", 
                          ["📈 Dashboard Overview", "📊 Forecast Accuracy", "🏷️ Brand Analysis", 
                           "📈 Monthly Trends", "🚨 Issue Analysis", "📥 Export Reports"])
    
    if page == "📈 Dashboard Overview":
        show_dashboard_overview(analyzer)
    elif page == "📊 Forecast Accuracy":
        show_forecast_accuracy(analyzer)
    elif page == "🏷️ Brand Analysis":
        show_brand_analysis(analyzer)
    elif page == "📈 Monthly Trends":
        show_monthly_trends(analyzer)
    elif page == "🚨 Issue Analysis":
        show_issue_analysis(analyzer)
    elif page == "📥 Export Reports":
        show_export_reports(analyzer)

def show_dashboard_overview(analyzer):
    """Menampilkan dashboard overview"""
    st.header("📈 Forecast Accuracy Dashboard Overview")
    
    # Calculate accuracy data
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if accuracy_df.empty:
        st.warning("Tidak ada data accuracy yang dapat dihitung")
        return
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        overall_accuracy = accuracy_df['Overall_Accuracy'].mean()
        st.metric("Overall Accuracy", f"{overall_accuracy:.1f}%")
    
    with col2:
        avg_bias = accuracy_df['Bias_Pct'].mean()
        bias_label = "Over-Forecast" if avg_bias < 0 else "Under-Forecast"
        st.metric("Average Bias", f"{abs(avg_bias):.1f}%", bias_label)
    
    with col3:
        excellent_accuracy = len(accuracy_df[accuracy_df['Overall_Accuracy'] >= 90])
        st.metric("Excellent Accuracy (≥90%)", f"{excellent_accuracy} SKUs")
    
    with col4:
        poor_accuracy = len(accuracy_df[accuracy_df['Overall_Accuracy'] < 70])
        st.metric("Poor Accuracy (<70%)", f"{poor_accuracy} SKUs")
    
    # Accuracy Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Accuracy Distribution")
        accuracy_levels = accuracy_df['Accuracy_Level'].value_counts()
        fig = px.pie(values=accuracy_levels.values, names=accuracy_levels.index,
                    title="Distribution of Forecast Accuracy Levels")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Accuracy vs Sales Volume")
        fig = px.scatter(accuracy_df, x='Total_Sales', y='Overall_Accuracy', 
                        color='Accuracy_Level', hover_data=['Product_Name'],
                        title="Accuracy vs Sales Volume")
        st.plotly_chart(fig, use_container_width=True)
    
    # Recent Issues
    st.subheader("🚨 Recent Forecast Issues")
    issues = analyzer.identify_forecast_issues(accuracy_df)
    for issue in issues:
        st.warning(issue)

def show_forecast_accuracy(analyzer):
    """Halaman detail forecast accuracy"""
    st.header("📊 Detailed Forecast Accuracy Analysis")
    
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if accuracy_df.empty:
        st.warning("Tidak ada data accuracy yang dapat dihitung")
        return
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        accuracy_filter = st.selectbox("Filter by Accuracy Level:", 
                                     ["All", "Excellent (≥90%)", "Good (80-89%)", "Fair (70-79%)", "Poor (<70%)"])
    
    with col2:
        brands = ["All"] + sorted(accuracy_df['Brand'].unique().tolist())
        brand_filter = st.selectbox("Filter by Brand:", brands)
    
    with col3:
        min_sales = st.number_input("Minimum Sales Volume:", min_value=0, value=0)
    
    # Apply filters
    filtered_df = accuracy_df.copy()
    
    if accuracy_filter != "All":
        if accuracy_filter == "Excellent (≥90%)":
            filtered_df = filtered_df[filtered_df['Overall_Accuracy'] >= 90]
        elif accuracy_filter == "Good (80-89%)":
            filtered_df = filtered_df[(filtered_df['Overall_Accuracy'] >= 80) & (filtered_df['Overall_Accuracy'] < 90)]
        elif accuracy_filter == "Fair (70-79%)":
            filtered_df = filtered_df[(filtered_df['Overall_Accuracy'] >= 70) & (filtered_df['Overall_Accuracy'] < 80)]
        else:  # Poor
            filtered_df = filtered_df[filtered_df['Overall_Accuracy'] < 70]
    
    if brand_filter != "All":
        filtered_df = filtered_df[filtered_df['Brand'] == brand_filter]
    
    if min_sales > 0:
        filtered_df = filtered_df[filtered_df['Total_Sales'] >= min_sales]
    
    # Display results
    st.subheader(f"Forecast Accuracy Results ({len(filtered_df)} SKUs)")
    
    # Summary metrics for filtered data
    if len(filtered_df) > 0:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_accuracy = filtered_df['Overall_Accuracy'].mean()
            st.metric("Average Accuracy", f"{avg_accuracy:.1f}%")
        
        with col2:
            avg_bias = filtered_df['Bias_Pct'].mean()
            st.metric("Average Bias", f"{avg_bias:.1f}%")
        
        with col3:
            total_forecast = filtered_df['Total_Forecast'].sum()
            st.metric("Total Forecast Volume", f"{total_forecast:,.0f}")
    
    # Detailed table
    display_cols = ['SKU_SAP', 'Product_Name', 'Brand', 'Overall_Accuracy', 'Bias_Pct', 
                   'Accuracy_Level', 'Total_Forecast', 'Total_Sales']
    
    st.dataframe(filtered_df[display_cols].sort_values('Overall_Accuracy', ascending=False),
                use_container_width=True)

def show_brand_analysis(analyzer):
    """Halaman analisis brand"""
    st.header("🏷️ Brand Performance Analysis")
    
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if accuracy_df.empty:
        st.warning("Tidak ada data accuracy yang dapat dihitung")
        return
    
    brand_summary = analyzer.get_brand_accuracy_summary(accuracy_df)
    
    if brand_summary.empty:
        st.warning("Tidak ada data brand summary")
        return
    
    # Brand Performance Metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Brand Accuracy Ranking")
        fig = px.bar(brand_summary, x='Avg_Accuracy', y='Brand', orientation='h',
                    title="Average Forecast Accuracy by Brand",
                    color='Avg_Accuracy', color_continuous_scale='RdYlGn')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Accuracy vs Sales Volume by Brand")
        fig = px.scatter(brand_summary, x='Total_Sales', y='Avg_Accuracy', size='SKU_Count',
                        color='Brand', hover_data=['Avg_Bias'],
                        title="Brand Performance: Accuracy vs Sales Volume")
        st.plotly_chart(fig, use_container_width=True)
    
    # Detailed Brand Table
    st.subheader("Detailed Brand Performance")
    st.dataframe(brand_summary, use_container_width=True)

def show_monthly_trends(analyzer):
    """Halaman trend bulanan"""
    st.header("📈 Monthly Accuracy Trends")
    
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if accuracy_df.empty:
        st.warning("Tidak ada data accuracy yang dapat dihitung")
        return
    
    monthly_trend = analyzer.get_monthly_accuracy_trend(accuracy_df)
    
    if monthly_trend.empty:
        st.warning("Tidak ada data trend bulanan")
        return
    
    # Monthly Accuracy Trend
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Monthly Accuracy Trend")
        fig = px.line(monthly_trend, x='Month', y='Avg_Accuracy',
                     title="Average Monthly Forecast Accuracy Trend")
        fig.add_scatter(x=monthly_trend['Month'], y=monthly_trend['Avg_Accuracy'], 
                       mode='lines+markers', name='Accuracy')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Accuracy Variability by Month")
        fig = px.bar(monthly_trend, x='Month', y='Std_Deviation',
                    title="Accuracy Standard Deviation by Month")
        st.plotly_chart(fig, use_container_width=True)
    
    # Monthly details table
    st.subheader("Monthly Accuracy Details")
    st.dataframe(monthly_trend, use_container_width=True)

def show_issue_analysis(analyzer):
    """Halaman analisis issue"""
    st.header("🚨 Forecast Issue Analysis")
    
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if accuracy_df.empty:
        st.warning("Tidak ada data accuracy yang dapat dihitung")
        return
    
    issues = analyzer.identify_forecast_issues(accuracy_df)
    
    # Display issues
    for issue in issues:
        st.error(issue)
    
    # Detailed issue analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("SKUs with Poor Accuracy (<70%)")
        poor_accuracy = accuracy_df[accuracy_df['Overall_Accuracy'] < 70]
        if not poor_accuracy.empty:
            st.dataframe(poor_accuracy[['SKU_SAP', 'Product_Name', 'Brand', 'Overall_Accuracy', 'Bias_Pct']]
                        .sort_values('Overall_Accuracy'), use_container_width=True)
        else:
            st.success("✅ No SKUs with poor accuracy!")
    
    with col2:
        st.subheader("High Bias SKUs (>|20%|)")
        high_bias = accuracy_df[abs(accuracy_df['Bias_Pct']) > 20]
        if not high_bias.empty:
            st.dataframe(high_bias[['SKU_SAP', 'Product_Name', 'Brand', 'Overall_Accuracy', 'Bias_Pct']]
                        .sort_values('Bias_Pct'), use_container_width=True)
        else:
            st.success("✅ No SKUs with high bias!")
    
    # Root cause analysis
    st.subheader("📋 Root Cause Analysis Suggestions")
    
    st.markdown("""
    **Common causes of poor forecast accuracy:**
    - 🎯 **Data Quality Issues**: Missing historical data, outliers
    - 📊 **Seasonality**: Unaccounted seasonal patterns
    - 🚀 **New Products**: Lack of historical data for new SKUs
    - 📈 **Promotional Effects**: Unplanned promotions affecting sales
    - 🏭 **Supply Chain Issues**: Stockouts or overstock situations
    - 🔄 **Model Limitations**: Forecasting model not capturing trends
    """)

def show_export_reports(analyzer):
    """Halaman export laporan"""
    st.header("📥 Export Forecast Accuracy Reports")
    
    accuracy_df = analyzer.calculate_forecast_accuracy()
    
    if accuracy_df.empty:
        st.warning("Tidak ada data accuracy yang dapat dihitung")
        return
    
    st.info("Download laporan forecast accuracy dalam berbagai format")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Export accuracy summary
        summary_cols = ['SKU_SAP', 'Product_Name', 'Brand', 'Overall_Accuracy', 'Bias_Pct', 
                       'Accuracy_Level', 'Total_Forecast', 'Total_Sales']
        summary_df = accuracy_df[summary_cols]
        
        csv = summary_df.to_csv(index=False)
        st.download_button(
            label="📊 Download Accuracy Summary",
            data=csv,
            file_name="forecast_accuracy_summary.csv",
            mime="text/csv"
        )
    
    with col2:
        # Export brand performance
        brand_summary = analyzer.get_brand_accuracy_summary(accuracy_df)
        if not brand_summary.empty:
            csv = brand_summary.to_csv(index=False)
            st.download_button(
                label="🏷️ Download Brand Performance",
                data=csv,
                file_name="brand_accuracy_performance.csv",
                mime="text/csv"
            )
    
    with col3:
        # Export issues report
        poor_accuracy = accuracy_df[accuracy_df['Overall_Accuracy'] < 70]
        if not poor_accuracy.empty:
            csv = poor_accuracy[summary_cols].to_csv(index=False)
            st.download_button(
                label="🚨 Download Issues Report",
                data=csv,
                file_name="forecast_accuracy_issues.csv",
                mime="text/csv"
            )
    
    # Preview of data to be exported
    st.subheader("Data Preview")
    st.dataframe(accuracy_df[['SKU_SAP', 'Product_Name', 'Brand', 'Overall_Accuracy', 'Accuracy_Level']].head(10))

if __name__ == "__main__":
    main()
