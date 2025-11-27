import streamlit as st
import pandas as pd
import gspread
import numpy as np
import altair as alt

# -------------------------------------------------------------------
# 1. KONFIGURASI KONEKSI GOOGLE SHEETS
# -------------------------------------------------------------------

# Link Single Google Sheet
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1PuoII49N-IWOaNO8fSMYGwuvFf1T68_Kez30WN9q8Ds/edit"

# PENTING: Pastikan Nama Tab di Google Sheet Anda sesuai dengan ini!
SHEET_NAME_ROFO = "Rofo"   # Nama tab data Forecast
SHEET_NAME_PO = "PO"       # Nama tab data PO
SHEET_NAME_SALES = "Sales" # Nama tab data Sales

@st.cache_data(ttl=3600)
def get_service_account():
    """Load Service Account dari secrets.toml"""
    creds = st.secrets["gcp_service_account"]
    gc = gspread.service_account_from_dict(dict(creds))
    return gc

@st.cache_data(ttl=3600)
def load_data_from_gsheet(url: str, sheet_name: str):
    """Load data dari Google Sheets berdasarkan Nama Tab."""
    gc = get_service_account()
    try:
        sh = gc.open_by_url(url)
        worksheet = sh.worksheet(sheet_name)
        data = worksheet.get_all_values()
        # Header ada di baris pertama
        df = pd.DataFrame(data[1:], columns=data[0])
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Tab '{sheet_name}' tidak ditemukan! Pastikan nama tab di Google Sheet sama persis.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error load sheet: {sheet_name} | Error: {e}")
        return pd.DataFrame()

# -------------------------------------------------------------------
# 2. PROSES DATA (TRANSFORMASI & MERGE)
# -------------------------------------------------------------------

@st.cache_data
def process_data(df_rofo, df_po):
    
    # --- 2.1. ROFO Processing (Wide to Long) ---
    # Acuan: SKU SAP
    # Kolom tanggal diidentifikasi yang mengandung '202' (misal: 2025-01-25)
    rofo_date_cols = [col for col in df_rofo.columns if '202' in col]
    
    # Kolom ID selain tanggal (SKU SAP wajib ada)
    rofo_id_cols = [col for col in df_rofo.columns if col not in rofo_date_cols]

    # Melt menjadi format panjang
    df_rofo_long = df_rofo.melt(
        id_vars=rofo_id_cols,
        value_vars=rofo_date_cols,
        var_name="Date_Raw",
        value_name="ROFO Quantity"
    )

    # Standardisasi Tanggal (ambil Bulannya saja)
    df_rofo_long["Date"] = pd.to_datetime(df_rofo_long["Date_Raw"], errors="coerce").dt.to_period("M")
    
    # Bersihkan Quantity
    df_rofo_long["ROFO Quantity"] = pd.to_numeric(df_rofo_long["ROFO Quantity"], errors="coerce").fillna(0)
    
    # Grouping jaga-jaga jika ada duplikasi baris per bulan
    df_rofo_final = df_rofo_long.groupby(["SKU SAP", "Date"])["ROFO Quantity"].sum().reset_index()

    # --- 2.2. PO Processing (Transactional) ---
    # Acuan: SKU SAP & Document Date
    
    # Konversi Document Date ke Bulan
    df_po["Date"] = pd.to_datetime(df_po["Document Date"], errors="coerce").dt.to_period("M")
    
    # Konversi Quantity (Gunakan kolom 'Quantity' atau 'Order Quantity' sesuai data real)
    # Di sini saya gunakan 'Quantity' sesuai snippet CSV terakhir, ubah jika perlu.
    col_qty_po = "Quantity" if "Quantity" in df_po.columns else "Order Quantity"
    df_po["Actual Quantity"] = pd.to_numeric(df_po[col_qty_po], errors="coerce").fillna(0)

    # Agregasi PO per SKU SAP per Bulan
    df_po_final = df_po.groupby(["SKU SAP", "Date"])["Actual Quantity"].sum().reset_index()

    # --- 2.3. MERGING (Full Outer Join) ---
    df_merged = pd.merge(
        df_rofo_final,
        df_po_final,
        on=["SKU SAP", "Date"],
        how="outer"
    ).fillna(0)

    # Filter: Hanya ambil yang ada angkanya (biar tidak penuh dengan 0)
    df_merged = df_merged[(df_merged["ROFO Quantity"] > 0) | (df_merged["Actual Quantity"] > 0)]
    
    # Ubah Date kembali ke string untuk visualisasi Altair
    df_merged["Date"] = df_merged["Date"].astype(str)

    # --- 2.4. Hitung Metrik (FAR, Bias, Accuracy) ---
    def calculate_far(row):
        rofo = row["ROFO Quantity"]
        actual = row["Actual Quantity"]
        if rofo == 0:
            return 10.0 if actual > 0 else 1.0 # Flagging infinite/high FAR
        return actual / rofo

    df_merged["FAR"] = df_merged.apply(calculate_far, axis=1)
    df_merged["Bias"] = df_merged["ROFO Quantity"] - df_merged["Actual Quantity"]
    
    # Logic Accuracy: 80% - 120%
    df_merged["Accuracy Status"] = np.where(
        (df_merged["FAR"] >= 0.8) & (df_merged["FAR"] <= 1.2),
        "Accurate", "Non-Accurate"
    )

    return df_merged

# -------------------------------------------------------------------
# 3. DASHBOARD VISUALIZATION
# -------------------------------------------------------------------

def create_dashboard(df, df_sales):
    
    st.markdown("### 🔍 Filter Dashboard")
    
    # Filter SKU (Sort Alphabetical)
    sku_list = sorted(df["SKU SAP"].unique().astype(str))
    all_sku = ["All"] + sku_list
    selected_sku = st.selectbox("Pilih SKU SAP:", all_sku)

    # Filter Dataframe
    if selected_sku != "All":
        df_filtered = df[df["SKU SAP"] == selected_sku]
        df_sales_filtered = df_sales[df_sales["SKU SAP"] == selected_sku]
    else:
        df_filtered = df
        df_sales_filtered = df_sales

    if df_filtered.empty:
        st.warning("⚠️ Data tidak ditemukan untuk filter ini.")
        return

    # --- KPI CARDS ---
    st.markdown("#### 📊 Performa Utama")
    
    total_rofo = df_filtered["ROFO Quantity"].sum()
    total_actual = df_filtered["Actual Quantity"].sum() # Ini dari PO
    total_bias = total_rofo - total_actual
    
    # Overall FAR
    overall_far = (total_actual / total_rofo) if total_rofo > 0 else 0
    
    # Accuracy Count Ratio
    acc_count = df_filtered[df_filtered["Accuracy Status"] == "Accurate"].shape[0]
    total_rows = df_filtered.shape[0]
    acc_ratio = acc_count / total_rows if total_rows > 0 else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Overall FAR (PO vs Rofo)", f"{overall_far:.1%}", "Target: 80-120%", delta_color="off")
    c2.metric("Accuracy Rate (by Month)", f"{acc_ratio:.1%}", f"{acc_count} / {total_rows} Periods", delta_color="off")
    c3.metric("Total Bias (Unit)", f"{total_bias:,.0f}", "Positif = Overforecast", delta_color="inverse")

    st.divider()

    # --- CHARTS ---
    c_chart1, c_chart2 = st.columns([2, 1])

    with c_chart1:
        st.subheader("📈 Tren: Rofo vs PO Submitted")
        # Siapkan data untuk Altair
        chart_data = df_filtered.groupby("Date")[["ROFO Quantity", "Actual Quantity"]].sum().reset_index()
        chart_data = chart_data.melt("Date", var_name="Type", value_name="Qty")
        
        chart = alt.Chart(chart_data).mark_line(point=True).encode(
            x=alt.X("Date:O", title="Bulan"),
            y=alt.Y("Qty:Q", title="Quantity"),
            color=alt.Color("Type", scale=alt.Scale(domain=["ROFO Quantity", "Actual Quantity"], range=["#29b5e8", "#ff7f0e"])),
            tooltip=["Date", "Type", alt.Tooltip("Qty", format=",")]
        ).properties(height=350).interactive()
        
        st.altair_chart(chart, use_container_width=True)

    with c_chart2:
        st.subheader("🏆 Top SKU Bias")
        if selected_sku == "All":
            # Top 10 SKU dengan Bias Tertinggi (Absolut)
            bias_data = df.groupby("SKU SAP")["Bias"].sum().reset_index()
            bias_data["AbsBias"] = bias_data["Bias"].abs()
            bias_data = bias_data.sort_values("AbsBias", ascending=False).head(10)
            
            bar_chart = alt.Chart(bias_data).mark_bar().encode(
                x=alt.X("Bias:Q", title="Total Bias"),
                y=alt.Y("SKU SAP:N", sort="-x"),
                color=alt.condition(alt.datum.Bias > 0, alt.value("#d62728"), alt.value("#2ca02c")),
                tooltip=["SKU SAP", "Bias"]
            ).properties(height=350)
            st.altair_chart(bar_chart, use_container_width=True)
        else:
            st.info("Pilih 'All' pada filter SKU untuk melihat perbandingan antar SKU.")

    # --- DATA TABLES ---
    t1, t2 = st.tabs(["📋 Detail Forecast vs PO", "📦 Data Sales (Ref)"])
    
    with t1:
        st.dataframe(df_filtered.sort_values(["Date", "SKU SAP"]), use_container_width=True)
        
    with t2:
        st.caption("Data Sales (hanya sebagai referensi/pembanding tambahan)")
        # Bersihkan data sales untuk tampilan
        if not df_sales_filtered.empty:
            st.dataframe(df_sales_filtered.head(100), use_container_width=True)
        else:
            st.write("Tidak ada data sales untuk SKU ini.")

# -------------------------------------------------------------------
# 4. MAIN APP
# -------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Forecast Dashboard", layout="wide")
    st.title("📊 Forecast Accuracy Dashboard (Single Source)")
    st.markdown(f"Mengambil data dari: [Google Sheet]({SPREADSHEET_URL})")

    # Load Data
    with st.spinner("Sedang menarik data dari Google Sheet..."):
        df_rofo = load_data_from_gsheet(SPREADSHEET_URL, SHEET_NAME_ROFO)
        df_po = load_data_from_gsheet(SPREADSHEET_URL, SHEET_NAME_PO)
        df_sales = load_data_from_gsheet(SPREADSHEET_URL, SHEET_NAME_SALES)

    # Validasi Load Data
    if df_rofo.empty or df_po.empty:
        st.error("Gagal memuat data ROFO atau PO. Cek nama Tab di GSheet atau permission Service Account.")
        return

    # Validasi Kolom Kunci
    if "SKU SAP" not in df_rofo.columns or "SKU SAP" not in df_po.columns:
        st.error("Kolom 'SKU SAP' tidak ditemukan di data ROFO atau PO. Cek header file Anda.")
        return
    
    if "Document Date" not in df_po.columns:
         st.error("Kolom 'Document Date' tidak ditemukan di data PO. Pastikan nama kolom sesuai.")
         return

    # Proses & Tampilkan
    df_processed = process_data(df_rofo, df_po)
    create_dashboard(df_processed, df_sales)

if __name__ == "__main__":
    main()
