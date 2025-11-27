# File: app.py

import streamlit as st
import pandas as pd
import gspread
import io # Diperlukan untuk memproses data sebagai file dalam memori

# ---- 1. SETUP KONEKSI GOOGLE SHEETS ----
@st.cache_resource(ttl=3600) # Data di-cache selama 1 jam
def get_service_account():
    # Mengambil kredensial dari secrets.toml
    creds = st.secrets["gcp_service_account"]
    
    # Inisiasi koneksi gspread
    gc = gspread.service_account_from_dict(dict(creds))
    return gc

def load_data_from_gsheet(spreadsheet_url: str, sheet_name: str):
    """Fungsi untuk memuat data dari Sheet tertentu di Google Sheets."""
    gc = get_service_account()
    
    try:
        # 1. Buka Spreadsheet berdasarkan URL
        sh = gc.open_by_url(spreadsheet_url)
        
        # 2. Ambil data dari Worksheet/Sheet Name
        worksheet = sh.worksheet(sheet_name)
        
        # 3. Ambil semua data sebagai list of lists
        data = worksheet.get_all_values()
        
        # 4. Konversi ke Pandas DataFrame (baris pertama adalah header)
        df = pd.DataFrame(data[1:], columns=data[0])
        
        return df
    
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Error: Spreadsheet dengan URL ini tidak ditemukan. Pastikan URL benar.")
        return pd.DataFrame()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Error: Sheet dengan nama '{sheet_name}' tidak ditemukan.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Terjadi error saat memuat data: {e}")
        return pd.DataFrame()

# Ganti ini dengan URL Google Sheets Anda
SPREADSHEET_URL = "GANTI DENGAN URL GOOGLE SHEETS ANDA" 

# ---- 2. MEMUAT DATA ----
st.title("Loading Data Forecast Dashboard")

# Nama Sheet dari file CSV yang Anda berikan (asumsi)
SHEET_ROFO = "Data-Forecast(Rofo)" # Asumsi nama sheet 1
SHEET_SALES = "Data-Sales" # Asumsi nama sheet 2

# Memuat ROFO Data
df_rofo = load_data_from_gsheet(SPREADSHEET_URL, SHEET_ROFO)
st.subheader("Data ROFO (Forecast)")
st.write(f"Baris: {len(df_rofo)}, Kolom: {len(df_rofo.columns)}")
# Tampilkan 5 baris pertama untuk inspeksi
st.dataframe(df_rofo.head())

# Memuat Sales Data (Actual)
df_sales = load_data_from_gsheet(SPREADSHEET_URL, SHEET_SALES)
st.subheader("Data Sales (Actual)")
st.write(f"Baris: {len(df_sales)}, Kolom: {len(df_sales.columns)}")
st.dataframe(df_sales.head())


# ---- 3. LANGKAH SELANJUTNYA (DATA PROCESSING & VISUALISASI) ----
st.header("Langkah Selanjutnya: Data Processing")
st.markdown("""
Setelah data berhasil dimuat (di atas), kita akan:
1.  **Transformasi Data:** Ubah data dari format 'Wide' (bulanan ke samping) menjadi format 'Long' (bulanan ke bawah) untuk memudahkan perhitungan.
2.  **Pembersihan Data:** Pastikan kolom Kuantitas adalah numerik dan kolom Tanggal adalah format datetime.
3.  **Penggabungan Data:** Gabungkan `df_rofo` dan `df_sales` berdasarkan **Tanggal** dan **SKU**.
4.  **Perhitungan Metrik:** Hitung MAPE, Accuracy, dan Bias per bulan dan per SKU.
5.  **Visualisasi:** Gunakan Streamlit dan Altair/Plotly untuk membuat grafik yang interaktif.
""")
