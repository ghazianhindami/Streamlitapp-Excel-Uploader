import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import os
from pipeline.extract_and_transform import Extractor
from pipeline.uploader import Uploader


load_dotenv()
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

# Create database engine connection
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")

def baca_file(file):
    if "Oleic Acid" in tipe_file:
        df_hasil = Extractor.proses_etl_l1(uploaded_file, tanggal, bulan, tahun, perulangan)
    elif "Fatty Acid" in tipe_file:
        df_hasil = Extractor.proses_etl_fatty_acid(uploaded_file,  tanggal, bulan, tahun, perulangan)
    return df_hasil

    
# --- UI STREAMLIT ---
st.set_page_config(page_title="Data ETL Uploader", layout="wide")

# Add logo with title
col1, col2 = st.columns([1, 5])
with col1:
    st.image("Apps/assets/Logo.png", width=100)
with col2:
    st.title("Excel Data Uploader to Database")

# Initialize session state
if 'perulangan_changed' not in st.session_state:
    st.session_state.perulangan_changed = False

# 1. Pilih Jenis File
tipe_file = st.selectbox(
    "Pilih Jenis File Excel:",
    ["Oleic Acid", "Fatty Acid"]
)
# Dataframe untuk pilihan bulan dan tahun

df_bulan = pd.DataFrame({
    'first column': ["Jan", "Feb", "Mar", "Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],
    'urutan_bulan': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]})

df_tahun = pd.DataFrame({
    'second column': ["2021", "2022", "2023", "2024","2025","2026","2027","2028","2029","2030"]})

df_tanggal = pd.DataFrame({
    'second column': ["1", "2", "3", "4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]})


default_month = date.today().month
month_list = df_bulan['urutan_bulan'].tolist()
month_index = month_list.index(default_month)


default_year = str(date.today().year)
year_list = df_tahun['second column'].tolist()
year_index = year_list.index(default_year)

default_tanggal = str(date.today().day -1)
tanggal_list = df_tanggal['second column'].tolist()
tanggal_index = tanggal_list.index(default_tanggal)

left_column, center_column, right_column = st.columns(3)

with left_column:
    bulan = st.selectbox(
    'Silahkan Pilih Bulan Sesuai File Anda:',
     df_bulan['first column'],index=month_index)

    'You selected: ', bulan

with center_column:
    tahun = st.selectbox(   
    'Silahkan Pilih Tahun Sesuai File Anda:',
     df_tahun['second column'], index=year_index)
    

    'You selected: ', tahun

with right_column:
    tanggal = st.selectbox(   
    'Silahkan Pilih Tanggal yang ingin anda masukkan:',
     df_tanggal['second column'], index=tanggal_index)
    
    
    'You selected: ', tanggal

left1_column, right2_column = st.columns(2)    

# 2. Upload File
with right2_column:
    uploaded_file = st.file_uploader("Pilih file Excel anda, dan pastikan file sudah sesuai dengan kategori dan format yang diminta", type=["xlsx", "xls", "xlsm"])

with left1_column:
        # Menambahkan opsi perulangan untuk upload data beberapa hari sekaligus
    show_perulangan = st.selectbox(
        'Masukkan jumlah hari data yang ingin diupload?',
        ['Tidak', 'Ya']
    )
    st.markdown("Apa bila Anda memilih 'Ya', maka Anda dapat mengupload data untuk beberapa hari sekaligus sesuai dengan jumlah hari yang Anda masukkan. Jika memilih 'Tidak', maka hanya data untuk satu hari saja yang akan diupload.")

if uploaded_file is not None:
    st.info(f"Memproses file untuk kategori: {tipe_file}")
    # Jalankan logika berdasarkan pilihan

    df_hasil = None

    if show_perulangan == 'Ya':
        perulangan = st.number_input(
            'Masukkan jumlah hari data yang ingin diupload (maksimal 30 hari):',
            min_value=0,
            max_value=30,
            value=1,
            step=1
        )
        
        if perulangan > 1:
            st.session_state.perulangan_changed = True

        'Anda akan mengupdate data dalam rentang: ', pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')-timedelta(days=perulangan-1), ' sampai ', pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')

        rentang_update = pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')-timedelta(days=perulangan-1)

        if st.session_state.get('perulangan_changed'):
            try:
                # Baca file Excel
                df_hasil = baca_file(uploaded_file)
            except:
                st.error(f"Sepertinya Anda Salah memilih file atau format file tidak sesuai, silahkan cek kembali.")
                df_hasil = None
    else:
        perulangan = 1
        rentang_update = pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')
        try:
            # Baca file Excel
            df_hasil = baca_file(uploaded_file)
        except Exception as e:
            st.error(f"Sepertinya Anda Salah memilih file atau format file tidak sesuai, silahkan cek kembali.: {e}")
            df_hasil = None     
            
    # 3. Tombol Push ke Database
    if df_hasil is not None:
        if st.button("🚀 Push ke Database"):
            try:
                nama_tabel = Uploader.destination_table(tipe_file)
                
                try:
                    uploader = Uploader(tanggal, bulan, tahun, perulangan, engine)
                    uploader.house_keeping( nama_tabel, rentang_update)
                    df_hasil['upload_timestamp'] = datetime.now()
                    df_hasil.to_sql(nama_tabel, engine, if_exists='append', index=False)
                    st.success(f"Berhasil! Data telah masuk ke tabel: {nama_tabel}")
                except Exception as e:
                    st.error(f"Terjadi kesalahan saat push data: {e}")
                
            except Exception as e:
                st.error(f"Terjadi kesalahan saat push data: {e}")