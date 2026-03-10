import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import os


load_dotenv()
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

# Create database engine connection
engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")
# --- FUNGSI ETL ---
def proses_etl_l1(file):
    df_metadata = pd.ExcelFile(file)
    exclude_sheets = ["Sign", "Main Page", "Summary", "Dashboard"]

    filtered_sheets = [sheet for sheet in df_metadata.sheet_names 
                   if sheet not in exclude_sheets]
    
    gugu = pd.DataFrame()
    gugu['sheet_names'] = filtered_sheets

    gugu.reset_index(drop=True,inplace=True)
    gugu['clean_date'] = gugu['sheet_names']
    gugu['clean_date'] = gugu['clean_date'] + ' ' + bulan + ' ' + tahun

    gugu['date'] = pd.to_datetime(gugu['clean_date'], format='%d %b %Y')
    gugu['id_date']= gugu.index
    
    df_canvas = []
    for i in range(0,perulangan):
        # target_date = pd.to_datetime(date.today()-timedelta(days=i))
        target_date = pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')-timedelta(days=i)
        mask = gugu['date'] == target_date
        matching_indices = gugu.index[mask].tolist()
        idx = matching_indices[0]
        datedesire = gugu.loc[idx, 'sheet_names']
        df = pd.read_excel(file,
                    sheet_name = datedesire,
                    skiprows = 9,
                    nrows = 23,
                    usecols = 'A:O',
                    
                
                    )
        df.drop(df.columns[[1,3, 4,5]],axis=1,inplace=True)
        df.rename(columns={"Unnamed: 0":"Section","Product":"Shift 2","Product.1":"Shift 3","Product.2":"Shift 1","Unnamed: 2":"Product",
                        "D.Time":"DownTime_shift2",
                        "D.Time.1":"DownTime_shift3",
                        "D.Time.2":"DownTime_shift1",
                        "122T...":"Tangki_shift2",
                        "122T....1":"Tangki_shift3",
                        "122T....2":"Tangki_shift1"
                        },inplace=True)
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(" ","_")
        df['section'] = df['section'].ffill()
        df['activity'] = np.where(df['product'].str.contains('Feed', case=False, na=False), 
                                'Feed', 
                                'Production')
        df['production_dates'] = target_date


        df_melted_prod = df.melt(
        id_vars=['section','activity','product', 'production_dates'],           # Columns to keep as they are
            value_vars=['shift_1', 'shift_2', 'shift_3'],      # Columns to turn into rows
            var_name='shift_category',                         # Name for the new category column
            value_name='shift_value'                           # Name for the numeric values column
            )
        df_melted_prod['shift_value'] = df_melted_prod['shift_value'].fillna(0)
        df_melted_prod['shift_category'] = df_melted_prod['shift_category'].str.replace('shift_', 'Shift ')  

        df_melted_downtime = df.melt(
        id_vars=['section','activity','product', 'production_dates'],           # Columns to keep as they are
            value_vars=['downtime_shift1', 'downtime_shift2', 'downtime_shift3'],      # Columns to turn into rows
            var_name='shift_category',                         # Name for the new category column
            value_name='downtime_value'                           # Name for the numeric values column
            )
        df_melted_downtime['downtime_value'] = df_melted_downtime['downtime_value'].fillna(0)
        df_melted_downtime['shift_category'] = df_melted_downtime['shift_category'].str.replace('downtime_shift', 'Shift ')  

        df_melted_tangki = df.melt(
        id_vars=['section','activity','product', 'production_dates'],           # Columns to keep as they are
            value_vars=['tangki_shift1', 'tangki_shift2', 'tangki_shift3'],      # Columns to turn into rows
            var_name='shift_category',                         # Name for the new category column
            value_name='tangki_value'                           # Name for the numeric values column
            )
        df_melted_tangki['tangki_value'] = df_melted_tangki['tangki_value'].fillna('-')
        df_melted_tangki['shift_category'] = df_melted_tangki['shift_category'].str.replace('tangki_shift', 'Shift ')

        df_finalize = df_melted_prod.merge(df_melted_downtime, on=['section','activity','product', 'production_dates','shift_category'])
        df_finalize = df_finalize.merge(df_melted_tangki, on=['section','activity','product', 'production_dates','shift_category'])
        df_canvas.append(df_finalize)

    df_final = pd.concat(df_canvas, ignore_index=True)
    df_final2= df_final.copy()
    shift_summary = df_final2.groupby(['section','production_dates', 'product', 'activity'])[['shift_value','downtime_value']].sum().reset_index()
    shift_summary.columns = [ 'Section','Production Date', 'Product', 'Activity', 'Total Shift Value', 'Total Downtime Value' ]

    left_column, right_column = st.columns(2)
# You can use a column just like st.sidebar:
    with left_column:
        st.write("Preview Data Production Report L1 Oleic:", df_final)

    with right_column:
       
        st.write("Preview Data Summary Report L1 Oleic :", shift_summary)
#
    return df_final

def proses_etl_fatty_acid(file):
    df_metadata = pd.ExcelFile(file)
    gugu = pd.DataFrame()
    gugu['sheet_names'] = df_metadata.sheet_names
    gugu['clean_date'] = gugu['sheet_names']
    gugu['clean_date'] = gugu['clean_date'].str.strip()
    gugu['clean_date'] = gugu['clean_date'].str[:2]
    gugu['clean_date'] = gugu['clean_date'] + ' ' + bulan + ' ' + tahun
    # Convert to datetime
    gugu['date'] = pd.to_datetime(gugu['clean_date'], format='%d %b %Y')
    gugu['id_date']= gugu.index

    df_canvas = []
    for i in range(0,perulangan):
        # target_date = pd.to_datetime(date.today()-timedelta(days=i))
        target_date = pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')-timedelta(days=i)
        mask = gugu['date'] == target_date
        matching_indices = gugu.index[mask].tolist()
        idx = matching_indices[0]
        datedesire = gugu.loc[idx, 'sheet_names']
        datedesireplus1 = gugu.loc[idx+1, 'sheet_names']
        ################################################
        df_full = []
        df = pd.read_excel(file,
                    sheet_name = datedesire,
                    skiprows = 8,
                    nrows = 24,
                    usecols = 'A:O',
                    
                
                    )
        df.drop(df.columns[[1,3,4,5,6,7,8]],axis=1,inplace=True)
        df.rename(columns={"Unnamed: 0":"Section",
                        "Product.1":"Shift 2",
                        "Unnamed: 2":"Product",
                        "Product.2":"Shift 3",
                        "D.Time.1":"DownTime_shift2",
                        "122T....1":"Tangki_shift2",
                        "D.Time.2":"DownTime_shift3",
                        "122T....2":"Tangki_shift3",},inplace=True)
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(" ","_")
        df['section'] = df['section'].ffill()
        df['section'] =df['section'].astype('str')
        df['activity'] = np.where(df['product'].str.contains('Feed', case=False, na=False), 
                                'Feed', 
                                'Production')
        df['id_product']= df.index

        ##################################################
        df_end = pd.read_excel(file,
                    sheet_name = datedesireplus1,
                    skiprows = 8,
                    nrows = 24,
                    usecols = 'A:I',
                    
                
                    )
        df_end.drop(df_end.columns[[1, 3,4,5]],axis=1,inplace=True)
        df_end.rename(columns={"Product":"Shift 1",
                            "Unnamed: 2":"Product",
                            "Unnamed: 0":"Section",
                            "D.Time":"DownTime_shift1",
                            "122T...":"Tangki_shift1"},inplace=True)
        df_end.columns = df_end.columns.str.lower()
        df_end.columns = df_end.columns.str.replace(" ","_")
        df_end['section'] = df_end['section'].ffill()
        df_end['section'] =df_end['section'].astype('str')
        df_end['activity'] = np.where(df_end['product'].str.contains('Feed', case=False, na=False), 
                                'Feed', 
                                'Production')
        df_end['id_product']= df_end.index

        # ####################################################
        df_finalize = pd.merge(df,df_end,how='inner')
        df_finalize.drop('id_product',axis=1,inplace=True)
        df_finalize['production_dates'] = target_date

    #     # ###################################################
        df_melted_prod = df_finalize.melt(
        id_vars=['section','activity','product', 'production_dates'],           # Columns to keep as they are
            value_vars=['shift_1', 'shift_2', 'shift_3'],      # Columns to turn into rows
            var_name='shift_category',                         # Name for the new category column
            value_name='shift_value'                           # Name for the numeric values column
            )
        df_melted_prod['shift_value'] = df_melted_prod['shift_value'].fillna(0)
        df_melted_prod['shift_category'] = df_melted_prod['shift_category'].str.replace('shift_', 'Shift ')  


        df_melted_downtime = df_finalize.melt(
        id_vars=['section','activity','product', 'production_dates'],           # Columns to keep as they are
            value_vars=['downtime_shift1', 'downtime_shift2', 'downtime_shift3'],      # Columns to turn into rows
            var_name='shift_category',                         # Name for the new category column
            value_name='downtime_value'                           # Name for the numeric values column
            )
        df_melted_downtime['downtime_value'] = df_melted_downtime['downtime_value'].fillna(0)
        df_melted_downtime['shift_category'] = df_melted_downtime['shift_category'].str.replace('downtime_shift', 'Shift ')  


        df_melted_tangki = df_finalize.melt(
        id_vars=['section','activity','product', 'production_dates'],           # Columns to keep as they are
            value_vars=['tangki_shift1', 'tangki_shift2', 'tangki_shift3'],      # Columns to turn into rows
            var_name='shift_category',                         # Name for the new category column
            value_name='tangki_value'                           # Name for the numeric values column
            )
        df_melted_tangki['tangki_value'] = df_melted_tangki['tangki_value'].fillna('-')
        df_melted_tangki['shift_category'] = df_melted_tangki['shift_category'].str.replace('tangki_shift', 'Shift ')


        df_result = df_melted_prod.merge(df_melted_downtime, on=['section','activity','product', 'production_dates','shift_category'])
        df_result= df_result.merge(df_melted_tangki, on=['section','activity','product', 'production_dates','shift_category'])
        df_result['section'] = df_result['section'].str.replace(".0","")

        df_canvas.append(df_result)
    
    df_final = pd.concat(df_canvas, ignore_index=True)

    df_final2= df_final.copy()
    shift_summary = df_final2.groupby(['section','production_dates', 'product', 'activity'])[['shift_value','downtime_value']].sum().reset_index()
    shift_summary.columns = [ 'Section','Production Date', 'Product', 'Activity', 'Total Shift Value', 'Total Downtime Value' ]

    left_column, right_column = st.columns(2)
# You can use a column just like st.sidebar:
    with left_column:
        st.write("Preview Data Production Report Fatty Acid:", df_final)

    with right_column:
       
        st.write("Preview Data Summary Report Fatty Acid :", shift_summary)
#
    return df_final


def baca_file(file):
    if "Oleic Acid" in tipe_file:
        df_hasil = proses_etl_l1(file)
    elif "Fatty Acid" in tipe_file:
        df_hasil = proses_etl_fatty_acid(file) 
    return df_hasil

def destination_table(tipe_file):
    if "Oleic Acid" in tipe_file:
        nama_tabel = "excel_report_oleic"
    elif "Fatty Acid" in tipe_file:
        nama_tabel = "excel_report_fattyacid"
    return nama_tabel
    
def house_keeping():
    target_date = pd.to_datetime(tanggal + ' ' + bulan + ' ' + tahun, format='%d %b %Y')
    statement = f'''DELETE FROM "public"."{nama_tabel}"
                    WHERE production_dates BETWEEN '{rentang_update}' AND '{target_date}'
                        '''
    engine.execute(statement)

    
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
                nama_tabel = destination_table(tipe_file)
                
                try:
                    house_keeping()
                    df_hasil['upload_timestamp'] = datetime.now()
                    df_hasil.to_sql(nama_tabel, engine, if_exists='append', index=False)
                    st.success(f"Berhasil! Data telah masuk ke tabel: {nama_tabel}")
                except Exception as e:
                    st.error(f"Terjadi kesalahan saat push data: {e}")
                
            except Exception as e:
                st.error(f"Terjadi kesalahan saat push data: {e}")