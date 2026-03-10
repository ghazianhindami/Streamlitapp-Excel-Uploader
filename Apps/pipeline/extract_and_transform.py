import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date

class Extractor:
    def __init__(self, file):
        self.file = file

    def proses_etl_l1(file, tanggal, bulan, tahun, perulangan):
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
    
    def proses_etl_fatty_acid(file, tanggal, bulan, tahun, perulangan):
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