import pandas as pd
from sqlalchemy import create_engine

class Uploader:
    def __init__(self, tanggal, bulan, tahun, perulangan,engine):
        self.tanggal = tanggal
        self.bulan = bulan
        self.tahun = tahun
        self.perulangan = perulangan
        self.engine = engine

    def destination_table(tipe_file):
        if "Oleic Acid" in tipe_file:
            nama_tabel = "excel_report_oleic"
        elif "Fatty Acid" in tipe_file:
            nama_tabel = "excel_report_fattyacid"
        return nama_tabel
        
    def house_keeping(self, nama_tabel, rentang_update):
        target_date = pd.to_datetime(self.tanggal + ' ' + self.bulan + ' ' + self.tahun, format='%d %b %Y')
        statement = f'''DELETE FROM "public"."{nama_tabel}"
                        WHERE production_dates BETWEEN '{rentang_update}' AND '{target_date}'
                            '''
        self.engine.execute(statement)