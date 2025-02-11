import os
import pandas as pd
import boto3
from TradingviewData import TradingViewData, Interval

aws_access_key_id = ""
aws_secret_access_key = ""
aws_session_token = ""

# Configura el cliente de S3
s3 = boto3.client('s3',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  aws_session_token=aws_session_token,
                  region_name="eu-south-2" 
                  )


bucket_name = "cryptos-imat3a06"  # Reemplaza con el nombre de tu bucket

# Inicializa TradingViewData
request = TradingViewData()

list_cryptos = ['BTCUSD', 'ETHUSD', 'XRPUSD', 'SOLUSD', 'DOGEUSD', 'ADAUSD', 'SHIBUSD', 'DOTUSD', 'AAVEUSD', 'XLMUSD']
years = [2021, 2022, 2023, 2024]
months = list(range(1, 13))  # Lista de meses del 1 al 12

for crypto in list_cryptos:
    print(crypto)
    data = request.get_hist(symbol=crypto, exchange='CRYPTO', interval=Interval.daily, n_bars=1600).reset_index()
    
    for year in years:
        for month in months:
            # Filtrar los datos por año y mes
            data_clone = data[(data['datetime'].dt.year == year) & (data['datetime'].dt.month == month)]
            
            # Crear la ruta del archivo local
            local_file_path = f'csvs/{crypto}/{year}/{month}/{crypto}{year}{month}.csv'
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            # Guardar los datos en un archivo CSV
            data_clone.to_csv(local_file_path, index=False)
            
            # Crear la ruta en S3 (corrigiendo el error)
            s3_key = f'data/{crypto}/{year}/{month}/{crypto}{year}{month}.csv'
            
            # Subir el archivo a S3
            s3.upload_file(local_file_path, bucket_name, s3_key)
            print(f"Archivo {local_file_path} subido a s3://{bucket_name}/{s3_key}")
