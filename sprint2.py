import boto3

aws_access_key_id = ""
aws_secret_access_key = ""
aws_session_token = ""

# Configura el cliente de glue
glue = boto3.client('glue',
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  aws_session_token=aws_session_token,
                  region_name="eu-south-2" 
                  )

database_name = "trade_data_imat3a06"

try:
    glue.create_database(
        DatabaseInput={
            'Name': database_name,
            'Description': 'Base de datos para almacenar datos de criptomonedas desde S3'
        }
    )
    print(f"Base de datos '{database_name}' creada exitosamente.")
except glue.exceptions.AlreadyExistsException:
    print(f"La base de datos '{database_name}' ya existe.")

crawler_name = "crawler_imat3a06"
role_arn = "arn:aws:iam::124355644169:role/service-role/AWSGlueServiceRole-s3"

try:
    glue.create_crawler(
        Name=crawler_name,
        Role=role_arn,
        DatabaseName=database_name,
        Targets={'S3Targets': [{'Path': 's3://cryptos-imat3a06/'}]},
        TablePrefix='crypto_',
        SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE', 'DeleteBehavior': 'DEPRECATE_IN_DATABASE'}
    )
    print(f"Crawler '{crawler_name}' creado exitosamente.")
except glue.exceptions.AlreadyExistsException:
    print(f"El crawler '{crawler_name}' ya existe.")

glue.start_crawler(Name=crawler_name)
print(f"Crawler '{crawler_name}' iniciado.")

