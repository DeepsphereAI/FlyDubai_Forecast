import boto3
import json
from sqlalchemy import create_engine
import pandas as pd

class RedshiftConnection:
    
    username = None
    password = None
    engine = None
    host,port = None,None
    
    def __init__(self,username,passw,engine,host,port):
        self.username = username
        self.password = passw
        self.engine = engine
        self.host = host
        self.port = port
        self.connection = create_engine('postgresql://'+username+':'+passw+'@'+host+':'+str(port)+'/dev').connect()


    def write_redshift_booking(self,dataframe):
        val = dataframe.to_sql('flydubai_fact_transaction_booking_v3', self.connection, schema='ds_ai_fd_schema',index=False, if_exists ='append')
        self.connection.close()
        
    def write_redshift_revenue(self,dataframe):
        val = dataframe.to_sql('flydubai_fact_transaction_revenue_v3', self.connection, schema='ds_ai_fd_schema',index=False, if_exists ='append')
        self.connection.close()

    def read_redshift_data_booking(self):
        dataframe = pd.read_sql('''select data_category,data_source,model_name,travel_date,year,quarter,month,week,day,hour,region,origin,destination,flight,capacity,price_type,promotion,roundtrip_or_oneway,customer_type,product_type,location_lifestyle,location_economical_status,location_employment_status,location_event,source_precipitation,substring(source_wind,1,4) as source_wind,substring(destination_wind,1,4) as destination_wind,
substring(source_humidity,1,2) as source_humidity,substring(destination_humidity,1,2) as destination_humidity,destination_precipitation,number_of_booking,date,model_accuracy,accuracy_probability from ds_ai_fd_schema.flydubai_fact_transaction_booking_v3 where data_category='ACTUAL' ''',self.connection)
        return dataframe

    def read_redshift_data_revenue(self):
        dataframe = pd.read_sql('''select data_category,data_source,model_name,travel_date,year,quarter,month,week,day,hour,region,origin,destination,flight,capacity,price_type,promotion,roundtrip_or_oneway,customer_type,product_type,location_lifestyle,location_economical_status,location_employment_status,location_event,source_precipitation,substring(source_wind,1,4) as source_wind,substring(destination_wind,1,4) as destination_wind,
substring(source_humidity,1,2) as source_humidity,substring(destination_humidity,1,2) as destination_humidity,destination_precipitation,number_of_booking,currency,revenue,date,model_accuracy,accuracy_probability from ds_ai_fd_schema.flydubai_fact_transaction_revenue_v3 where data_category='ACTUAL' ''',self.connection)
        return dataframe
    
    def truncate_table_booking(self,model_name):
        dataframe = self.connection.execute('''delete ds_ai_fd_schema.flydubai_fact_transaction_booking_v3 where model_name = '''+"'"+model_name+"'")
        self.connection.close()

    def truncate_table_revenue(self,model_name):
        dataframe = self.connection.execute('''delete ds_ai_fd_schema.flydubai_fact_transaction_revenue_v3 where model_name = '''+"'"+model_name+"'")
        self.connection.close()


def get_secret():

    secret_name = 'arn:aws:secretsmanager:us-east-2:363247502029:secret:Redshift-Dev-Credential-YsDfUO'
    region_name = "us-east-2"
    secret = ''

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    # Decrypts secret using the associated KMS CMK.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return json.loads(secret)