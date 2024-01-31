import sys
import os
import pandas as pd
import pymysql
# access
from dbaccess import access
from secrets import sec
# S3 Connection
import boto3
from io import StringIO
from sqlalchemy import create_engine
# acces details db

host1 = access.get('host')
user1=access.get('user')
password1=access.get('password')
port1=access.get('port')
database1=access.get('database')

# acces details s3

aws_access_key = sec.get('aws_access_key_id')
aws_secret_access=sec.get('aws_secret_access_key')
bucketname=sec.get('bucketname')

try: 
    # Create connection to MySQL DB and execute the Stored Procedure to create pwinfo table
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=host1, db=database1, user=user1, pw=password1))
    engine.execute('call usp_pw_info_create()')
except Exception as err:
    print("Something went wrong: {}".format(err))
    
try:
    # Read data from csv file
    client=boto3.client('s3',aws_access_key_id=aws_access_key,
       aws_secret_access_key=aws_secret_access)
    bucket_name=bucketname
    object_key='normalizedfiles/pwinfo.csv'
    csv_obj=client.get_object(Bucket=bucket_name,Key=object_key)
    body=csv_obj['Body']
    csv_string=body.read().decode('utf-8')
    pwinfo=pd.read_csv(StringIO(csv_string))
    pwinfo.columns = ['pw_id','pw_track_num','pw_amount','pw_determ_date','pw_expire_date','pw_job_title','pw_level' ,'pw_soc_code','pw_source_name' ,'pw_unit_of_pay']
    csv_buf=StringIO()
    pwinfo.to_csv(csv_buf,header=True,index=False)
    csv_buf.seek(0)
except Exception as err:
    print("Something went wrong: {}".format(err))
    
try:
    # Insert data into pw_info table 
    pwinfo.to_sql('pw_info',engine, index=False,if_exists='replace')
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:
    engine.dispose()
