import sys
import os
import pandas as pd
import numpy as np
import snowflake.connector 
import pandas as pd
import pymysql
from snowflake.connector.pandas_tools import write_pandas
from dbaccess import access
from snowflakecreds import con
from io import StringIO


hostdb= access.get('host')
userdb=access.get('user')
passworddb=access.get('password')
portdb=access.get('port')
databasedb=access.get('database')

user1 = con.get('user')
password1=con.get('password')
account1=con.get('account')
warehouse1=con.get('warehouse')
database1=con.get('database')
schema1=con.get('schema')

year='2022'

try:
    connection = pymysql.connect(host=hostdb,user=userdb,password=passworddb,port=portdb,database=databasedb)
    query_geography='''select geography_id,postal_code,city,state,'''+year+' as ApplicationYear,'+'''current_timestamp() as lastrefreshedon from geography '''
    geography = pd.read_sql(query_geography,connection)
    geography.columns = geography.columns.str.upper()
    geography=geography.loc[ geography['STATE'].str.len()>2 ]
    geography=geography.replace(r'', np.NaN)
    geography[['POSTAL_CODE','CITY','STATE']]=geography[['POSTAL_CODE','CITY','STATE'  ]].fillna('Unknown')
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    connection.close()
    
try:
    conn=snowflake.connector.connect(user=user1,password=password1,account=account1,warehouse=warehouse1,database=database1,schema=schema1) 
    table_name = 'GEOGRAPHY'
    write_pandas(
            conn=conn,
            df=geography,
            table_name=table_name,
            database=database1,
            schema=schema1
        )
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    conn.close()
    

try:
    connection = pymysql.connect(host=hostdb,user=userdb,password=passworddb,port=portdb,database=databasedb)
    query_education_info='''select education_id, education_info, education_info_other,'''+year+' as ApplicationYear,'+'''current_timestamp() as lastrefreshedon from education_info '''
    education_info = pd.read_sql(query_education_info,connection)
    education_info.columns = education_info.columns.str.upper()
    education_info=education_info.replace(r'', np.NaN)
    education_info[['EDUCATION_ID','EDUCATION_INFO','EDUCATION_INFO_OTHER']]=education_info[['EDUCATION_ID','EDUCATION_INFO','EDUCATION_INFO_OTHER']].fillna('Unknown')
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    connection.close()


try:
    conn=snowflake.connector.connect(user=user1,password=password1,account=account1,warehouse=warehouse1,database=database1,schema=schema1) 
    table_name = 'EDUCATION_INFO'
    write_pandas(
            conn=conn,
            df=education_info,
            table_name=table_name,
            database=database1,
            schema=schema1
        )
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    conn.close()
    
try:
    connection = pymysql.connect(host=hostdb,user=userdb,password=passworddb,port=portdb,database=databasedb)
    query_agent='''select agent_id, agent_firm_name, agent_city, agent_state,'''+year+' as ApplicationYear,'+'''current_timestamp() as lastrefreshedon  from agent '''
    agent = pd.read_sql(query_agent,connection)
    agent.columns = agent.columns.str.upper()
    agent=agent.replace(r'', np.NaN)
    agent[['AGENT_FIRM_NAME','AGENT_CITY','AGENT_STATE']]=agent[['AGENT_FIRM_NAME','AGENT_CITY','AGENT_STATE']].fillna('Unknown')
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    connection.close()
    
 
try:
    conn=snowflake.connector.connect(user=user1,password=password1,account=account1,warehouse=warehouse1,database=database1,schema=schema1) 
    table_name = 'AGENT'
    write_pandas(
            conn=conn,
            df=agent,
            table_name=table_name,
            database=database1,
            schema=schema1
        )
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    conn.close()
    

try:
    connection = pymysql.connect(host=hostdb,user=userdb,password=passworddb,port=portdb,database=databasedb)
    query_pwinfo='''select pw_id, pw_track_num, pw_determ_date, pw_expire_date, pw_job_title, pw_level, pw_soc_code, pw_source_name,'''+year+' as ApplicationYear,'+'''current_timestamp() as lastrefreshedon   from pw_info '''
    pwinfo = pd.read_sql(query_pwinfo,connection)
    pwinfo.columns = pwinfo.columns.str.upper()
    pwinfo=pwinfo.replace(r'', np.NaN)
    pwinfo[['PW_SOC_CODE']]=pwinfo[['PW_SOC_CODE']].fillna('99-9999')
    pwinfo[['PW_TRACK_NUM','PW_JOB_TITLE','PW_LEVEL','PW_SOURCE_NAME']]=pwinfo[['PW_TRACK_NUM','PW_JOB_TITLE','PW_LEVEL','PW_SOURCE_NAME']].fillna('Unknown')
    pwinfo[['PW_DETERM_DATE','PW_EXPIRE_DATE']]=pwinfo[['PW_DETERM_DATE','PW_EXPIRE_DATE']].fillna('01/01/1900')
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    connection.close()
    
try:
    conn=snowflake.connector.connect(user=user1,password=password1,account=account1,warehouse=warehouse1,database=database1,schema=schema1) 
    table_name = 'PW_INFO'
    write_pandas(
            conn=conn,
            df=pwinfo,
            table_name=table_name,
            database=database1,
            schema=schema1
        ) 
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    conn.close()
    
 
try:
    connection = pymysql.connect(host=hostdb,user=userdb,password=passworddb,port=portdb,database=databasedb)
    query_application='''
    SELECT an.application_case_number
    ,an.applicant_id
    ,an.case_received_date
    ,an.case_status
    ,an.class_of_admission
    ,an.country_of_citizenship
    ,an.decision_date
    ,an.pw_id
    ,an.preparer_info_title
    ,an.us_economic_sector
    ,an.wage_offer_from
    ,an.wage_offer_to
    ,an.wage_offer_unit_of_pay_9089
    ,at.applicant_birth_country
    ,at.applicant_edu_id
    ,at.applicant_major
    ,at.applicant_institution
    ,at.geography_id AS applicant_geography_id
    ,ag.agent_id
    ,ag.agent_firm_name
    ,ag.agent_city
    ,ag.agent_state
    ,e.employer_id
    ,e.employer_name
    ,e.employer_address_1
    ,e.employer_address_2
    ,e.geography_id as employer_geography_id
    ,e.employer_num_employees
    ,e.employer_phone
    ,e.employer_phone_ext
    ,e.employer_yr_estab
    ,ji.job_info_id
    ,ji.job_title
    ,ji.education_id
    ,ji.experience_num_months
    ,ji.job_major
    ,ji.training_field
    ,ji.training_num_months
    ,ji.geography_id
    ,ji.alt_combined_yrs
    ,ji.alt_combo_edu
    ,ji.alt_field_name
    ,ji.alt_occupation
    ,ji.alt_occupation_job_title
    ,ji.alt_occupation_num_months
    ,'''+year+' as ApplicationYear,'+'''current_timestamp() as lastrefreshedon  
    FROM application an
    INNER JOIN applicant at ON an.applicant_id = at.applicant_id
    INNER JOIN agent ag ON ag.agent_id = an.agent_id
    INNER JOIN employer e ON e.employer_id = an.employer_id
    INNER JOIN job_info ji ON ji.job_info_id = an.job_info_id
    WHERE RIGHT(case_received_date,4)=2022
    '''
    application = pd.read_sql(query_application,connection)
    application=application.replace(r'', np.NaN)
    application[['applicant_id', 'pw_id','applicant_edu_id','applicant_geography_id', 
             'agent_id','employer_id','employer_geography_id', 'employer_num_employees',
             'employer_yr_estab', 'job_info_id','education_id','experience_num_months',
             'training_num_months', 'geography_id', 'alt_combined_yrs',
             'alt_occupation_num_months']]=application[['applicant_id', 'pw_id','applicant_edu_id',
                                                        'applicant_geography_id', 'agent_id',
                                                        'employer_id','employer_geography_id', 
                                                        'employer_num_employees','employer_yr_estab', 'job_info_id','education_id',
                                                        'experience_num_months','training_num_months', 'geography_id', 'alt_combined_yrs',
                                                        'alt_occupation_num_months']].fillna(9999)
    application[['application_case_number',  'case_status', 'class_of_admission', 
             'country_of_citizenship', 'preparer_info_title', 'us_economic_sector', 
             'wage_offer_unit_of_pay_9089', 'applicant_birth_country', 'applicant_major', 
             'applicant_institution', 'agent_firm_name', 'agent_city', 'agent_state', 
             'employer_name', 'employer_address_1', 'employer_address_2', 
             'employer_phone', 'employer_phone_ext', 'job_title', 'job_major', 'training_field', 
             'alt_combo_edu','alt_field_name', 'alt_occupation', 
             'alt_occupation_job_title']]=application[['application_case_number',  'case_status', 'class_of_admission', 
                                                       'country_of_citizenship', 'preparer_info_title', 'us_economic_sector', 
                                                       'wage_offer_unit_of_pay_9089', 'applicant_birth_country', 'applicant_major', 
                                                       'applicant_institution', 'agent_firm_name', 'agent_city', 'agent_state', 
                                                       'employer_name', 'employer_address_1', 'employer_address_2', 
                                                       'employer_phone', 'employer_phone_ext', 'job_title', 'job_major', 'training_field', 
                                                       'alt_combo_edu','alt_field_name', 'alt_occupation', 
                                                       'alt_occupation_job_title']].fillna('Unknown')
    application[['case_received_date', 'decision_date']]=application[['case_received_date', 
                                                             'decision_date']].fillna('01/01/1900')
    application[['wage_offer_from', 'wage_offer_to']]=application[['wage_offer_from', 'wage_offer_to']].fillna(0.00)
    application['wage_offer_to'] = pd.to_numeric(application['wage_offer_to'])
    application['wage_offer_from'] = pd.to_numeric(application['wage_offer_from'])
    application.columns = application.columns.str.upper()
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    connection.close()


try:
    conn=snowflake.connector.connect(user=user1,password=password1,account=account1,warehouse=warehouse1,database=database1,schema=schema1) 
    table_name = 'APPLICATION'
    write_pandas(
            conn=conn,
            df=application,
            table_name=table_name,
            database=database1,
            schema=schema1
        )
except Exception as err:
    print("Something went wrong: {}".format(err))
finally:    
    conn.close()

        
    