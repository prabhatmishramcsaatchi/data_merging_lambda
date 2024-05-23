import boto3
import os
import json
import pandas as pd
from io import StringIO
from datetime import datetime ,timedelta
from io import BytesIO
import botocore

# Initialize S3 client
s3 = boto3.client('s3')

# Source and destination bucket and prefix details
source_bucket = 'wikitablescrapexample'
source_prefix = 'amazon_sprinklr_pull/fluency/'
destination_bucket = 'wikitablescrapexample'
destination_prefix = 'amazon_sprinklr_pull/result/'
backup_prefix = "amazon_sprinklr_pull/monthlybackeup/"
excel_key = 'amazon_sprinklr_pull/mappingandbenchmark/adjectivemapping.xlsx'
weekly_prefix = 'amazon_sprinklr_pull/Fluency-Weekly/'
 
# List of file names to read
GOOD_FILES = [
    'Facebook_Pull_1_FluencyMonthly',
    'Instagram_3_FluencyMonthly',
    'Instagram_Story_5_FluencyMonthly',
    'LinkedIn_6_FluencyMonthly',
    'Twitter_2_FluencyMonthly',
    'YouTube_10_FluencyMonthly',
    'User_Group_Lookup_9_FluencyMonthly',
    'Tiktok_19_FluencyMonthly'
]
 
ADDITIONAL_FILES = [
    "Paid_Data_11_FluencyWeekly"
]
#Paid_Data_11_FluencyWeekly.json
#Paid_Data_11_FluencyWeekly.json

def list_all_objects(bucket, prefix):
    objects = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        objects.extend(response.get('Contents', []))

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return objects

def read_json_from_s3(bucket, key):
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    json_data = s3_object['Body'].read()
    json_data_str = json_data.decode('utf8')
    data = pd.read_json(json_data_str, lines=True)
    return data

def read_excel_from_s3(bucket, key):
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    excel_data = s3_object['Body'].read()
    excel_data_io = BytesIO(excel_data)
    data = pd.read_excel(excel_data_io)
    return data
def create_dataframe(data, platform_name, pull_date, is_paiddata):
    df = pd.DataFrame(data)
    df['platform_name'] = platform_name
    df['Pull Date'] = pull_date
    df['is_paiddata'] = is_paiddata
    return df


def upload_csv_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )


def process_additional_file(additional_key, excel_key, pull_date):
    additional_data = read_json_from_s3(source_bucket, additional_key)
    excel_data = read_excel_from_s3(source_bucket, excel_key)
    merged_df = pd.merge(additional_data, excel_data, left_on='AD_OBJECTIVE', right_on='Objective')
    merged_df['Pull Date'] = pull_date
    merged_df['PERMALINK'] = merged_df['AD_POST_PERMALINK']
    merged_df['is_paiddata'] = 1
    merged_df = merged_df.drop(columns=['AD_POST_PERMALINK'])
    return merged_df
 
      
def filter_data(df):
    # First, ensure 'Pull Date' is in datetime format for comparison
    df['Pull Date'] = pd.to_datetime(df['Pull Date']).dt.date

    # Find the maximum 'Pull Date' within the DataFrame to use as the current date
    current_date = df['Pull Date'].max()
    
    # Calculate cutoff dates
    cutoff_date_organic = current_date - timedelta(days=45)
    cutoff_date_paid = current_date - timedelta(days=5)

    # Filter organic data
    organic_df = df[(df['is_paiddata'] == 0) & (df['Pull Date'] >= cutoff_date_organic)]
    
    # Filter paid data
    paid_df = df[(df['is_paiddata'] == 1) & (df['Pull Date'] >= cutoff_date_paid)]
   
    # Combine both datasets
    filtered_df = pd.concat([organic_df, paid_df], ignore_index=True)
    
    return filtered_df


    
def paid_region_mapping(master_df, additional_df):
    
    

    region = {   "Sell On Amazon":'IN',
    "Amazon AU - OGB":'AU',
    "FR - LUX Entity":'FR',
    "Amazon Australia LinkedIn":'AU',
    "SE - LUX Entity":'SE',
    "PL - LUX Entity":'PL',
    "DE - LUX Entity":'DE',
    "Amazon PL Corp PR 2022 (EUR B744 PO)":'PL',
    "Amazon FR Corp PR 2022 - EUR":'FR',
    "Amazon GSMC APAC":'APAC',
    "SG_Amazon_Socialyse_231806596327391":'SG',
    "AmazonNewsES - Ogilvy":'ES',
    'Amazon Japan [PR JP]':'JP',
    'ES - LUX Entity':'ES' ,
    'NL - LUX Entity':'NL',
    'AU SMC PR FB 2P # Ad Account':'AU',
    'BE - Lux Entity':'BE',
    'IT - LUX Entity':'IT',
    'UK - LUX Entity':'UK',
    'AmazonNews EU - Amazon':'EU',
    'AU WWC GSMC FB 2P # Consumer PR':'AU',
    'UK - UK Entity':'UK',
    'Amazon DE Corp PR 2022 (EUR 51 PO)':'DE',
    'Highlights - LUX Entity':'Global',
    'DE – DE Entity':'DE',
    'ES – ES Entity':'ES',
    'IT – IT Entity':'IT',
    'TR - TR Entity':'TR'
    }
       
    
     
 
    additional_df['Region'] = additional_df['AD_ACCOUNT'].replace(region) 

    rechmapping = {
        "ES": 0.868750038,
        "IT": 0.743954342,
        "UK": 0.681252769,
        "FR": 0.750896438,
        "NL": 0.746447156,
        "PL": 0.739573255,
        "TR": 0.794604181,
        "SE": 0.664015732,
        "DE": 0.676741732,
        "HI": 0.740692849,
        "BE": 0.740692849,
        "APAC": 0.74,
        "SG": 0.74,
        "AU": 0.74,
        "IN": 0.74,
        'JP':0.74,
        'EU':0.74,
        'Global':0.74
    }
  

    additional_df['Paid_Reach'] = additional_df['IMPRESSIONS__SUM'] * additional_df['Region'].map(rechmapping)
    
                

    return additional_df    
# pulldate='2023-09-06'
#pulldate = event['Records'][0]['s3']['object']['key'].split('/')[2].split('_')[0]
 #pulldate = event['Records'][0]['s3']['object']['key'].split('/')[2].split('_')[0]
    
   
def lambda_handler(event, context):
    try:
        current_date = datetime.now().strftime('%Y-%m-%d')
        #pulldate = '2023-09-04'  # hardcoded for now, change as per your requirement
        pulldate = event['Records'][0]['s3']['object']['key'].split('/')[2].split('_')[0]   
        pulldate_datetime = datetime.strptime(pulldate, '%Y-%m-%d')
        pulldate_day_of_week = pulldate_datetime.strftime('%A')

        dfs = []
        additional_file_exists = False
        additional_keys = []
        
        response =list_all_objects(bucket=source_bucket, prefix=source_prefix)
 
        

        for content in response:
            key = content['Key']
            folder_date = key.split('/')[2].split('_')[0]
            file_basename, file_extension = os.path.splitext(os.path.basename(key))
            
       
            if file_extension == '.json' and file_basename in GOOD_FILES and folder_date == pulldate:
                
                
                data = read_json_from_s3(source_bucket, key)
                platform_name = file_basename.split('_')[0]
                df = create_dataframe(data, platform_name, pulldate, 0)
                df = df[df['platform_name'] != "User"]
                dfs.append(df)
           
        if pulldate_day_of_week == 'Monday':
            response_weekly = list_all_objects(bucket=source_bucket, prefix=weekly_prefix)
            for content in response_weekly:
                key = content['Key']
                folder_date = key.split('/')[2].split('_')[0]
                
                file_basename, file_extension = os.path.splitext(os.path.basename(key))
                if file_extension == '.json' and file_basename in ADDITIONAL_FILES and folder_date == pulldate:
                    additional_file_exists = True
                    additional_keys.append(key)
 
        if not dfs:
            return {'statusCode': 500, 'body': 'No data to process'}

        master_df = pd.concat(dfs, ignore_index=True)
  

        if additional_file_exists:
            
            for additional_key in additional_keys:
                print(additional_key)
               
                merged_df = process_additional_file(additional_key, excel_key, pulldate)
               
               
                merged_df = paid_region_mapping(master_df, merged_df)
                merged_df['is_paiddata'] = 1
                master_df = pd.concat([master_df, merged_df], ignore_index=True)
                

        try:
            obj = s3.get_object(Bucket=destination_bucket, Key=destination_prefix + 'master_table.csv')
            existing_df = pd.read_csv(obj['Body'])
            
            last_row_number = existing_df['row_number'].max()
            master_df['row_number'] = range(last_row_number + 1, last_row_number + 1 + len(master_df))
            
            combined_df = pd.concat([existing_df, master_df], ignore_index=True)
            combined_df.drop_duplicates(subset=[col for col in combined_df.columns if col != 'row_number'], keep='last', inplace=True)

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                master_df['row_number'] = range(1, len(master_df) + 1)
                combined_df = master_df
            else:
                raise
        #Monday   
        if pulldate_day_of_week == 'Monday':
            master_df_filtered = filter_data(combined_df)
            max_pull_date = pd.to_datetime(combined_df['Pull Date']).max()
            backup_key = backup_prefix + 'backupmaster_' + max_pull_date.strftime('%Y-%m-%d') + '.csv'
            upload_csv_to_s3(master_df_filtered,destination_bucket, backup_key)

        upload_csv_to_s3(combined_df, destination_bucket, destination_prefix + 'master_table.csv')

        return {'statusCode': 200, 'body': 'Master table created successfully!'}
    except Exception as e:
        return {'statusCode': 500, 'body': f'Error occurred: {str(e)}'}

               
