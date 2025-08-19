import os
from datetime import datetime, timedelta
import boto3
import json

def lambda_handler(event,context):

    s3 = boto3.client('s3')
    sns = boto3.client('sns', region_name=os.environ['TOPIC_REGION'])
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']

    alert_var,messages=0,[]

    if isinstance(event, str):
        event=json.loads(event)
    
    print("s3 object and sns topic succesfully created..")

    src_files_list=[]

    if 'detail' in event:
        src_bucket = event['detail']['bucket']['name']
        src_files_list.append(event['detail']['object']['key'])
        dst_bucket_1 = os.environ['DST_BUCKET_1']
        dst_bucket_2 = os.environ['DST_BUCKET_2']
        dst_bucket_3 = os.environ['DST_BUCKET_3']
    
    else:
        # Manual test event
        src_bucket = os.environ['SRC_BUCKET']
        dst_bucket_1 = os.environ['DST_BUCKET_1']
        dst_bucket_2 = os.environ['DST_BUCKET_2']
        dst_bucket_3 = os.environ['DST_BUCKET_3']

    print(f"SRC_BUCKET : {src_bucket},DST_BUCKET1 : {dst_bucket_1}, DST_BUCKET2 : {dst_bucket_2},DST_BUCKET3 : {dst_bucket_3}\n----------------------------")


    dst_files_list_1 = s3.list_objects_v2(Bucket=dst_bucket_1, Delimiter = '/' , Prefix ='incoming_family/')
    dst_files_list_2 = s3.list_objects_v2(Bucket=dst_bucket_2, Delimiter = '/' , Prefix ='incoming_family/')
    dst_files_list_3 = s3.list_objects_v2(Bucket=dst_bucket_3, Delimiter = '/' , Prefix ='eMDM/')
    dst_file_names_1, dst_file_names_2, dst_file_names_3 = [], [], []
        
    dst_file_names_1 = [obj['Key'].split('/')[-1] for obj in dst_files_list_1.get('Contents', [])]
    dst_file_names_2 = [obj['Key'].split('/')[-1] for obj in dst_files_list_2.get('Contents', [])]
    dst_file_names_3 = [obj['Key'].split('/')[-1] for obj in dst_files_list_3.get('Contents', [])]
    
    print("Starting the process...")

    print(src_files_list)
    for file_nm in src_files_list:
        if file_nm.endswith('/') or '.' not in file_nm or 'transfer_family/' not in file_nm:
            print("Skipping the folders and files without extension")
            continue

        name=((file_nm.split('/'))[-1]).split('.')[0]
        ext=((file_nm.split('/'))[-1]).split('.')[1]
        print(f"{name}.{ext}")

        copy_Source = {
                "Bucket" : src_bucket,
                "Key" : str(file_nm)
            }

        print(copy_Source)

        try:
            response = s3.head_object(Bucket=src_bucket, Key=file_nm)
            last_modified = response['LastModified']
        except Exception as e:
            print("Error in fetching the metadata of file i.e., last_modified.")
            continue

        #1st filetypes Copy       
        if 'transfer_family/dst1/' in file_nm:
            revise_file_name = name+str(last_modified.strftime('%Y%m%d'))+str('.')+ext
            if revise_file_name not in dst_file_names_1:
                copy_logic(s3,copy_Source,src_bucket, dst_bucket_1, revise_file_name, file_nm)
            else:
                alert_var = 1
                msg= alert(s3,copy_Source,dst_bucket_1, name, ext, last_modified)
                messages.append(msg)

        #2nd filetypes Copy
        elif 'transfer_family/dst2/' in file_nm:

            if '_' in ext:
                ext=ext.split('_')[0]
            revise_file_name = name+str('_')+str(last_modified.strftime('%Y%m%d%H%M%S'))+str('.')+ext

            if revise_file_name not in dst_file_names_2:
                copy_logic(s3, copy_Source, src_bucket, dst_bucket_2, revise_file_name, file_nm)
            else:
                alert_var = 1
                msg = alert(s3,copy_Source,dst_bucket_2, name, ext, last_modified)
                messages.append(msg)

        #3rd filetypes Copy       
        elif 'transfer_family/dst3/' in file_nm:
            revise_file_name = name+str('_')+str(last_modified.strftime('%Y%m%d%H%M%S'))+str('.')+ext
            if revise_file_name not in dst_file_names_3:
                copy_logic(s3,copy_Source,src_bucket, dst_bucket_3, revise_file_name, file_nm, sub_folder='eMDM/')
            else:
                alert_var = 1
                msg= alert(s3,copy_Source,dst_bucket_3, name, ext, last_modified, sub_folder='eMDM/')
                messages.append(msg)

    if alert_var != 0:
        messages.append(f"\nIt copied to archive folder in destination bucket.\n")
        messages = list(dict.fromkeys(messages))
        sns.publish(
        TopicArn=sns_topic_arn,
        Subject="S3 File Conflict Alert",
        Message="\n".join(messages)
    )          

def alert(s3,copy_Source,dst_bucket_nm, name,ext, last_modified, sub_folder='incoming_family/'):
    revise_file_name=name+str(last_modified.strftime('%Y%m%d'))+str('_')+str(last_modified.strftime('%H%M%S'))+str('.')+ext
    print(f"File {revise_file_name} already exist in {dst_bucket_nm}/{sub_folder}, unable to load, just copying it to archive")
    s3.copy(copy_Source,dst_bucket_nm,sub_folder+"archive/"+revise_file_name)
    return f"File {revise_file_name} already exists in {dst_bucket_nm}/{sub_folder}"

def copy_logic(s3,copy_Source,src_bucket_nm,dst_bucket_nm, revise_file_name, key, sub_folder='incoming_family/'):
    s3.copy(copy_Source,dst_bucket_nm,sub_folder+revise_file_name)
    s3.copy(copy_Source,dst_bucket_nm, sub_folder+"archive/"+revise_file_name)
    print(f"Copied the file {src_bucket_nm}->{key} to {dst_bucket_nm}->{sub_folder}->{revise_file_name}")

    print(f"Copied the file {src_bucket_nm}->{key} to {dst_bucket_nm}->{sub_folder}->archive->{revise_file_name}","\n")

