
import os
import re
import json
import os
import logging4
import boto3
# from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

def load():
    # filename = os.path.basename(filepath)
    # print(filename)
    try:
        dir_path = '/Users/parkdaekyu/Downloads/logs'
        for filenames in os.listdir(dir_path):
            print('------------------------',filenames,'------------------------')
            with open("output.txt", "w") as fwrite:
                with open(dir_path + '/' + filenames, 'r') as file:
                    for line in file:
                        # print(line)
                        match = re.search(r"/api/v1/orders/+\d+\"", line)
                        if match:
                            log_dict = json.loads(line)
                            url = log_dict['url']
                            substring = "com"
                            index = url.find(substring) + len(substring) + 1
                            service = url[index:]
                            user_id = log_dict['user_id']
                            user_type = getUserType(log_dict['user_type'])
                            time = log_dict['time']
                            method = log_dict['method']
                            
                            if method == 'GET':
                            # print(service, user_id, user_type, time)
                                query = "INSERT INTO logispot_develop.log_location_accesses (user_id, user_type, origin_id, origin_type, origin_source, device, driver_location_id, provide_method, provide_service,created_at, updated_at) VALUES\
('"+str(user_id)+"', '"+str(user_type)+"', 0,'App\\\\Model\\\\User\\\\UserDriver', 'DriverApp', 'web', null,'"+str(method)+"', '"+str(service)+"', '"+str(time)+"', '"+str(time)+"');\n"
                                fwrite.writelines(query)
    except Exception as e:
            print("The error is: ",e)

def getUserType(userType):
    if userType == 1:
        result = "App\\\\Model\\\\User\\\\UserClient"
    elif userType == 2:
        result = "App\\\\Model\\\\User\\\\UserCarrier"
    elif userType == 3:
        result = "App\\\\Model\\\\User\\\\UserDriver"
    elif userType == "1":
        result = "App\\\\Model\\\\User\\\\UserClient"
    elif userType == "2":
        result = "App\\\\Model\\\\User\\\\UserCarrier"
    elif userType == "3":
        result = "App\\\\Model\\\\User\\\\UserDriver"
    else:
        result = "Invalid choice"
    return result

def getAwsS3():
    session = boto3.Session(
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key',
        region_name='your_region_name'
    )

    AWS_ACCESS_KEY_ID = os.environ.get('AWS_S3_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_S3_SECRET_ACCESS_KEY')
    REGION_NAME = os.environ.get('AWS_S3_DEFAULT_REGION')
    s3_client = boto3.client(
        service_name='s3', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    filename = 'output.txt'
    bucket = os.environ.get('AWS_S3_BUCKET')
    object_name = 'hello_world_in_s3.txt'
    object_key = 'laravel_logs/2019-06-05/ls.s3.00c46d75-0e11-4feb-a9d2-b8cbe29d6b06.2019-06-05T11.04.part16.txt'
    local_file_path = '/Users/parkdaekyu/Downloads/logs/'
    prefix = 'laravel_logs/'
    condition = '2022-04-11'
    try:        
        # response = s3_client.get_object(Bucket=bucket, Key=object_key)
        # content = response['Body'].read().decode('utf-8')
        
        # file_list = []
        # response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        # for obj in response['Contents']:
        #     file_list.append(obj['Key'])

        # response = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")
        # print(response['CommonPrefixes'])

        list = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        for obj in list.get('CommonPrefixes'):
            directory = obj.get('Prefix');
            print('directory====',directory, directory[13:-1], condition < directory[13:-1])
            if condition < directory[13:-1]:
                with open(local_file_path+directory[:-1], "w") as fwrite:
                    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=directory)
                    for obj in objects['Contents']:
                        print('files=======>',obj['Key'])
                        object = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                        content = object['Body'].read().decode('utf-8')
                        fwrite.write(content)
                    
                with open(local_file_path+directory[:-1]+'.sql', "w") as qwrite:
                    with open(local_file_path+directory[:-1], 'r') as fread:
                        for line in fread:
                            match = re.search(r"/api/v1/orders/+\d+\"", line)
                            if match:
                                log_dict = json.loads(line)
                                url = log_dict['url']
                                substring = "com"
                                index = url.find(substring) + len(substring) + 1
                                service = url[index:]
                                user_id = log_dict['user_id']
                                user_type = getUserType(log_dict['user_type'])
                                time = log_dict['time']
                                method = log_dict['method']
                                
                                if method == 'GET':
                                # print(service, user_id, user_type, time)
                                    query = "INSERT INTO logispot_develop.log_location_accesses (user_id, user_type, origin_id, origin_type, origin_source, device, driver_location_id, provide_method, provide_service,created_at, updated_at) VALUES ('"+str(user_id)+"', '"+str(user_type)+"', 0,'App\\\\Model\\\\User\\\\UserDriver', 'DriverApp', 'web', null,'"+str(method)+"', '"+str(service)+"', '"+str(time)+"', '"+str(time)+"');\n"
                                    qwrite.writelines(query)
        # response = s3_client.list_objects_v2(Bucket=bucket, Prefix='', Delimiter='/')
        # for content in response.get('CommonPrefixes', []):
        #     yield content.get('Prefix')
        
        # s3_client.download_file(bucket, object_key, local_file_path)
        
        # response = s3_client.upload_file(filename, bucket, object_name)
        # print("The success is: ",content)
    except Exception as e:
            print("The error is: ",e)
    
if __name__ == '__main__':
    try:
        print("start process")
        getAwsS3()
        # load()
    except:
        print("stop process")
        