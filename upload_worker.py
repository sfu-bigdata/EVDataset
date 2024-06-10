import configparser
import boto3
import daemon, sys, os, time, threading, glob
from ChargePointDatasetUtils import get_logger

cwd = os.getcwd()
config = configparser.ConfigParser()
config.read('config.ini')

def upload_files(client):
    
    bucket_name = 'ieee-dataport'
    prefix = 'open/27422/11280/'

    logger = get_logger('Upload Worker', cwd, 'log/upload.log')
    upload_frequency = int(config.get('Parameters', 'upload_frequency'))
    
    file_list = glob.glob(f'{cwd}/data/*')
    logs = glob.glob(f'{cwd}/log/*')
    file_list.extend(logs)
    
    while True:
        try:
            for file in file_list:
                file_name = file.split('/')[-1]
                file_key = f'{prefix}{file_name}'
                client.upload_file(file, bucket_name, file_key)
                logger.info(f"File uploaded: {file_name}")
            
            time.sleep(upload_frequency)
    
        except Exception as e:
            logger.error(f"An exception occurred: {str(e)}")
        
    

        
def start_upload_worker(client):
    
    th = threading.Thread(target=upload_files, args=(client, ))
    th.start()
    th.join()
    


if __name__ == '__main__':
    with daemon.DaemonContext(stdout=sys.stdout) as context:
        print(os.getpid())

        try:
            access_key, secret = [v for k, v in config.items('DataPort')]
            s3_client = boto3.client(
                    's3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret
                )
            start_upload_worker(s3_client)
        
        except Exception as e:
            print(f"An exception occurred: {str(e)}")

        
