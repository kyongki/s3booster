#!/bin/env python3
'''
** Chaveat: not suitable for millions of files, it shows slow performance to get object list
ChangeLogs
- 2021.08.03:
  - support multiprocessing(spawn) 
  - fixing windows path delimeter (\)
  - support compatibility of file name between MAC and Windows 
- 2021.08.02:
  - adding logger
  - fixing error on python3.8, multiprocessing.set_start_method("fork")
    - https://github.com/pytest-dev/pytest-flask/issues/104
- 2021.08.01: adding uploader feature
- 2021.07.24: 
- 2021.07.23: applying multiprocessing.queue + process instead of pool
- 2021.07.21: modified getObject function
  - for parallel processing, multiprocessing.Pool used
  - used bucket.all instead of paginator
- 2021.07.20: first created
'''

#requirement
## python 3.7+ (os.name)
## boto3
## preferred os: linux (mac, windows works as well, but performance is slower)

import os
import boto3
import botocore
import multiprocessing
from os import path, makedirs
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import logging
import time
import unicodedata

#region = 'us-east-2' ## change it with your region
prefix_list = ['/data/']  ## Don't forget to add last slash '/'
##Common Variables
region = 'ap-northeast-2' ## change it with your region
bucket_name = 'your-own-bucket'
max_process = 256
endpoint='https://s3.'+region+'.amazonaws.com'
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR
# CMD variables
cmd='upload_dir' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
# end of variables ## you don't need to modify below codes.

errorlog_file = 'error.log'
successlog_file = 'success.log'
quit_flag = 'DONE'
if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# S3 session
#s3_client = boto3.client('s3')
s3 = boto3.resource('s3',endpoint_url=endpoint, region_name=region)
bucket = s3.Bucket(bucket_name)
# setup logger
def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)
## define logger
setup_logger('error', errorlog_file, level=log_level)
setup_logger('success', successlog_file, level=log_level)
error_l = logging.getLogger('error')
success_l = logging.getLogger('success')
# execute multiprocessing
def run_multip(max_process, exec_func, q):
    p_list = []
    for i in range(max_process):
        p = multiprocessing.Process(target = exec_func, args=(q,))
        p_list.append(p)
        p.daemon = True
        p.start()
    return p_list

def finishq(q, p_list):
    for j in range(max_process):
        q.put(quit_flag)
    for pi in p_list:
        pi.join()

# get files to upload
def upload_get_files(sub_prefix, q):
    num_obj=0
   # get all files from given directory
    for r,d,f in os.walk(sub_prefix):
        for file in f:
            file_name = os.path.join(r,file)
            # support compatibility of MAC and windows
            file_name = unicodedata.normalize('NFC', file_name)
            if os.name == 'nt':
                obj_name = file_name.replace(sub_prefix,'',1).replace('\\', '/')
            else:
                obj_name = file_name.replace(sub_prefix,'',1)
            mp_data = tuple([file_name, obj_name])
            success_l.debug('get_file mp_data: %s', mp_data)
            try:
                q.put(mp_data)
            except ClientError as e:
                error_l.info('client error: putting %s into queue %s is failed' % file_name)
                error_l.info(e)
            except Exception as e:
                error_l.info('exception error: putting %s into queue %s is failed' % file_name)
                error_l.info(e)
            num_obj+=1
            #time.sleep(0.1)
    q.put(quit_flag)
    #q.close()
    #q.join_thread()
    return num_obj

def upload_file(q):
    while True:
        mp_data = q.get()
        if mp_data == quit_flag:
            break
        file_name = mp_data[0] # filename in local
        obj_name = mp_data[1] # object name in S3
        try:
            time.sleep(1)
            response = bucket.upload_file(file_name, obj_name)
            success_l.info('%s is uploaded' % file_name)
        except ClientError as e:
            error_l.info('client error: %s is failed, obj name: %s' % (file_name, obj_name))
            error_l.info(e)
        except Exception as e:
            error_l.info('exception error: %s is failed, obj name: %s' % (file_name, obj_name))
            error_l.info(e)
        #return 0 ## for the dubug, it will pause with error
def upload_file_multi(s3_dirs):
    q = multiprocessing.Queue()
    total_obj = 0
    for s3_dir in s3_dirs:
        success_l.info('%s directory is uploading' % s3_dir)
        p_list = run_multip(max_process, upload_file, q)
        # get object list and ingest to processes
        num_obj = upload_get_files(s3_dir, q)
        # sending quit_flag and join processes
        finishq(q, p_list)
        success_l.info('%s directory is uploaded' % s3_dir)
        total_obj += num_obj
    return total_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")
# start main function
if __name__ == '__main__':
    #print("starting script...")
    start_time = datetime.now()
    s3_dirs = prefix_list
    if cmd == 'upload_dir':
        total_files = upload_file_multi(s3_dirs)
    else:
        s3_booster_help

    end_time = datetime.now()
    success_l.info('====================================')
    #for d in down_dir:
    #    stored_dir = local_dir + d
    #    print("[Information] Download completed, data stored in %s" % stored_dir)
    success_l.info('Duration: {}'.format(end_time - start_time))
    success_l.info('Total File numbers: %d' % total_files)
    success_l.info('S3 Endpoint: %s' % endpoint)
    success_l.info('End')
