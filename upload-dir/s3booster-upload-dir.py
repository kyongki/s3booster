#!/bin/env python3
'''
** Chaveat: not suitable for millions of files, it shows slow performance to get object list
ChangeLogs
- 2021.08.13
  - add arguments
  - adding conv_obj_name()
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
import argparse

## treating arguments
parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', help='your bucket name e) your-bucket', action='store', required=True)
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True)
#parser.add_argument('--region', help='aws_region e) ap-northeast-2', action='store')
parser.add_argument('--endpoint', help='snowball endpoint e) http://10.10.10.10:8080 or https://s3.ap-northeast-2.amazonaws.com', action='store', default='https://s3.ap-northeast-2.amazonaws.com', required=True)
parser.add_argument('--profile_name', help='aws_profile_name e) sbe1', action='store', default='default')
parser.add_argument('--prefix_root', help='prefix root e) dir1/', action='store', default='')
parser.add_argument('--max_process', help='NUM e) 256', action='store', default=256, type=int)
args = parser.parse_args()

prefix_list = args.src_dir  ## Don't forget to add last slash '/'
prefix_root = args.prefix_root ## Don't forget to add last slash '/'
##Common Variables
bucket_name = args.bucket_name
profile_name = args.profile_name
endpoint = args.endpoint
max_process = args.max_process
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR

#region = 'us-east-2' ## change it with your region
#prefix_list = ['/data/']  ## Don't forget to add last slash '/'
###Common Variables
#region = 'ap-northeast-2' ## change it with your region
#bucket_name = 'your-own-bucket'
#max_process = 256
#endpoint='https://s3.'+region+'.amazonaws.com'
#log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR

## End of user variable
# CMD variables
cmd='upload_dir' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
# create log directory
try:
    os.makedirs('log')
except: pass
errorlog_file = 'log/error-%s.log' % current_time
successlog_file = 'log/success-%s.log' % current_time
filelist_file = 'log/filelist-%s.log' % current_time
quit_flag = 'DONE'
if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# S3 session
#s3_client = boto3.client('s3')
s3 = boto3.resource('s3',endpoint_url=endpoint)
bucket = s3.Bucket(bucket_name)
# setup logger
def setup_logger(logger_name, log_file, level=logging.INFO, sHandler=False):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    if sHandler:
        l.addHandler(streamHandler)
    else:
        pass
## define logger
setup_logger('error', errorlog_file, level=log_level, sHandler=True)
setup_logger('success', successlog_file, level=log_level, sHandler=True)
setup_logger('filelist', filelist_file, level=log_level, sHandler=False)
error_log = logging.getLogger('error')
success_log = logging.getLogger('success')
filelist_log = logging.getLogger('filelist')

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
# convert object name
def conv_obj_name(file_name, prefix_root, sub_prefix):
    if sub_prefix[-1] != '/':
        sub_prefix = sub_prefix + '/'
    if prefix_root[-1] != '/':
        prefix_root = prefix_root + '/'
    if os.name == 'nt':
        obj_name = prefix_root + file_name.replace(sub_prefix,'',1).replace('\\', '/')
    else:
        obj_name = prefix_root + file_name.replace(sub_prefix,'',1)
    return obj_name

# get files to upload
def upload_get_files(sub_prefix, q):
    num_obj=0
   # get all files from given directory
    for r,d,f in os.walk(sub_prefix):
        for file in f:
            file_name = os.path.join(r,file)
            # support compatibility of MAC and windows
            #file_name = unicodedata.normalize('NFC', file_name)
            obj_name = conv_obj_name(file_name, prefix_root, sub_prefix)
            mp_data = tuple([file_name, obj_name])
            success_log.debug('get_file mp_data: %s', mp_data)
            try:
                q.put(mp_data)
            except ClientError as e:
                error_log.info('client error: putting %s into queue %s is failed' % file_name)
                error_log.info(e)
            except Exception as e:
                error_log.info('exception error: putting %s into queue %s is failed' % file_name)
                error_log.info(e)
            num_obj+=1
            #time.sleep(0.1)
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
            response = bucket.upload_file(file_name, obj_name)
            success_log.info('%s is uploaded' % file_name)
            filelist_log.info('%s , %s' % (file_name, obj_name))
        except ClientError as e:
            error_log.info('client error: %s is failed, obj name: %s' % (file_name, obj_name))
            error_log.info(e)
        except Exception as e:
            error_log.info('exception error: %s is failed, obj name: %s' % (file_name, obj_name))
            error_log.info(e)
        #return 0 ## for the dubug, it will pause with error
def upload_file_multi(src_dir):
    q = multiprocessing.Queue()
    success_log.info('%s directory is uploading' % src_dir)
    p_list = run_multip(max_process, upload_file, q)
    # get object list and ingest to processes
    num_obj = upload_get_files(src_dir, q)
    # sending quit_flag and join processes
    finishq(q, p_list)
    success_log.info('%s directory is uploaded' % src_dir)
    return num_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")

# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)

# start main function
if __name__ == '__main__':
    #print("starting script...")
    start_time = datetime.now()
    src_dir = prefix_list
    check_srcdir(src_dir)
    if cmd == 'upload_dir':
        total_files = upload_file_multi(src_dir)
    else:
        s3_booster_help

    end_time = datetime.now()
    success_log.info('====================================')
    #for d in down_dir:
    #    stored_dir = local_dir + d
    #    print("[Information] Download completed, data stored in %s" % stored_dir)
    success_log.info('Duration: {}'.format(end_time - start_time))
    success_log.info('Total File numbers: %d' % total_files)
    success_log.info('S3 Endpoint: %s' % endpoint)
    success_log.info('End')
