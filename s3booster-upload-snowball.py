#!/bin/env python3
'''
ChangeLogs
- 2021.08.05:
  - this utility will copy files from local filesystem to SnowballEdge in parallel  
  - snowball_uploader alternative
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
## preferred os: linux (Windows works as well, but performance is slower)

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
import random
import string
import math
import io
import tarfile

#region = 'us-east-2' ## change it with your region
#Below prefix will download from /data/dir1/* on filesystem to fs1/* on S3
prefix_list = ['/data/dir1/']  ## Don't forget to add last slash '/'
prefix_root = 'dir1/' ## Don't forget to add last slash '/'
#prefix_root = '' ## This is for the all files and directories under /data/dir1/
##Common Variables
bucket_name = 'your-own-bucket'
profile_name = 'sbe1'
#region = 'ap-northeast-2' ## change it with your region
#endpoint='https://s3.'+region+'.amazonaws.com'
endpoint='http://192.168.10.10:8080'
max_process = 5
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR
# end of user variables ## you don't need to modify below codes.
##### Optional variables
## begin of snowball_uploader variables
s3_client_class = 'STANDARD' ## value is fixed, snowball only transferred to STANDARD class
max_tarfile_size = 10 * (1024 ** 3) # 10GiB, 100GiB is max limit of snowball
max_part_size = 300 * (1024 ** 2) # 100MB, 500MiB is max limit of snowball
min_part_size = 5 * 1024 ** 2 # 16MiB for S3, 5MiB for SnowballEdge
max_part_count = int(math.ceil(max_tarfile_size / max_part_size))
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
# CMD variables
cmd='upload_dir' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
errorlog_file = 'log/error.log' 
successlog_file = 'log/success.log'
quit_flag = 'DONE'
# End of Variables

if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# S3 session
#s3_client = boto3.client('s3')
session = boto3.Session(profile_name=profile_name)
s3_client = session.client('s3', endpoint_url=endpoint)

# defining function
## setup logger
def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    #l.addHandler(streamHandler)
## define logger
setup_logger('error', errorlog_file, level=log_level)
setup_logger('success', successlog_file, level=log_level)
error_log = logging.getLogger('error')
success_log = logging.getLogger('success')

## code from snowball_uploader
def create_mpu(key_name):
    mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=key_name, StorageClass=s3_client_class, Metadata={"snowball-auto-extract": "true"})
    mpu_id = mpu["UploadId"]
    return mpu_id

def upload_mpu(key_name, mpu_id, data, index, parts):
    #part = s3_client.upload_part(Body=data, Bucket=bucket_name, Key=key_name, UploadId=mpu_id, PartNumber=index, ContentLength=max_buf_size)
    part = s3_client.upload_part(Body=data, Bucket=bucket_name, Key=key_name, UploadId=mpu_id, PartNumber=index)
    parts.append({"PartNumber": index, "ETag": part["ETag"]})
    success_log.debug('parts list: %s' % str(parts))
    return parts

def complete_mpu(key_name, mpu_id, parts):
    result = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        UploadId=mpu_id,
        MultipartUpload={"Parts": parts})
    return result

def adjusting_parts_order(mpu_parts):
    return sorted(mpu_parts, key=lambda item: item['PartNumber'])

def buf_fifo(buf):
    tmp_buf = io.BytesIO()            # added for FIFO operation
    tmp_buf.write(buf.read())    # added for FIFO operation
    #print ('3. before fifo, recv_buf_size: %s' % len(buf.getvalue()))
    #print('3.before fifo, recv_buf_pos : %s' % buf.tell())
    buf.seek(0,0)
    buf.truncate(0)
    tmp_buf.seek(0,0)
    buf.write(tmp_buf.read())
    return buf

def copy_to_snowball(tar_name, org_files_list):
    delimeter = ' ,'
    tar_file_size = 0
    recv_buf = io.BytesIO()
    mpu_id = create_mpu(tar_name)
    parts_index = 1
    parts = []
    with tarfile.open(fileobj=recv_buf, mode="w") as tar:
        for file_name, obj_name, file_size in org_files_list:
            if os.path.isfile(file_name):
                tar.add(file_name, arcname=obj_name)
                #success_log.debug('1. recv_buf_size: %s' % len(recv_buf.getvalue()))
                success_log.info(file_name + delimeter + obj_name + delimeter + str(file_size)) #kyongki
                recv_buf_size = recv_buf.tell()
                #success_log.debug('1. recv_buf_pos: %s' % recv_buf.tell())
                if recv_buf_size > max_part_size:
                    print('multi part uploading:  %s / %s , size: %s bytes' % (parts_index, max_part_count, recv_buf_size))
                    chunk_count = int(recv_buf_size / max_part_size)
                    tar_file_size = tar_file_size + recv_buf_size
                    #print('%s is accumulating, size: %s byte' % (tar_name, tar_file_size))
                    for buf_index in range(chunk_count):
                        start_pos = buf_index * max_part_size
                        recv_buf.seek(start_pos,0)
                        mpu_parts = upload_mpu(tar_name, mpu_id, recv_buf.read(max_part_size), parts_index, parts)
                        parts_index += 1
                    ####################
                    buf_fifo(recv_buf)
                    recv_buf_size = recv_buf.tell()
                    #print('3.after fifo, recv_buf_pos : %s' % recv_buf.tell())
                    #print ('3. after fifo, recv_buf_size: %s' % len(recv_buf.getvalue()))
                else:
                    pass
                    #print('accumulating files...')
            else:
                error_log.info(file_name,' does not exist\n')
                print (file_name + ' is not exist...............................................\n')
    recv_buf.seek(0,0)
    mpu_parts = upload_mpu(tar_name, mpu_id, recv_buf.read(), parts_index, parts)
    parts_index += 1
    mpu_parts = adjusting_parts_order(mpu_parts)
    complete_mpu(tar_name, mpu_id, mpu_parts)
    ### print metadata
    meta_out = s3_client.head_object(Bucket=bucket_name, Key=tar_name)
    print('metadata info: %s\n' % str(meta_out))
    #print ('\n tar file: %s \n' % tar_name)
    print('%s is uploaded successfully\n' % tar_name)
## end of code from snowball_uploader

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set*6, 6)))
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
    sum_size = 0
    org_files_list = []
   # get all files from given directory
    for r,d,f in os.walk(sub_prefix):
        for file in f:
            try:
                file_name = os.path.join(r,file)
                # support compatibility of MAC and windows
                file_name = unicodedata.normalize('NFC', file_name)
                if os.name == 'nt':
                    obj_name = prefix_root + file_name.replace(sub_prefix,'',1).replace('\\', '/')
                else:
                    obj_name = prefix_root + file_name.replace(sub_prefix,'',1)
                f_size = os.stat(file_name).st_size                
                file_info = (file_name, obj_name, f_size)
                org_files_list.append(file_info)
                sum_size = sum_size + f_size
                if max_tarfile_size < sum_size:
                    sum_size = 0
                    mp_data = org_files_list
                    org_files_list = []
                    try:
                        # put files into queue in max_tarfile_size
                        q.put(mp_data)
                        success_log.debug('0, sending mp_data size: %s'% len(mp_data))
                        success_log.debug('0, sending mp_data: %s'% mp_data)
                    except Exception as e:
                        error_log.info('exception error: putting %s into queue is failed' % file_name)
                        error_log.info(e)
                num_obj+=1
            except Exception as e:
                error_log.info('exception error: getting %s file info is failed' % file_name)
                error_log.info(e)
            #time.sleep(0.1)
    try:
        # put remained files into queue
        mp_data = org_files_list
        q.put(mp_data)
        success_log.debug('1, sending mp_data size: %s'% len(mp_data))
        success_log.debug('1, sending mp_data: %s'% mp_data)
    except Exception as e:
        error_log.info('exception error: putting %s into queue is failed' % file_name)
        error_log.info(e)
    q.put(quit_flag)
    return num_obj

def upload_file(q):
    while True:
        mp_data = q.get()
        org_files_list = mp_data
        randchar = str(gen_rand_char())
        tar_name = ('snowball-%s-%s.tar' % (randchar, current_time))
        success_log.debug('receving mp_data size: %s'% len(org_files_list))
        success_log.debug('receving mp_data: %s'% org_files_list)
        if mp_data == quit_flag:
            break
        try:
            copy_to_snowball(tar_name, org_files_list)
            #print('%s is uploaded' % tar_name)
        except Exception as e:
            error_log.info('exception error: %s uploading failed' % tar_name)
            error_log.info(e)
        #return 0 ## for the dubug, it will pause with error
        
def upload_file_multi(s3_dirs):
    total_obj = 0
    for s3_dir in s3_dirs:
        success_log.info('%s directory is uploading' % s3_dir)
        p_list = run_multip(max_process, upload_file, q)
        # get object list and ingest to processes
        num_obj = upload_get_files(s3_dir, q)
        # sending quit_flag and join processes
        finishq(q, p_list) 
        success_log.info('%s directory is uploaded' % s3_dir)
        total_obj += num_obj
    return total_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")
# start main function
if __name__ == '__main__':
    # define simple queue
    q = multiprocessing.Queue()
    # create log directory
    try:
        os.makedirs('log')
    except: pass
    
    #print("starting script...")
    start_time = datetime.now()
    s3_dirs = prefix_list
    if cmd == 'upload_dir':
        total_files = upload_file_multi(s3_dirs)
    else:
        s3_booster_help

    end_time = datetime.now()
    print('====================================')
    #for d in down_dir:
    #    stored_dir = local_dir + d
    #    print("[Information] Download completed, data stored in %s" % stored_dir)
    print('Duration: {}'.format(end_time - start_time))
    print('Total File numbers: %d' % total_files) #kyongki
    print('S3 Endpoint: %s' % endpoint)
    print('End')
