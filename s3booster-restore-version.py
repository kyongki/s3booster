#!/bin/env python3
'''
** Chaveat: not suitable for millions of files, it shows slow performance to get object list
ChangeLogs
- 2021.07.24: 
- 2021.07.23: applying multiprocessing.queue + process instead of pool
- 2021.07.21: modified getObject function
  - for parallel processing, multiprocessing.Pool used
  - used bucket.all instead of paginator
- 2021.07.20: first created
'''

#requirement
## python 3.4+
## boto3

import os
import boto3
import botocore
import multiprocessing
from os import path, makedirs
from datetime import datetime, timezone
from botocore.exceptions import ClientError

#region = 'us-east-2' ## change it with your region
prefix_list = ['data1/']
##Common Variables
region = 'ap-northeast-2' ## change it with your region
bucket_name = 'your-bucket-versioned'
max_process = 512
endpoint='https://s3.'+region+'.amazonaws.com'
debug_en = False
# CMD variables
cmd='restore_obj_version' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
#cmd='del_obj_version' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
#restore_deleted_time = datetime(2019, 10, 22, 20, 0, 0, tzinfo=timezone.utc)

# end of variables ## you don't need to modify below codes.
quit_flag = 'DONE'
if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# S3 session
#s3 = boto3.client('s3', region)
s3 = boto3.resource('s3',endpoint_url=endpoint, region_name=region)
bucket = s3.Bucket(bucket_name)

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

# restore versioned objects 
def restore_get_obj_delmarker(sub_prefix, q):
    num_obj=0
    s3_client = boto3.client('s3')

   # using paginator to iterate over 1000 keys
    paginator = s3_client.get_paginator('list_object_versions')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=sub_prefix)
    for page in pages:
        if 'DeleteMarkers' in page.keys():
            for delmarker_obj in page['DeleteMarkers']:
                key = delmarker_obj['Key']
                vid = delmarker_obj['VersionId']
                mp_data = (key, vid)
                #print('restore mp_data:', mp_data)
                q.put(mp_data)
                num_obj+=1
        else:
            print('no delmarker')
    q.put(quit_flag)
    return num_obj

def restore_obj_version(q):
    while True:
        mp_data = q.get()
        if mp_data == quit_flag:
            break
        key = mp_data[0] # keyname
        vid = mp_data[1] # versionid
        try:
            obj_version = s3.ObjectVersion(bucket_name, key, vid)
            obj_version.delete()
            print("[dubug2] object(%s, %s) is restored" %(key, vid))
        except Exception as e:
            print("[warning] restoring object(%s, %s) is failed" % (key, vid))
            print(e)
def restore_obj_version_multi(s3_dirs):
    q = multiprocessing.Queue()
    total_obj = 0
    for s3_dir in s3_dirs:
        # multiprocessing tasks
        print("[Information] %s directory is restoring" % s3_dir)
        p_list = run_multip(max_process, restore_obj_version, q)
        # get object list and ingest to processes
        num_obj = restore_get_obj_delmarker(s3_dir, q)
        # sending quit_flag and join processes
        finishq(q, p_list)
        print("[Information] %s is restored" % s3_dir)
        total_obj += num_obj
    return total_obj

def s3_booster_help():
    print("example: python3 s3_restore_latest_version.sh")
# start main function
if __name__ == '__main__':
    #print("starting script...")
    start_time = datetime.now()
    s3_dirs = prefix_list
    if cmd == 'restore_obj_version':
        total_files = restore_obj_version_multi(s3_dirs)
    else:
        s3_booster_help

    end_time = datetime.now()
    print('=============================================')
    #for d in down_dir:
    #    stored_dir = local_dir + d
    #    print("[Information] Download completed, data stored in %s" % stored_dir)
    print('Duration: {}'.format(end_time - start_time))
    print('Total File numbers: %d' % total_files)
    print('S3 Endpoint: %s' % endpoint)
    print("ended")
