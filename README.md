# s3booster
s3booster is developed to increase the download/upload/restore performance of S3 objects by direcotry.
It's using multiprocessing to be executed in parallel, you can adjust concurrent processes with *max_process* variable.
Also every utility is using paginator or similar mechanism to overcome 1000 objects limit of view. 

## How to Use It
### requirement
- python3.4 +
- boto3
### command
1. edit variables
```shell
# variables
region = 'ap-northeast-2' ## change it with your region
bucket_name = 'your-own-bucket' ## change it with bucket name
prefix_list = ['data1/' ## common prefix name of S3 object
// input_list= [prefix+'dir01/', prefix+'dir02/'] ## sub_prefix name
local_dir = '/download_here/'  ## local directory to save files 
max_process = 256  ## number of concurrent processes 
```

2. run script
```
python3 s3booster_{upload_dir | download | restore_version}.py

```
3. result
when I performed downloading test with 1545 files, it took 15 sec. 
```
[dubug] downloading object: D002/2021-06-26/data4/d2100/file8336 to /download/D002/2021-06-26/data4/d2100/file8336
[dubug] downloading object: D002/2021-06-26/data4/d2100/file9312 to /download/D002/2021-06-26/data4/d2100/file9312
[dubug] downloading object: D002/2021-06-26/data4/d2100/file7357 to /download/D002/2021-06-26/data4/d2100/file7357
Duration: 0:00:15.046753
Total File numbers: 1545
ended
```

With 10001 files, it  took 42 sec.
```
Duration: 0:00:42.204100
Total File numbers: 10001
ended
```
