import boto3

# S3 list all keys with the prefix 'data1/'
s3 = boto3.resource('s3')
bucket_name = 'your-own-bucket'
bucket = s3.Bucket(bucket_name)
for obj in bucket.objects.filter(Prefix='day1/data1/'):
    print('{0}:{1}'.format(bucket.name, obj.key))
