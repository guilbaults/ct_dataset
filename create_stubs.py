#!/usr/bin/python
import boto3
import sys
import os
import xattr
from botocore.handlers import disable_signing

dataset = 'nyc-tlc'
lustre_path = '/mnt/client/datasets/'

s3 = boto3.resource('s3')
s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
bucket = s3.Bucket(dataset)

for bucket_object in bucket.objects.all():
    if bucket_object.size != 0:
        obj_path = lustre_path + "/" + dataset + "/" + bucket_object.key
        dir_path = os.path.dirname(obj_path)
        if os.path.isdir(dir_path) is False:
            os.makedirs(dir_path)

        with open(obj_path, "wb") as stub:
            stub.truncate(bucket_object.size)

        xattr.setxattr(obj_path, 'trusted.lhsm.type',
                       'aws_s3'.encode())
        xattr.setxattr(obj_path, 'trusted.lhsm.s3_bucket',
                       dataset.encode())
        xattr.setxattr(obj_path, 'trusted.lhsm.s3_key',
                       bucket_object.key.encode())

        # need to be done on each file, until I can figure out how to create
        # a native stub
        # find /mnt/client/datasets/ -type f -exec lfs hsm_archive "{}" \;
        # find /mnt/client/datasets/ -type f -exec lfs hsm_release "{}" \;
