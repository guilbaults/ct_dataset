#!/usr/bin/python
import argparse
import xattr
import sys
import time
import os.path
import boto3
import logging
from botocore.handlers import disable_signing

parser = argparse.ArgumentParser()
parser.add_argument('--fd', type=int)
parser.add_argument('--fid', required=True)
parser.add_argument("--lustre-root", required=True)

group_action = parser.add_mutually_exclusive_group(required=True)
group_action.add_argument('--restore', action='store_true',
                          help="Retrieve the content from the dataset")

parser.add_argument('--verbose', '-v', action='count')


args = parser.parse_args()


def fid2lupath(lustre_root, fid):
    return "{lustre_root}/.lustre/fid/{fid}".format(
        lustre_root=args.lustre_root,
        fid=args.fid.strip('[]'),
    )


if args.restore:
    if args.fd is None:
        logging.error('Need a FD handle to restore a file')
        sys.exit(1)
    fid_path = fid2lupath(args.lustre_root, args.fid)
    dataset_type = xattr.getxattr(fid_path, 'trusted.lhsm.type').decode()

    if dataset_type == 'aws_s3':
        s3 = boto3.resource('s3')
        s3.meta.client.meta.events.register('choose-signer.s3.*',
                                            disable_signing)
        bucket = xattr.getxattr(fid_path, 'trusted.lhsm.s3_bucket').decode()
        key = xattr.getxattr(fid_path, 'trusted.lhsm.s3_key').decode()

        with open('/proc/self/fd/{0}'.format(args.fd), "wb") as f:
            s3.Bucket(bucket).download_fileobj(key, f)

    else:
        logging.error('Dataset of type {0} is unimplemented'.format(
                      dataset_type))
        sys.exit(1)
