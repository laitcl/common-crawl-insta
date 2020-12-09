import boto3
import sys

# Run this script by following this example
# python src/cc-downloader.py crawl-data/CC-MAIN-2020-50/segments/1606141753148.92/warc/CC-MAIN-20201206002041-20201206032041-00718.warc.gz
# Note that there is no bucket name; only prefixes
# File can be found in the tmp/ directory

filename = sys.argv[1]
s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')
s3.download_file("commoncrawl", filename, './tmp/{0}'.format(os.path.split(filename)[1]))