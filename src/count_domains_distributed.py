import logging
import re
import os
from pathlib import Path
import sys
from operator import add
import yaml

from tempfile import TemporaryFile
import dateutil.parser

import boto3
import botocore

from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed
from urllib.parse import unquote, urlparse

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, Row, DataFrameWriter, Window
from pyspark.sql.functions import rank, col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, LongType
import findspark
findspark.init()

SRC_DIR = Path(os.path.dirname(os.path.realpath(__file__)))

PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class CCSpark:
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    def __init__(self):
        self.name = "common-crawl-insta"
        self.tmp_dir = str(SRC_DIR) + "/tmp/"
        self.text_file = str(SRC_DIR) + "/spark/rdd.txt"
        self.domain_name = "instagram"
        self.domain = "instagram.com"
        self.proper_domain = "https://www.instagram.com"

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def run(self):
        conf = SparkConf()

        sc = SparkContext(appName=self.name, conf=conf)
        sqlc = SQLContext(sparkContext=sc)

        self.init_accumulators(sc)
        self.run_job(sc, sqlc)

        sc.stop()

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.text_file, minPartitions=4)

        output = input_data.mapPartitionsWithIndex(
            self.process_warcs).reduce(add)

        output_json = sc.parallelize(output)

        self.reference_to_instagram_df = output_json.toDF() \
                                            .orderBy("reference_link", "warc_date") \

        window = Window.partitionBy("instagram_link", "reference_link").orderBy("warc_date",'tiebreak')
        self.reference_to_instagram_df = (self.reference_to_instagram_df
         .withColumn('tiebreak', monotonically_increasing_id())
         .withColumn('rank', rank().over(window))
         .filter(col('rank') == 1).drop('rank','tiebreak'))


        self.log_aggregators(sc)

        self.prepare_csv(sc, sqlc)

    def process_warcs(self, id_, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        for uri in iterator:
            self.warc_input_processed.add(1)
            if uri.startswith('s3://'):
                self.get_logger().info('Reading from S3 {}'.format(uri))
                s3match = s3pattern.match(uri)
                if s3match is None:
                    self.get_logger().error("Invalid S3 URI: " + uri)
                    continue
                bucketname = s3match.group(1)
                path = s3match.group(2)

                warctemp = TemporaryFile(mode='w+b',
                                         dir=self.tmp_dir)
                try:
                    s3client.download_fileobj(bucketname, path, warctemp)
                except botocore.client.ClientError as exception:
                    self.get_logger().error(
                        'Failed to download {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    warctemp.close()
                    continue

                warctemp.seek(0)

                stream = warctemp
            else:
                self.get_logger().info('Reading local stream {}'.format(uri))
                if uri.startswith('file:'):
                    uri = uri[5:]
                uri = os.path.join(base_dir, uri)
                try:
                    stream = open(uri, 'rb')
                except IOError as exception:
                    self.get_logger().error(
                        'Failed to open {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    continue

            try:
                archive_iterator = ArchiveIterator(stream, arc2warc=True)
                for res in self.iterate_records(uri, archive_iterator):
                    yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(uri, exception))
            finally:
                stream.close()

    def iterate_records(self, _warc_uri, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res
            self.records_processed.add(1)

    def process_record(self, record):
        if record.rec_type != 'response':
            return
        content_type = record.http_headers.get_header('content-type', None)
        if content_type is None or 'html' not in content_type:
            # skip non-HTML or unknown content types
            return

        try:
            parser = BeautifulSoup(
                record.content_stream().read(), features="html.parser")
        except:
            return

        links = parser.find_all("a")

        if links:
            for link in links:
                href = link.attrs.get("href")
                if href is not None:
                    if self.domain in href and href.startswith("http"):
                        path = urlparse(href).path
                        domain_link = self.proper_domain+path
                        link_data = [{
                            '{0}_link'.format(self.domain_name): domain_link,
                            'reference_link': record.rec_headers.get_header('WARC-TARGET-URI'),
                            'warc_date': dateutil.parser.parse(record.rec_headers.get_header('WARC-Date'))
                        }]
                        yield link_data

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        return spark_context._jvm.org.apache.log4j.LogManager \
            .getLogger(self.name)

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC/WAT/WET input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'WARC/WAT/WET input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'WARC/WAT/WET records processed = {}')

    def prepare_csv(self, sc, sqlc):
        with open(str(PROJECT_DIR) + "/config/db.yaml") as ymlfile:
            cfg = yaml.load(ymlfile)

        db_properties = {}
        db_url = cfg['cc_insta']['url']
        db_properties['username'] = cfg['cc_insta']['username']
        db_properties['password'] = cfg['cc_insta']['password']
        db_properties['url'] = cfg['cc_insta']['url']
        db_properties['driver'] = cfg['cc_insta']['driver']

        reference_pairs = self.reference_to_instagram_df.alias("reference_pairs")
        reference_links = sqlc.read.jdbc(
            url=db_url, table='public.reference_links', properties=db_properties)

        reference_links = reference_links.alias("reference_links")

        reference_pairs = reference_pairs.selectExpr([col + ' as rp_' + col for col in reference_pairs.columns])
        reference_links = reference_links.selectExpr([col + ' as rl_' + col for col in reference_links.columns])

        df = reference_pairs.join(
            reference_links, 
            reference_pairs.rp_reference_link == reference_links.rl_reference_link, 
            how='left')

        df.createOrReplaceTempView("joined_references")

        remove_references = sqlc.sql("select distinct rp_reference_link from joined_references where rp_warc_date > rl_warc_date")
        new_reference_pairs = sqlc.sql("select rp_instagram_link, rp_reference_link, rp_warc_date from joined_references where (rl_warc_date is null) or (rp_warc_date > rl_warc_date)")
        new_reference_links = sqlc.sql("select rp_reference_link, max(rp_warc_date) as rp_warc_date from joined_references where (rl_warc_date is null) or (rp_warc_date > rl_warc_date) group by rp_reference_link")

        remove_references.coalesce(1).write.csv(self.tmp_dir+'remove_references.csv')
        new_reference_pairs.coalesce(1).write.csv(self.tmp_dir+'new_reference_pairs.csv')
        new_reference_links.coalesce(1).write.csv(self.tmp_dir+'new_reference_links.csv')


if __name__ == '__main__':
    job = CCSpark()
    job.run()
