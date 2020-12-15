import logging
import re
import os

from tempfile import TemporaryFile

from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import unquote, urlparse

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

SRC_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent

class CCSpark:
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    def __init__(self):
        self.name = "common-crawl-insta"
        self.tmp_dir = SRC_DIR + "/tmp/"
        self.text_file = SRC_DIR + "/spark/rdd.txt"
        self.domain_name = "instagram"
        self.domain = "instagram.com"
        self.proper_domain = "https://www.instagram.com"

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed=sc.accumulator(0)


    def run(self):
        conf = SparkConf()

        sc = SparkContext(appName=self.name, conf=conf)
        sqlc = SQLContext(spark_context=sc)

        self.init_accumulators(sc)

        self.run_job(sc, sqlc)

        sc.stop()

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.text_file, minPartitions=400)

        output = input_data.mapPartitionsWithIndex(self.process_warcs).reduceByKey(self.reduce_by_key_func)

        output_json = sc.parallelize(output)
        df = sqlc.read.json(output_json)

        print(df)

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

                stream=warctemp
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

    def reduce_by_key_func(a, b):
        return a + b