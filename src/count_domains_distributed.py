import logging
import re
import os
from pathlib import Path
import sys
from operator import add
import yaml
import datetime

from tempfile import TemporaryFile
import dateutil.parser

import boto3
import botocore

from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed
from urllib.parse import unquote, urlparse
import psycopg2

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, Row, DataFrameWriter, Window
from pyspark.sql.functions import rank, col, monotonically_increasing_id, lit
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

    def create_db_connection(self):
        with open(str(PROJECT_DIR) + "/config/db.yaml") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

        hostname = cfg['cc_insta']['hostname']
        port = cfg['cc_insta']['port']
        username = cfg['cc_insta']['username']
        password = cfg['cc_insta']['password']
        driver = cfg['cc_insta']['driver']
        database = cfg['cc_insta']['database']
        db_url = "jdbc:postgresql://{host}:{port}/{db}".format(host=hostname, port=port, db=database)

        # This section sets up the self.db_properties dictionary for the Spark Engine
        self.db_properties = {}
        self.db_properties['username'] = username
        self.db_properties['password'] = password
        self.db_properties['url'] = db_url
        self.db_properties['driver'] = driver

        # This section does the psycopg2 connection, used for different circumstances
        db_string = "host={host} user={user} dbname={db}".format(host=hostname, user=username, db=database)
        self.conn = psycopg2.connect(db_string)
        self.cur = self.conn.cursor()

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

        self.create_db_connection()

        self.reference_to_instagram_df = output_json.toDF() \
                                            .orderBy("reference_link", "warc_date") \

        window = Window.partitionBy("instagram_link", "reference_link").orderBy("warc_date",'tiebreak')
        self.reference_to_instagram_df = (self.reference_to_instagram_df
         .withColumn('tiebreak', monotonically_increasing_id())
         .withColumn('rank', rank().over(window))
         .filter(col('rank') == 1).drop('rank','tiebreak'))


        self.log_aggregators(sc)

        self.prepare_csv(sc, sqlc)

        try:
            self.drop_outdated_references()
            self.perform_aggregations()

            self.conn.commit()
            self.conn.close()

        finally:
            pass
            #self.cleanup_csvs()

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
                        if domain_link[-1] != '/':
                            domain_link += '/'
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
        reference_pairs = self.reference_to_instagram_df.alias("reference_pairs")
        reference_links = sqlc.read.jdbc(
            url=self.db_properties['url'], table='public.reference_links', properties=self.db_properties)

        instagram_links = sqlc.read.jdbc(
            url=self.db_properties['url'], table='public.instagram_links', properties=self.db_properties)

        reference_links = reference_links.alias("reference_links")
        instagram_links = instagram_links.alias("instagram_links")

        reference_pairs = reference_pairs.selectExpr([col + ' as rp_' + col for col in reference_pairs.columns])
        reference_links = reference_links.selectExpr([col + ' as rl_' + col for col in reference_links.columns])
        instagram_links = instagram_links.selectExpr([col + ' as il_' + col for col in instagram_links.columns])
        
        df = reference_pairs.join(
            reference_links, 
            reference_pairs.rp_reference_link == reference_links.rl_reference_link, 
            how='left').join(
            instagram_links,
            reference_pairs.rp_instagram_link == instagram_links.il_instagram_link,
            how='left')

        df.createOrReplaceTempView("joined_references")

        self.remove_references = sqlc.sql("select distinct rp_reference_link from joined_references where rp_warc_date > rl_warc_date")

        self.new_reference_pairs = sqlc.sql("select rp_instagram_link, rp_reference_link, rp_warc_date from joined_references where (rl_warc_date is null) or (rp_warc_date > rl_warc_date)")
        self.new_reference_pairs = self.add_date_columns(self.new_reference_pairs)
        self.new_reference_pairs = self.new_reference_pairs.drop("rp_warc_date")
        self.new_reference_pairs.coalesce(1).write.csv(self.tmp_dir+'new_reference_pairs')
        

        self.new_reference_links = sqlc.sql("select rp_reference_link, max(rp_warc_date) as rp_warc_date from joined_references where (rl_warc_date is null) or (rp_warc_date > rl_warc_date) group by rp_reference_link")
        self.new_reference_links = self.add_date_columns(self.new_reference_links)
        self.new_reference_links.coalesce(1).write.csv(self.tmp_dir+'new_reference_links')

        self.new_instagram_links = sqlc.sql("select max(rp_instagram_link) from joined_references where (rl_warc_date is null) or (rp_warc_date > rl_warc_date) GROUP BY rp_instagram_link")
        self.new_instagram_links = self.new_instagram_links.withColumn("linked_count", lit(0))
        self.new_instagram_links = self.add_date_columns(self.new_instagram_links)
        self.new_instagram_links.coalesce(1).write.csv(self.tmp_dir+'new_instagram_links')

    def add_date_columns(self, df):
        df = df.withColumn("created_at", lit(datetime.datetime.now()))
        df = df.withColumn("updated_at", lit(datetime.datetime.now()))
        return df

    def drop_outdated_references(self):
        removal_list = [row[0] for row in self.remove_references.select('rp_reference_link').collect()]
        removal_clause = "($$" + "$$,$$".join(removal_list) + "$$)"
        self.cur.execute("DELETE FROM address_linked_by WHERE reference_link in " + removal_clause)


    def perform_aggregations(self):
        self.update_reference_links_table()
        self.update_instagram_links_table()
        self.update_address_linked_by_table()
        self.count_instagram_links()
        self.update_last_run_time()

    def bulk_update(self, csv_location, table_name, conflict_condition = "", columns = []):
        if columns:
            column_string = ",".join(columns)
        
        for file in os.listdir(csv_location):
            if file.endswith('.csv'):
                filename=csv_location+'/'+file
                self.cur.execute("""
                    CREATE TEMP TABLE tmp_{table_name}_table
                    (LIKE {table_name} INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                    """.format(table_name=table_name))

                self.cur.execute("""
                    COPY tmp_{table_name}_table ({columns}) FROM '{filename}' CSV;
                    """.format(table_name=table_name, columns=column_string, filename=filename))

                self.cur.execute("""
                    INSERT INTO {table_name} ({columns})
                    SELECT {columns} FROM tmp_{table_name}_table 
                    {conflict_condition};
                    """.format(table_name=table_name, columns=column_string, conflict_condition=conflict_condition))

    def update_reference_links_table(self):
        conflict_condition = """ON CONFLICT (reference_link) 
                    DO UPDATE SET 
                        updated_at = '{0}', 
                        warc_date = EXCLUDED.warc_date
                    """.format(datetime.datetime.now())

        columns = ['reference_link', 'warc_date', 'created_at', 'updated_at']

        self.bulk_update(self.tmp_dir+'new_reference_links/', 'reference_links', conflict_condition, columns=columns)

    def update_instagram_links_table(self):
        conflict_condition = """ON CONFLICT (instagram_link)
            DO UPDATE SET updated_at = '{0}'
            """.format(datetime.datetime.now())

        columns = ['instagram_link', 'linked_count', 'created_at', 'updated_at']

        self.bulk_update(self.tmp_dir+'new_instagram_links/', 'instagram_links', conflict_condition, columns=columns)

    def update_address_linked_by_table(self):
        columns = ['instagram_link', 'reference_link', 'created_at', 'updated_at']

        self.bulk_update(self.tmp_dir+'new_reference_pairs', 'address_linked_by', columns=columns)

    def count_instagram_links(self):
        self.cur.execute("""
            with 
            last_run as(
            SELECT time FROM last_run_time
            ),
            recent_references as(
            SELECT alb.instagram_link, alb.reference_link
            FROM address_linked_by alb
            CROSS JOIN last_run_time lrt
            WHERE alb.updated_at > lrt.time
            ),
            counts as (
            SELECT instagram_link, count(reference_link) as reference_count
            FROM recent_references
            GROUP BY instagram_link
            ),
            summed as(
            SELECT 
                il.instagram_link, 
                c.reference_count
            FROM instagram_links il
            JOIN counts c on il.instagram_link = c.instagram_link
            )
            UPDATE instagram_links il2
            SET linked_count = reference_count
            FROM summed
            WHERE summed.instagram_link = il2.instagram_link;
            """)

    def update_last_run_time(self):
        self.cur.execute("""
            UPDATE last_run_time
            SET time = '{0}'
            """.format(datetime.datetime.now()))

    def cleanup_csvs(self):
        os.system("rm -rf {0}".format(self.tmp_dir+'new_*'))

if __name__ == '__main__':
    job = CCSpark()
    job.run()
