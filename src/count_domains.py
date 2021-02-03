# Standard libraries
import os
from pathlib import Path
import re
import gzip
from io import StringIO
import datetime
import sys
import yaml

# Third Parties
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import unquote, urlparse
import psycopg2
import dateutil.parser
import getpass

# Internal
SRC_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
sys.path.append(str(SRC_DIR)+'/')
from db.table import table



class etl_worker:
    def __init__(
            self,
            archives,
            db_string,
            domain_name="instagram",
            domain="instagram.com",
            proper_domain="https://www.instagram.com"
    ):
        self.data = []
        self.domain_name = "instagram"
        self.domain = domain
        self.proper_domain = proper_domain
        self.archives = archives
        # db_string has the following format
        # "dbname=localhost user={user} dbname=cc_insta"

        self.conn = psycopg2.connect(db_string)
        self.cur = self.conn.cursor()




    def create_tables(self):
        self.instagram_links = table('instagram_links', self.cur)
        self.reference_links = table('reference_links', self.cur)
        self.address_linked_by = table('address_linked_by', self.cur)
        self.last_run_time = table('last_run_time', self.cur)

    def read_warc_archive(self, archive_path):
        with open(archive_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    try:
                        parser = BeautifulSoup(
                            record.content_stream().read(), features="html.parser")
                    except:
                        continue
                    links = parser.find_all("a")
                    if links:
                        for link in links:
                            href = link.attrs.get("href")
                            if href is not None:
                                if self.domain in href and href.startswith("http"):
                                    path = urlparse(href).path
                                    domain_link = self.proper_domain+path
                                    self.data.append({
                                        '{0}_link'.format(self.domain_name): domain_link,
                                        'reference_link': record.rec_headers.get_header('WARC-TARGET-URI'),
                                        'warc_date': dateutil.parser.parse(record.rec_headers.get_header('WARC-Date'))
                                    })

    def insert_data_to_db(self):
        for link in self.data:
            domain_link = link['{0}_link'.format(self.domain_name)]
            reference_link = link['reference_link']
            warc_date = link['warc_date']
            self.reference_links.custom_action("select_warc_date", reference_link)
            reference_db_warc_date = self.cur.fetchone()
            if reference_db_warc_date is not None and reference_db_warc_date[0] >= warc_date:
                continue
            self.insert_statements(link)
        self.instagram_links.custom_action("count_links")
        self.last_run_time.custom_action(
            "update_last_run_time", datetime.datetime.now())

    def insert_statements(self, link):
        domain_link = link['{0}_link'.format(self.domain_name)]
        reference_link = link['reference_link']
        warc_date = link['warc_date']
        self.instagram_links.simple_insert(
            {"{0}_link".format(self.domain_name): domain_link,
             "created_at": datetime.datetime.now(),
             "updated_at": datetime.datetime.now()},
            "ON CONFLICT DO NOTHING"
        )
        self.reference_links.simple_insert(
            {"reference_link": reference_link,
             "created_at": datetime.datetime.now(),
             "updated_at": datetime.datetime.now(),
             "warc_date": warc_date},
            "ON CONFLICT (reference_link) DO UPDATE SET updated_at = '{0}', warc_date = '{1}'".format(datetime.datetime.now(), warc_date)
        )
        self.address_linked_by.custom_action("remove_old_linked_by", reference_link)
        self.address_linked_by.custom_action("insert_linked_by", domain_link, reference_link,
                                        datetime.datetime.now(), datetime.datetime.now())


def create_db_string():
    with open(str(PROJECT_DIR) + "/config/db.yaml") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    hostname = cfg['cc_insta']['hostname']
    port = cfg['cc_insta']['port']
    username = cfg['cc_insta']['username']
    password = cfg['cc_insta']['password']
    driver = cfg['cc_insta']['driver']
    database = cfg['cc_insta']['database']
    db_url = "jdbc:postgresql://{host}:{port}/{db}".format(host=hostname, port=port, db=database)

    # This section does the psycopg2 connection, used for different circumstances
    db_string = "host={host} user={user} dbname={db}".format(host=hostname, user=username, db=database)
    return db_string

def main():

    db_string=create_db_string()

    archives = [str(PROJECT_DIR)+'/tmp/example3.warc.gz']

    instagram_counter = etl_worker(archives, db_string)

    instagram_counter.create_tables()

    for archive in instagram_counter.archives:
        instagram_counter.read_warc_archive(archive)
    instagram_counter.insert_data_to_db()

    instagram_counter.conn.commit()
    instagram_counter.conn.close()

if __name__ == '__main__':
    main()