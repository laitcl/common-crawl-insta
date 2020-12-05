# Standard libraries
import os
from pathlib import Path
import re
import gzip
from io import StringIO
import datetime

# Third Parties
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import unquote, urlparse
import psycopg2
import dateutil.parser

import code; code.interact(local=dict(globals(), **locals()))
exit()

# Internal
from .db.table import table

PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
DOMAIN = "instagram.com"
PROPER_DOMAIN = "https://www.instagram.com"


WARC_ARCHIVE = str(PROJECT_DIR) + "/tmp/CC-MAIN-20201101001251-20201101031251-00719.warc.gz"
# WARC_ARCHIVE = str(PROJECT_DIR)+'/tmp/example.warc.gz'


conn = psycopg2.connect(
    host="localhost",
    dbname="cc_insta",
    user="laitcl",
    password="")
cur = conn.cursor()

def create_tables():
    instagram_links = table('instagram_links')
    reference_links = table('reference_links')
    address_linked_by = table('address_linked_by')
    last_run_time = table('last_run_time')


create_tables()



address_linked = []

with open(WARC_ARCHIVE, 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            try:
                parser = BeautifulSoup(record.content_stream().read(), features="html.parser")
            except:
                continue
            links = parser.find_all("a")
            if links:
                for link in links:
                    href = link.attrs.get("href")
                    if href is not None:
                        if DOMAIN in href and href.startswith("http"):
                            path = urlparse(href).path
                            instagram_link = PROPER_DOMAIN+path
                            address_linked.append({
                                'instagram_link': instagram_link, 
                                'reference_link': record.rec_headers.get_header('WARC-TARGET-URI'),
                                'warc_date': dateutil.parser.parse(record.rec_headers.get_header('WARC-Date'))
                                })
                            


for link in address_linked:
    instagram_link = link['instagram_link']
    reference_link = link['reference_link']
    warc_date = link['warc_date']

    cur.execute("SELECT warc_date FROM reference_links WHERE reference_link = $${reference_link}$$".format(reference_link = reference_link))
    reference_db_warc_date =cur.fetchone()

    if reference_db_warc_date is not None and reference_db_warc_date[0] >= warc_date: continue

    insert_instagram_links_sql="""
        INSERT INTO instagram_links
        (instagram_link, created_at, updated_at)
        VALUES
        ($${instagram_link}$$, '{created_at}', '{updated_at}')
        ON CONFLICT DO NOTHING
        """.format(
            instagram_link = instagram_link, 
            created_at = datetime.datetime.now(), 
            updated_at = datetime.datetime.now()
            )

    insert_reference_links_sql="""
        INSERT INTO reference_links
        (reference_link, created_at, updated_at, warc_date)
        VALUES
        ($${reference_link}$$, '{created_at}', '{updated_at}', '{warc_date}')
        ON CONFLICT (reference_link) DO UPDATE 
        SET updated_at = '{updated_at}', warc_date = '{warc_date}'
        """.format(
            reference_link= reference_link, 
            created_at = datetime.datetime.now(), 
            updated_at = datetime.datetime.now(), 
            warc_date = warc_date,
            )

    remove_old_linked_by_sql="""
        DELETE FROM address_linked_by
        WHERE reference_link = $${reference_link}$$
    """.format(reference_link= reference_link)

    insert_linked_by_sql="""
        INSERT INTO address_linked_by
        (instagram_link_id, instagram_link, reference_link_id, reference_link, created_at, updated_at)
        VALUES
        (
            (SELECT id FROM instagram_links WHERE instagram_link = $${instagram_link}$$),
            $${instagram_link}$$,
            (SELECT id FROM reference_links WHERE reference_link = $${reference_link}$$),
            $${reference_link}$$,
            '{created_at}',
            '{updated_at}'
        )
        """.format(
            instagram_link = instagram_link,
            reference_link = reference_link,
            created_at = datetime.datetime.now(),
            updated_at = datetime.datetime.now(),
            warc_date = warc_date,
            )

    cur.execute(insert_instagram_links_sql)
    cur.execute(insert_reference_links_sql)
    cur.execute(remove_old_linked_by_sql)
    cur.execute(insert_linked_by_sql)

count_links_sql = """
    with 
    last_run as(
    SELECT time FROM last_run_time
    ),
    recent_references as(
    SELECT instagram_link_id, reference_link_id
    FROM address_linked_by alb
    CROSS JOIN last_run_time lrt
    WHERE alb.updated_at > lrt.time
    ),
    counts as (
    SELECT instagram_link_id, count(reference_link_id) as reference_count
    FROM recent_references
    GROUP BY instagram_link_id
    ),
    summed as(
    SELECT 
        il.id as id, 
        il.linked_count + c.reference_count as total_count
    FROM instagram_links il
    JOIN counts c on il.id = c.instagram_link_id
    )
    UPDATE instagram_links il2
    SET linked_count = total_count
    FROM summed
    WHERE summed.id = il2.id
"""

cur.execute(count_links_sql)

update_last_run_time_sql = """
UPDATE last_run_time
SET time = '{time}'
""".format(time = datetime.datetime.now())

cur.execute(update_last_run_time_sql)

conn.commit()
conn.close()