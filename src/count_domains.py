import os
from pathlib import Path
import re
import gzip
from bs4 import BeautifulSoup
from io import StringIO
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import unquote, urlparse
import psycopg2
import datetime

INSTAGRAM_LINK_PATTERN = re.compile('')
PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent

WARC_ARCHIVE = str(PROJECT_DIR) + "/tmp/CC-MAIN-20201101001251-20201101031251-00719.warc.gz"
# WARC_ARCHIVE = str(PROJECT_DIR)+'/tmp/example.warc.gz'


conn = psycopg2.connect(
    host="localhost",
    dbname="cc_insta",
    user="laitcl",
    password="")
cur = conn.cursor()

def create_tables():
    create_instagram_links_sql="""
    CREATE TABLE IF NOT EXISTS instagram_links
    (
    id              serial PRIMARY KEY,
    instagram_link  text UNIQUE,
    linked_count    int DEFAULT 0,
    created_at      timestamp,
    updated_at      timestamp
    );
    """

    create_address_linked_by_sql="""
    CREATE TABLE IF NOT EXISTS address_linked_by 
    (
    id                  serial PRIMARY KEY,
    instagram_link_id   integer REFERENCES instagram_links,
    instagram_link      text,
    referenced_by       text,
    created_at          timestamp,
    updated_at          timestamp
    );
    """

    create_run_status_sql="""
    CREATE TABLE IF NOT EXISTS last_run_time 
    (
    time       timestamp DEFAULT CURRENT_TIMESTAMP(0)
    );
    """

    insert_last_run_time_sql="""
    INSERT INTO last_run_time (time) SELECT '{time}' WHERE NOT EXISTS (SELECT time from last_run_time)
    """.format(time = datetime.datetime.now())

    cur.execute(create_instagram_links_sql)
    cur.execute(create_address_linked_by_sql)
    cur.execute(create_run_status_sql)
    cur.execute(insert_last_run_time_sql)

create_tables()
address_linked = []
domain = "instagram.com"
proper_domain = "https://www.instagram.com/"



counter = 0

with open(WARC_ARCHIVE, 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            parser = BeautifulSoup(record.content_stream().read())
            counter+=1
            if counter > 100:
                break
            links = parser.find_all("a")
            if links:
                for link in links:
                    href = link.attrs.get("href")
                    if href is not None:
                        if domain in href and href.startswith("http"):
                            path = urlparse(href).path
                            instagram_link = proper_domain+path
                            address_linked.append({
                                'instagram_link': instagram_link, 
                                'referencing_link': record.rec_headers.get_header('WARC-TARGET-URI'),
                                })


for link in address_linked:
    instagram_link = link['instagram_link']
    referencing_link = link['referencing_link']

    insert_instagram_links_sql="""
        INSERT INTO instagram_links
        (instagram_link, created_at, updated_at)
        VALUES
        ('{instagram_link}', '{created_at}', '{updated_at}')
        ON CONFLICT DO NOTHING
    """.format(instagram_link = instagram_link, created_at = datetime.datetime.now(), updated_at = datetime.datetime.now())

    insert_linked_by_sql="""
        INSERT INTO address_linked_by
        (instagram_link_id, instagram_link, referenced_by, created_at, updated_at)
        (
            SELECT id,
            '{instagram_link}',
            '{referenced_by}',
            '{created_at}',
            '{updated_at}'
            FROM instagram_links WHERE instagram_link = '{instagram_link}'
        )
    """.format(
        instagram_link = instagram_link,
        referenced_by = referencing_link,
        created_at = datetime.datetime.now(),
        updated_at = datetime.datetime.now(),
        )

    cur.execute(insert_instagram_links_sql)
    cur.execute(insert_linked_by_sql)

count_links_sql = """
    with 
    last_run as(
    SELECT time FROM last_run_time
    ),
    recent_references as(
    SELECT instagram_link_id, referenced_by
    FROM address_linked_by alb
    CROSS JOIN last_run_time lrt
    WHERE alb.updated_at > lrt.time
    ),
    counts as (
    SELECT instagram_link_id, count(referenced_by) as reference_count
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