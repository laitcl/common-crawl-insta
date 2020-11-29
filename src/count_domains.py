import os
from pathlib import Path
import re
import gzip
from bs4 import BeautifulSoup
from io import StringIO
from warcio.archiveiterator import ArchiveIterator
from collections import Counter, defaultdict
from urllib.parse import unquote, urlparse
import psycopg2

INSTAGRAM_LINK_PATTERN = re.compile('')

project_dir = Path(os.path.dirname(os.path.realpath(__file__))).parent

warc_archive = str(project_dir) + "/tmp/CC-MAIN-20201101001251-20201101031251-00719.warc.gz"

address_count=Counter()
address_linked = defaultdict(list)

domain = "instagram.com"
proper_domain = "https://www.instagram.com/"

with open(warc_archive, 'rb') as stream:
	for record in ArchiveIterator(stream):
		if record.rec_type == 'response':
			parser = BeautifulSoup(record.content_stream().read())
			links = parser.find_all("a")
			if links:
				for link in links:
					href = link.attrs.get("href")
					if href is not None:
						if domain in href and href.startswith("http"):
							path = urlparse(href).path
							instagram_link = proper_domain+path
							account_count[instagram_link]+=1 
							address_linked[instagram_link].append(record.rec_headers.get_header('WARC-TARGET-URI'))

conn = psycopg2.connect(
    host="localhost",
    database="cc_insta",
    user="postgres",
    password="")

cur = conn.cursor()

create_instagram_links_sql="""
CREATE TABLE IF NOT EXISTS instagram_links
(
id              serial PRIMARY KEY,
insta_link      text
);
"""

create_address_linked_count_sql="""
CREATE TABLE IF NOT EXISTS address_linked_count 
(
id                  serial PRIMARY KEY, 
instagram_link_id   integer REFERENCES instagram_links,
count               int
)
;
"""

create_address_linked_by_sql="""
CREATE TABLE IF NOT EXISTS address_linked_by 
(
id                  serial PRIMARY KEY,
instagram_link_id   integer REFERENCES instagram_links ,
referened_by        text
);
"""

cur.execute(create_instagram_links_sql)
cur.execute(create_address_linked_count_sql)
cur.execute(create_address_linked_by_sql)
