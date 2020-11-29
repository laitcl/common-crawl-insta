import os
from pathlib import Path
import re
import gzip
from bs4 import BeautifulSoup
from io import StringIO
from warcio.archiveiterator import ArchiveIterator
from collections import Counter, defaultdict
from urllib.parse import unquote, urlparse

INSTAGRAM_LINK_PATTERN = re.compile('')

project_dir = Path(os.path.dirname(os.path.realpath(__file__))).parent

warc_archive = str(project_dir) + "/tmp/CC-MAIN-20201101001251-20201101031251-00719.warc.gz"

account_count=Counter()
linked_instagrams = defaultdict(list)

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
							linked_instagrams[instagram_link].append(record.rec_headers.get_header('WARC-TARGET-URI'))
							print(account_count)
							print(linked_instagrams)