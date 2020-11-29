import os
from pathlib import Path
from warcio.capture_http import capture_http
from warcio.archiveiterator import ArchiveIterator
import requests  # requests must be imported after capture_http

PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
SAMPLE_ARCHIVE = str(PROJECT_DIR)+'/tmp/example.warc.gz'

list_of_links = [
    "https://laitcl.github.io/laitcl-Website/"
]


with capture_http(SAMPLE_ARCHIVE):
    for link in list_of_links:
        requests.get(link)

with capture_http() as writer:
    requests.get('http://example.com/')
    requests.get('https://google.com/')


with open(SAMPLE_ARCHIVE, 'rb') as stream:
    for record in ArchiveIterator(stream):
        print(record.content_stream().read())
