import os
from pathlib import Path
from warcio.capture_http import capture_http
from warcio.archiveiterator import ArchiveIterator
import requests  # requests must be imported after capture_http

PROJECT_DIR = Path(os.path.dirnameo(s.path.realpath(__file__))).parent
SAMPLE_ARCHIVE = str(PROJECT_DIR)+'/tmp/example.warc.gz'

list_of_links = [
    "https://laitcl.github.io/laitcl-Website/",
    "https://store.steampowered.com/",
    "https://www.roblox.com/home",
    "https://www.mcdonalds.com/us/en-us.html"
]

with capture_http(SAMPLE_ARCHIVE):
    for link in list_of_links:
        requests.get(link)
