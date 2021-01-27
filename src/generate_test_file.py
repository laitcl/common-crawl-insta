import os
from pathlib import Path
from warcio.capture_http import capture_http
from warcio.archiveiterator import ArchiveIterator
import requests  # requests must be imported after capture_http

PROJECT_DIR = Path(os.path.dirname(os.path.realpath(__file__))).parent
SAMPLE_ARCHIVE = str(PROJECT_DIR)+'/tmp/example4.warc.gz'

list_of_links = [
    "https://laitcl.github.io/laitcl-Website/",
    "https://laitcl.github.io/laitcl-Website/",
    "https://laitcl.github.io/laitcl-Website/",
    "https://store.steampowered.com/",
    "https://www.roblox.com/home",
    "https://www.mcdonalds.com/us/en-us.html",
    "https://www.supergiantgames.com/games/hades/"
]

with capture_http(SAMPLE_ARCHIVE):
    for link in list_of_links:
        requests.get(link)
