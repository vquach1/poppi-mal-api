import boto3
import requests
import random
import time
import re
import hashlib
from bs4 import *

"""Each XML file acts as a URL directory to resources. This dictionary maps the filename to the 
   bucket folder to the type of resource"""
filenames = {
    "anime-000.xml": "anime",
    "manga-000.xml": "manga",
    "character-000.xml": "character",
    "character-001.xml": "character",
    "people-000.xml": "people"
}

headers = {
    'User-Agent': 'Poppi MAL Scrape 1.0',
    'From': 'vitungquach1494@gmail.com'
}

base_delay = 15
max_extra_delay = 5

s3 = boto3.resource('s3')
bucket = s3.Bucket('mal-scrape-raw-html')

for fname in filenames:
    type = fname.split("-")[0] # Resource type - anime, character, manga, or people
    regex = "(?<={0}/)(\d+)".format(type)

    dir_file = open(fname, mode="r", encoding="utf-8")
    dir_soup = BeautifulSoup(dir_file.read(), "lxml")
    urls = dir_soup.select("loc")

    for url in urls:
        res = requests.get(url.text, headers=headers)
        if res.status_code == 200:
            match = re.search(regex, url.text)
            id = match.group(1)
            main_key = "{0}-{1}".format(type, id)           # The logical name of the file on S3

            h = hashlib.md5()
            h.update(url.text.encode('utf-8'))
            prefix = h.hexdigest()[0]                    # Random prefix to append to file

            full_key = "{0}/{1}".format(prefix, main_key)   #The actual name of the file on S3
            bucket.put_object(Body=res.text, Key=full_key)

        delay = base_delay + random.randint(0, max_extra_delay)
        time.sleep(delay)

    dir_file.close()


#res = requests.get("https://myanimelist.net/anime/1/Cowboy_Bebop", headers=headers)
#print(res.status_code)
#print(res.text)