import json
import os
import sys
import io
import requests
from io import BytesIO
import json
import time
import requests
from multiprocessing.dummy import Pool as ThreadPool
import concurrent.futures
import urllib.request

import sys, os, multiprocessing, urllib3, csv
from PIL import Image
from io import BytesIO
from tqdm import tqdm
import json
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

client = urllib3.PoolManager(500)


def DownloadImage(post):
    # get URL of the downloaded images

    url = post[0]
    filename = post[1]
    # url = url[:url.find('?')]
    # url = url.replace("https:","http:")
    if not (url.startswith("https:") or url.startswith("http:")):
        url = "https:" + url

    subdirectory = out_dir + "/" + sub_dir
    filename = subdirectory + filename + ".jpg"

    try:
        if not os.path.exists(subdirectory):
            os.makedirs(subdirectory)
    except Exception as e:
        print('Location: %s already exists.' % subdirectory)
        return

    if os.path.exists(filename):
        print('Image %s already exists. Skipping download.' % filename)
        return

    try:
        # global client
        # response = client.request('GET', url)#, timeout=30)
        # image_data = response.data
        response = requests.get(url)
        image_data = response.content
    except:
        print('Warning: Could not download image %s from %s' % (key, url))
        return

    try:
        pil_image = Image.open(BytesIO(image_data))
    except:
        print('Warning: Failed to parse image %s %s' % (key, url))
        return

    try:
        pil_image = pil_image.resize((400, 400), Image.ANTIALIAS)
    except:
        print('Warning: Failed to resize image %s %s' % (key, url))
        return

    try:
        pil_image_rgb = pil_image.convert('RGB')
    except:
        print('Warning: Failed to convert image %s to RGB' % key)
        return

    try:
        pil_image_rgb.save(filename, format='JPEG', quality=90)
        # print('Success: Proceeding to save image %s' % filename)
    except:
        print('Warning: Failed to save image %s' % filename)
        return


def main():
    all_urls = [[
        "https://ws-eu.amazon-adsystem.com/widgets/q?encoding=UTF8&MarketPlace=GB&ASIN=" + i + "&ServiceVersion=20070822&ID=AsinImage&WS=1&Format=SL250",
        i] for i in data["asin"]]

    max_batch = 200
    for batch in range(0, len(all_urls), max_batch):

        try:

            pool = ThreadPool(10)
            list_view_objects = all_urls[batch: batch + max_batch]
            print(batch)
            pool.map(DownloadImage, list_view_objects)
            pool.close()
            pool.join()
            print("YOLO")
            client.close()
        except Exception as e:

            # print("failed")
            with open('errors.csv', 'a') as errorFile:
                errorWriter = csv.writer(errorFile)
                errorWriter.writerow([batch, str(e)])
            raise


if __name__ == '__main__':
    # directories where to store
    out_dir = "/home/textmercato/filestore/images"
    sub_dir = "amazon"

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    # reading Data
    data = pd.read_pickle("/home/textmercato/filestore/amazon/combined_l4_product_data.pkl")
    print(data.head())

    # main function for initial starting.
    main()
