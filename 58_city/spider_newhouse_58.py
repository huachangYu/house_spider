# -*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import base64
from fontTools.ttLib import TTFont
import pandas as pd
import re
import random
import time
import json
import warnings

warnings.filterwarnings("ignore")


def get_html(target, trans_code=False):
    my_headers = [{
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36"
    }, {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0"
    }, {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14"
    }, {
        "User-Agent": "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)"
    }]
    response = requests.get(target, headers=random.choice(my_headers))
    if trans_code:
        html = response.text.encode('iso-8859-1').decode('gbk')
    else:
        html = response.text
    return html


def get_hrefs(html):
    hrefs = []
    soup = BeautifulSoup(html)
    list_soup = soup.find(attrs={"class": "key-list imglazyload"}).find_all(attrs={"class": "item-mod"})
    for house_soup in list_soup:
        try:
            href = house_soup.attrs["data-link"]
        except Exception:
            continue
        if href is not "":
            hrefs += [href]
    return hrefs


def get_house_structure(house_html):
    house_soup = BeautifulSoup(house_html)
    structure_soup = house_soup.find(id="j-housetype-showlink")
    structure_href = structure_soup.attrs["href"]
    structure_html = get_html(structure_href)
    structure_soup = BeautifulSoup(structure_html)
    pages_soup = structure_soup.find(attrs={"class": "pagination"}).find_all("a")
    structures = get_structure(structure_html)
    if len(pages_soup) >= 2:
        page_num = len(pages_soup)
        for page_id in range(page_num - 1):
            page_url = pages_soup[page_id].attrs["href"]
            structures += get_structure(get_html(page_url))
    return structures


def get_structure(structure_html):
    structure_soup = BeautifulSoup(structure_html)
    structures = structure_soup.find(attrs={"class": "hx-list g-clear"}).find_all(attrs={"class": "type-name"})
    struture_data = []
    for structure in structures:
        structure_str = structure.find(attrs={"class": "desc-v"}).text
        area_str = structure.find(attrs={"class": "desc-k area-k"}).text
        ind1 = area_str.index("约")
        ind2 = area_str.index("m²")
        area = float(area_str[ind1 + 1:ind2])
        data_row = {"structure": structure_str, "area": area}
        struture_data += [data_row]
    return struture_data


def get_price(house_html):
    soup = BeautifulSoup(house_html)
    price_str = soup.find(attrs={"class": "sp-price other-price"}).text
    try:
        return int(price_str)
    except Exception:
        return "unknown"


def get_xy(house_html):
    pattern = 'lat: (\d|\.)+, lng: (\d|\.)+'
    xy_str = re.search(pattern, house_html)
    pattern_num = '(([0-9]|\.)+)'
    xy = re.findall(pattern_num, xy_str.group())
    return [float(xy[0][0]), float(xy[1][0])]


def get_name(house_html):
    soup = BeautifulSoup(house_html)
    name = soup.find(attrs={"class": "basic-info"}).find("h1").text.strip()
    return name


def run():
    # cities = ["hz", "cd", "bj", "sh", "huizhou", "nb"]
    cities = ["huizhou", "nb"]
    for city in cities:
        house_data = pd.DataFrame(columns=("name", "price", "x", "y", "house_type_num", "house_structure_area"))
        url_head = "https://" + city + ".58.com/xinfang/loupan/all/p"
        for page_id in range(1, 20):
            url = url_head + str(page_id) + '/'
            try:
                html = get_html(url)
                hrefs = get_hrefs(html)
            except Exception:
                print(city + " " + str(page_id) + "-------------------big error----------------------")
                print(url)
                time.sleep(60)
                continue
            for href in hrefs:
                try:
                    house_html = get_html(href)
                    name = get_name(house_html)
                    try:
                        struture = get_house_structure(house_html)
                    except Exception:
                        struture = []
                    xy = get_xy(house_html)
                    price = get_price(house_html)
                    row_data = {"name": name, "price": price, "x": xy[0], "y": xy[1], "house_type_num": len(struture),
                                "house_structure_area": struture}
                except Exception:
                    print("error....")
                    print(href)
                    time.sleep(10)
                    continue
                print(row_data)
                house_data = house_data.append(row_data, ignore_index=True)
            house_data.to_csv("./58_newhouse/newhouse_58_" + city + ".csv")
            print(city + " " + str(page_id) + ":-----------------------saved------------------------")
        print(city + "----------------------------finished---------------------------------")
    print("---------------------------------all finished-----------------------------------------")


if __name__ == '__main__':
    run()
