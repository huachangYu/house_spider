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
    """ip_str = requests.get(
        'http://39.108.59.38:7772/Tools/proxyIP.ashx?OrderNumber=3b00ba1d85140f530e09086c89a7619f&poolIndex=1571202839&cache=1&qty=1')
    ip = 'https://' + ip_str.text.strip()
    pro = {'https': ip}
    response = requests.get(target, headers=random.choice(my_headers), proxies=pro, timeout=3)"""
    response = requests.get(target, headers=random.choice(my_headers))
    if trans_code:
        html = response.text.encode('iso-8859-1').decode('gbk')
    else:
        html = response.text
    return html


def get_hrefs(html):
    hrefs = []
    soup = BeautifulSoup(html)
    list_soup = soup.find(attrs={"class": "house-list"}).find_all(attrs={"class": "house-cell"})
    for house_soup in list_soup:
        try:
            href = house_soup.find(attrs={"class": "des"}).find("h2").find("a").attrs["href"]
        except Exception:
            continue
        if href is not "":
            hrefs += [href]
    return hrefs


def decode_number_58(d_text, html):  # 数字加密了，所以需要解析
    pattern = "base64,.*?'"
    base64_str = re.search(pattern, html).group()
    base64_str = base64_str[7:-1]
    bin_data = base64.decodebytes(base64_str.encode())
    path = r'./font/font.otf'
    with open(path, 'wb') as f:
        f.write(bin_data)
        f.close()
    font01 = TTFont(path)
    utf_list = font01['cmap'].tables[0].ttFont.tables['cmap'].tables[0].cmap  # c = font.getBestCmap()
    ret_list = []
    for i in d_text:
        if ord(i) in utf_list:
            text = int(utf_list[ord(i)][-2:]) - 1
        else:
            text = i
        ret_list.append(text)
    crack_text = ''.join([str(i) for i in ret_list])
    return crack_text


def get_name(house_html):
    soup = BeautifulSoup(house_html)
    name = soup.find(attrs={"class": "house-title"}).find("h1").text.strip()
    return name


def get_price(house_html):
    soup = BeautifulSoup(house_html)
    price_str = soup.find(attrs={"class": "house-pay-way f16"}).find("span").find("b").text.strip()
    price_str = decode_number_58(price_str, house_html)
    return int(price_str)


def get_house_info(house_html):  # return structure and aera
    soup = BeautifulSoup(house_html)
    house_des = soup.find(attrs={"class": "house-desc-item fl c_333"}).find(attrs={"class": "f14"}).find_all("li")
    house_type_str = house_des[1].find(attrs={"class": "strongbox"}).text.strip()
    house_type_str = decode_number_58(house_type_str, house_html).replace(' ', '')
    house_infos = house_type_str.split(' ')
    return house_infos[0], float(house_infos[2][:-1])


def get_xy(house_html):
    pattern = '"lat":(\d|\.)+.*?"lon":(\d|\.)+'
    result_str = re.search(pattern, house_html)
    pattern_num = '(([0-9]|\.)+)'
    xy = re.findall(pattern_num, result_str.group())
    return [float(xy[0][0]), float(xy[1][0])]


def run():
    # cities = ["hz", "cd", "bj", "sh", "huizhou", "nb"]
    cities = ["sh", "huizhou", "nb"]
    for city in cities:
        house_data = pd.DataFrame(columns=("name", "month_price", "house_structure", "area", "x", "y"))
        url_head = 'https://' + city + '.58.com/chuzu/pn'
        for page_id in range(1, 40):
            url = url_head + str(page_id) + '/'
            try:
                html = get_html(url)
                hrefs = get_hrefs(html)
            except Exception:
                print(str(page_id) + ":-------------------------big error-------------------------")
                print(url)
                time.sleep(40)
                print("----------------------waiting for 10s------------------------------------")
                continue
            for href in hrefs:
                time.sleep(0.3 * random.random())
                try:
                    house_html = get_html(href)
                    structure, area = get_house_info(house_html)
                    xy = get_xy(house_html)
                    data_row = {"name": get_name(house_html), "month_price": get_price(house_html),
                                "house_structure": structure, "area": area, "x": xy[0], "y": xy[1]}
                except Exception:
                    print("error...\n"+href)
                    time.sleep(5)
                    continue
                print(data_row)
                house_data = house_data.append(data_row, ignore_index=True)
            house_data.to_csv("./58_rent/rent_58_" + city + ".csv")
            print(city + " " + str(page_id) + ":------------------------saved---------------------------")
        print(city + ":-----------------------------finished-------------------------------")
    print("-----------------------------finished-------------------------------")


if __name__ == '__main__':
    run()
