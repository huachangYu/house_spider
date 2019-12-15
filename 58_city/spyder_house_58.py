# -*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import base64
from fontTools.ttLib import TTFont
import pandas as pd
import re
import time
import json
import warnings

warnings.filterwarnings("ignore")


def get_html(target, trans_code=False):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'}
    response = requests.get(target, headers=headers)
    if trans_code:
        html = response.text.encode('iso-8859-1').decode('gbk')
    else:
        html = response.text
    return html


def get_hrefs(html):
    hrefs = []
    soup = BeautifulSoup(html)
    list_soup = soup.find(attrs={"class": "house-list-wrap"}).find_all("li")
    for house_soup in list_soup:
        try:
            href = house_soup.find(attrs={"class": "list-info"}).find(attrs={"class": "title"}).find("a").attrs["href"]
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


def get_price(html):
    soup = BeautifulSoup(html)
    left_soup = soup.find(id="generalSituation").find(attrs={"class": "general-item-left"}).find_all("li")
    price_str = left_soup[0].find_all("span")[1].text.strip()
    number = decode_number_58(price_str, html)
    ind1 = number.index('万')
    ind2 = number.index('价')
    ind3 = number.index('元')
    total_price = number[:ind1]
    average_price = number[ind2 + 1:ind3]
    return float(total_price), float(average_price)


def get_xy(html):
    pattern = '{"baidulat":(\d|\.)+.*?"lat":(\d|\.)+}'
    result_str = re.search(pattern, html)
    pattern_num = '(([0-9]|\.)+)'
    xy = re.findall(pattern_num, result_str.group())
    len_xy = len(xy)
    return [float(xy[len_xy - 1][0]), float(xy[len_xy - 2][0])]


def get_house_structure(html):
    soup = BeautifulSoup(html)
    left_soup = soup.find(id="generalSituation").find(attrs={"class": "general-item-left"}).find_all("li")
    type_str = left_soup[1].find_all("span")[1].text.strip()
    return type_str


def get_aera(html):
    soup = BeautifulSoup(html)
    left_soup = soup.find(id="generalSituation").find(attrs={"class": "general-item-left"}).find_all("li")
    aera_str = left_soup[2].find_all("span")[1].text.strip()
    return float(aera_str[:aera_str.index('㎡')])


def get_years(html):
    soup = BeautifulSoup(html)
    right_soup = soup.find(id="generalSituation").find(attrs={"class": "general-item-right"}).find_all("li")
    year_str = right_soup[2].find_all("span")[1].text.strip()
    if year_str.find('年') is -1:
        return year_str
    return int(year_str[:year_str.index('年')])


def get_type(html):
    soup = BeautifulSoup(html)
    right_soup = soup.find(id="generalExpense").find(attrs={"class": "general-item-left"}).find_all("li")
    type_str = right_soup[1].find_all("span")[1].text.strip()
    return type_str


def get_name(html):
    soup = BeautifulSoup(html)
    name = soup.find(attrs={"class": "c_333 f20"}).text.strip()
    return name


def get_build_year(html):
    soup = BeautifulSoup(html)
    right_soup = soup.find(id="generalSituation").find(attrs={"class": "general-item-right"}).find_all("li")
    build_year = right_soup[3].find_all("span")[1].text.strip()
    if build_year.find('年') is -1:
        return build_year
    return int(build_year[:build_year.index('年')])


def run():
    cities = ["hz", "cd", "bj", "sh", "huizhou", "nb"]
    page_nums = [30, 30, 30, 30, 30, 30]
    for ct in cities:
        house_data = pd.DataFrame(columns=(
            "name", "type", "build_year", "year", "total_price", "average_price", "house_structure", "area", "x", "y"))
        url_head = 'https://' + ct + '.58.com/ershoufang/pn'
        print(ct + ":-------------------started------------------------")
        for page_id in range(1, 31):
            url = url_head + str(page_id) + '/'
            html = get_html(url)
            hrefs = get_hrefs(html)
            for href in hrefs:
                try:
                    house_html = get_html(href)
                    xy = get_xy(house_html)
                    total, avg = get_price(house_html)
                    data_row = {
                        "name": get_name(house_html), "type": get_type(house_html),
                        "build_year": get_build_year(house_html),
                        "year": get_years(house_html), "total_price": total, "average_price": avg,
                        "house_structure": get_house_structure(house_html), "area": get_aera(house_html), "x": xy[0],
                        "y": xy[1]}
                except Exception:
                    print(href)
                    print("error...")
                    time.sleep(20)
                    continue
                print(data_row)
                house_data = house_data.append(data_row, ignore_index=True)
            house_data.to_csv("./58/second_hard_house_58_" + ct + ".csv")
            print(
                ct + " " + str(page_id) + ":------------------------------saved--------------------------------------")
        print(ct + ":-----------------------------finished-------------------------------")
    print("-----------------------------finished-------------------------------")


if __name__ == '__main__':
    run()
