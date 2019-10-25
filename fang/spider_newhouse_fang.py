# -*- coding:gbk -*-
import requests
from bs4 import BeautifulSoup
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
    soup = BeautifulSoup(html)
    houses = soup.find(id="newhouse_loupai_list").find('ul').find_all('li')
    hrefs = []
    for i in range(len(houses)):
        a = houses[i].find('a')
        if a is None:
            continue
        href = 'https:' + a.attrs['href'] + '?ctm=1.hz.xf_search.lplist.' + str(i + 1)
        hrefs += [href]
    return hrefs


def get_xy(html_house):
    soup_house_html = BeautifulSoup(html_house)
    href_map = 'https:' + soup_house_html.find(id="iframe_map").attrs['src']
    html_map = get_html(href_map)
    pattern = '\"mapx\":\"([0-9]|\.)+\",\"mapy\":\"([0-9]|\.)+\"'
    pattern_baidu = '\"baidu_coord_x\":\"([0-9]|\.)+\",\"baidu_coord_y\":\"([0-9]|\.)+\"'
    result = re.search(pattern, html_map)
    result_baidu = re.search(pattern_baidu, html_map)
    result_str = result.group() + ',' + result_baidu.group()
    pattern_num = '(([0-9]|\.)+)'
    xy = re.findall(pattern_num, result_str)
    return [float(xy[0][0]), float(xy[1][0])]


def get_name(html_house):
    soup_house_html = BeautifulSoup(html_house)
    name = soup_house_html.find('h1').find('strong').text
    return name


def get_price(html_house):
    soup_house_html = BeautifulSoup(html_house)
    price_str = soup_house_html.find(attrs={'class': 'prib cn_ff'}).text
    try:
        price = int(price_str.strip())
    except Exception:
        price = price_str
    return price


def get_type(html_house):
    soup_house_html = BeautifulSoup(html_house)
    house_type = soup_house_html.find(attrs={'class': 'biaoqian1'}).find('a').text
    return house_type


def get_history_price(html_house):
    pattern = '{\"name\":.*?\"datatype\":\"house\".*?}'
    history = re.search(pattern, html_house).group()
    return history


def get_structure(house_html):
    soup = BeautifulSoup(house_html)
    strutures = []
    try:
        struture_url = "https:" + soup.find(attrs={"class": "jushi fr"}).find_all("a")[-1].attrs["href"]
        struture_html = get_html(struture_url)
        strutures += get_structure_from_html(struture_html)
        return strutures
    except Exception:
        return strutures


def get_structure_from_html(struture_html):
    soup = BeautifulSoup(struture_html)
    list_soup = soup.find(id="ListModel").find_all("li")
    strutures = []
    for li in list_soup:
        info_soup = li.find(attrs={"class": "tiaojian"})
        struture = decode(info_soup.find(attrs={"class": "fl"}).text)
        area_str = decode(info_soup.find(attrs={"class": "fr"}).text)
        ind = area_str.index("㎡")
        area = float(area_str[:ind])
        data_row = {"struture": struture, "area": area}
        strutures += [data_row]
    return strutures


def decode(str):
    return str.encode('iso-8859-1').decode('gbk')


def run():
    cities = ["huizhou", "nb"]
    page_nums = [17, 24]
    for city, page_num in zip(cities, page_nums):
        file_path = "./fang/fang_newhouse/newhouse_fang_" + city + ".csv"
        house_data = pd.DataFrame(columns=("name", "price", "year", "x", "y", "house_type_num", "house_structure_area"))
        url_head = "https://" + city + ".newhouse.fang.com/house/s/b9"
        for page_id in range(1, page_num + 1):
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
                    html_house = get_html(href, trans_code=True)
                    xy = get_xy(html_house)
                    name = get_name(html_house)
                    price = get_price(html_house)
                    struture = get_structure(html_house)
                except Exception:
                    print("error....")
                    print(href)
                    time.sleep(10)
                    continue
                data_row = {"name": name, "price": price, "year": None, "x": xy[1], "y": xy[0],
                            "house_type_num": len(struture), "house_structure_area": struture}
                print(data_row)
                house_data = house_data.append(data_row, ignore_index=True)
            house_data.to_csv(file_path)
            print(city + " " + str(page_id) + ":----saved to " + file_path + "--------------")
        print(city + "----------------------------finished---------------------------------")
    print("---------------------------------all finished-----------------------------------------")


if __name__ == '__main__':
    run()
