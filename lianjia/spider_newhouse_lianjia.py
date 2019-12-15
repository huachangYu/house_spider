import sys
sys.path.append("..")
from lib.spider_house import get_html
from bs4 import BeautifulSoup
import warnings
import re
import pandas as pd
import time

warnings.filterwarnings("ignore")


def get_hrefs(html, url_head):
    soup = BeautifulSoup(html)
    list_soup = soup.find(attrs={"class": "resblock-list-wrapper"}).find_all("li")
    hrefs = []
    for li in list_soup:
        try:
            href = url_head + li.find(attrs={"class": "name"}).attrs["href"]
            hrefs += [href]
        except Exception:
            continue
    return hrefs


def get_name(house_html):
    soup = BeautifulSoup(house_html)
    name = soup.find(attrs={"class": "DATA-PROJECT-NAME"}).text.strip()
    return name


def get_price(house_html):
    soup = BeautifulSoup(house_html)
    # price = soup.find(attrs={"class": "junjia"}).text
    price = soup.find_all(attrs={"class": "price-number"})[0].text
    return int(price)


def get_structure(house_html, url_head):
    structures = []
    soup = BeautifulSoup(house_html)
    try:
        href = url_head + soup.find(attrs={"class": "h2-flow"}).find("a").attrs["href"]
        structure_html = get_html(href)
        structure_soup = BeautifulSoup(structure_html)
        list_soup = structure_soup.find(attrs={"class": "main-wrap huxingtu"}).find_all(attrs={"class": "huxing-item"})
        for li in list_soup:
            info = li.find(attrs={"class": "info clear"})
            lis = info.find("ul").find_all("li")
            structure = lis[0].text.split(":")[-1].strip()
            area = re.search("(([0-9]|\.)+)", lis[1].text).group()
            total_price = li.find(attrs={"class": "price"}).find("i").text
            data_row = [{"structure": structure, "area": int(area), "total_price": int(total_price)}]
            structures += data_row
        return structures
    except Exception:
        return structures


def get_xy(house_html):
    soup = BeautifulSoup(house_html)
    xy_str = soup.find(attrs={"id": "mapWrapper"}).attrs["data-coord"]
    pattern_num = "(([0-9]|\.)+)"
    xy = re.findall(pattern_num, xy_str)
    return [float(xy[0][0]), float(xy[1][0])]


def get_year(house_html):
    try:
        soup = BeautifulSoup(house_html)
        info_soup = soup.find(attrs={"class": "mod-panel mod-details"}).find(attrs={"class": "box-loupan"}).find(
            attrs={"class": "table-list clear"}).find_all("li")
        year_str = info_soup[4].find(attrs={"class": "label-val"}).text.replace("年", "")
        return int(year_str)
    except Exception:
        return None


def run():
    cities = ["hz", "cd", "bj", "sh", "hui", "nb"]
    maxpage = [41, 84, 21, 82, 24, 24]
    for city, max_pageid in zip(cities, maxpage):
        house_data = pd.DataFrame(columns=("name", "price", "year", "x", "y", "house_type_num", "house_structure_area"))
        csv_path = "./data/newhouse_lianjia_new_" + city + ".csv"
        head_url = "https://" + city + ".fang.lianjia.com"
        for page_id in range(1, max_pageid + 1):
            url = head_url + "/loupan/nhs1pg" + str(page_id)
            try:
                html = get_html(url)
                hrefs = get_hrefs(html, url_head="https://hz.fang.lianjia.com")
            except Exception:
                print("-------------------big error-------------------")
                print(url)
                time.sleep(60)
                continue
            for href in hrefs:
                try:
                    house_html = get_html(href)
                    name = get_name(house_html)
                    price = get_price(house_html)
                    xy = get_xy(house_html)
                    structure = get_structure(house_html, url_head="https://hz.fang.lianjia.com")
                    year = get_year(house_html)
                    data_row = {"name": name, "price": price, "year": year, "x": xy[0], "y": xy[1],
                                "house_type_num": len(structure), "house_structure_area": structure}
                except Exception:
                    print("----------------error----------------")
                    print(href)
                    time.sleep(10)
                    continue
                print(data_row)
                house_data = house_data.append(data_row, ignore_index=True)
            house_data.to_csv(csv_path)
            print(city + " " + str(
                page_id) + ":------------------------------saved--------------------------------------")
        print(city + ":-----------------------------finished-------------------------------")
    print("-----------------------------finished-------------------------------")


if __name__ == '__main__':
    run()
