from lib.spider_house import get_html
from bs4 import BeautifulSoup
import re
import pandas as pd
import warnings
import time
import os

warnings.filterwarnings("ignore")


def get_refs(html):
    soup = BeautifulSoup(html)
    list_soup = soup.find(attrs={"class": "sellListContent"})
    list_soup = list_soup.find_all("li")
    hrefs = []
    for li in list_soup:
        try:
            href = li.find(attrs={"class": "title"}).find("a").attrs["href"]
        except Exception:
            continue
        hrefs += [href]
    return hrefs


def get_name(house_html):
    soup = BeautifulSoup(house_html)
    name = soup.find(attrs={"class": "main"}).text
    return name.strip()


def get_price(house_html):
    soup = BeautifulSoup(house_html)
    price_str = soup.find(attrs={"class": "total"}).text
    average_price_str = soup.find(attrs={"class": "unitPriceValue"}).text
    average_price_str = re.search('(([0-9]|\.)+)', average_price_str)
    return float(price_str.strip()), float(average_price_str.group())


def get_info(house_html):
    soup = BeautifulSoup(house_html)
    info_list = soup.find(attrs={"class": "base"}).find_all("li")
    try:
        year = int(info_list[11].text.replace("产权年限", "").replace("年", ""))
    except Exception:
        year = info_list[11].text.replace("产权年限", "")
    return info_list[0].text.replace("房屋户型", ""), float(info_list[2].text.replace("建筑面积", "").replace("㎡", "")), year


def get_xy(house_html):
    pattern = "resblockPosition:'.*?'"
    xy_str = re.search(pattern, house_html)
    xy = re.findall('(([0-9]|\.)+)', xy_str.group())
    return [float(xy[1][0]), float(xy[0][0])]


def get_build_year(house_html):
    soup = BeautifulSoup(house_html)
    info_list = soup.find(attrs={"class": "transaction"}).find_all("li")
    return info_list[2].find_all("span")[1].text.split("-")[0]


def get_type(house_html):
    soup = BeautifulSoup(house_html)
    info_list = soup.find(attrs={"class": "transaction"}).find_all("li")
    return info_list[1].find_all("span")[1].text


def run():
    cities = ["bj", "sh", "hui", "nb"]
    maxpage = [ 100, 100, 100, 100]
    # cities = ["hz", "cd", "bj", "sh", "hui", "nb"]
    # maxpage = [100, 100, 100, 100, 100, 100]
    for city,maxpagei in zip(cities,maxpage):
        house_data = pd.DataFrame(columns=(
            "name", "type", "build_year", "year", "total_price", "average_price", "house_structure", "area", "x", "y"))
        csv_path = "./data/second_hard_house_lianjia_" + city + ".csv"
        for page_id in range(1, maxpagei+1):
            url = "https://" + city + ".lianjia.com/ershoufang/pg" + str(page_id) + "/"
            try:
                html = get_html(url)
                hrefs = get_refs(html)
            except Exception:
                print("-------------------big error-------------------")
                print(url)
                time.sleep(60)
                continue
            for href in hrefs:
                try:
                    house_html = get_html(href)
                    name = get_name(house_html)
                    price, ave_price = get_price(house_html)
                    structure, area, year = get_info(house_html)
                    xy = get_xy(house_html)
                    build_year = get_build_year(house_html)
                    type = get_type(house_html)
                    data_row = {"name": name, "type": type, "build_year": build_year, "year": year,
                                "total_price": price,
                                "average_price": ave_price, "house_structure": structure, "area": area, "x": xy[0],
                                "y": xy[1]}
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
