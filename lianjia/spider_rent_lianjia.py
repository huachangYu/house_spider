from lib.spider_house import get_html
from bs4 import BeautifulSoup
import re
import pandas as pd
import warnings
import time
import os

warnings.filterwarnings("ignore")


def get_hrefs(html, head_url="https://hz.lianjia.com"):
    soup = BeautifulSoup(html)
    list_soup = soup.find(attrs={"class": "content__list"})
    list_soup = list_soup.find_all(attrs={"class": "content__list--item"})
    hrefs = []
    for li in list_soup:
        try:
            href = head_url + li.find(attrs={"class": "content__list--item--aside"}).attrs["href"]
        except Exception:
            continue
        hrefs += [href]
    return hrefs


def get_name(house_html):
    soup = BeautifulSoup(house_html)
    name = soup.find(attrs={"class": "content__title"}).text.strip()
    return name


def get_price(house_html):
    soup = BeautifulSoup(house_html)
    price_str = soup.find(attrs={"class": "content__aside--title"}).find("span").text.strip()
    return int(price_str)


def get_info(hosue_html):
    soup = BeautifulSoup(hosue_html)
    info_soup = soup.find(attrs={"class": "content__aside__list"}).find_all("li")
    info_str = info_soup[1].text.split("：")[-1].split(" ")
    structure = info_str[0]
    area = float(info_str[1].strip().replace("㎡", ""))
    return structure,area


def get_xy(house_html):
    pattern = "longitude: '.*?',\s+latitude: '.*?'"
    xy_str = re.search(pattern, house_html)
    xy = re.findall('(([0-9]|\.)+)', xy_str.group())
    return [float(xy[1][0]), float(xy[0][0])]


def run():
    cities = ["hz", "cd", "bj", "sh", "hui", "nb"]
    maxpage = [100, 100, 100, 100, 100, 100]
    for city, maxpagei in zip(cities, maxpage):
        house_data = pd.DataFrame(columns=("name", "month_price", "house_structure", "area", "x", "y"))
        csv_path = "./data/rent_lianjia_" + city + ".csv"
        for page_id in range(1, maxpagei):
            url = "https://" + city + ".lianjia.com/zufang/pg" + str(page_id)
            try:
                html = get_html(url)
                hrefs = get_hrefs(html)
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
                    structure, area = get_info(house_html)
                    data_row = {"name": name, "month_price": price, "house_structure": structure, "area": area,
                                "x": xy[0], "y": xy[1]}
                except Exception:
                    print("----------------error----------------")
                    print(href)
                    time.sleep(10)
                    continue
                house_data = house_data.append(data_row, ignore_index=True)
                print(data_row)
            house_data.to_csv(csv_path)
            print(city + " " + str(
                page_id) + ":------------------------------saved--------------------------------------")
        print(city + ":-----------------------------finished-------------------------------")
    print("-----------------------------finished-------------------------------")


if __name__ == '__main__':
    run()
