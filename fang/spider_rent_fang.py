from lib.spider_house import get_html
from bs4 import BeautifulSoup
import re
import time
import threading
import os
from lib.yhc_thread import YhcThread
import warnings
import pandas as pd

warnings.filterwarnings("ignore")


def get_hrefs(html, url_head):
    soup = BeautifulSoup(html)
    hrefs = []
    list_soup = soup.find(attrs={"class": "houseList"}).find_all("dl")
    for li in list_soup:
        try:
            href = url_head + li.find(attrs={"class": "title"}).find("a").attrs["href"]
        except Exception:
            continue
        hrefs += [href]
    return hrefs


def get_redirect_url(url):
    html = get_html(url)
    pattern = "t3='.*?'"
    part_url_str = re.search(pattern, html).group()
    part_url = part_url_str.replace("t3='", "").replace("'", "")
    return url + "?" + part_url


def get_redirect_html(href):
    href = get_redirect_url(href)
    house_html = get_html(href)
    return house_html


def get_name(soup):
    name = soup.find(attrs={"class": "tab-cont clearfix"}).find(attrs={"class": "title"}).text.strip()
    return name


def get_price(soup):
    try:
        price_str = soup.find(attrs={"class": "trl-item sty1"}).find("i").text
    except Exception:
        price_str = soup.find(attrs={"class": "trl-item sty1 rel"}).find("i").text
    return int(price_str)


def get_structure(soup):
    list_soup = soup.find_all(attrs={"class": "tt"})
    return list_soup[1].text.strip()


def get_area(soup):
    list_soup = soup.find_all(attrs={"class": "tt"})
    area_str = list_soup[2].text.strip()
    pattern_num = '(([0-9]|\.)+)'
    return float(re.search(pattern_num, area_str).group())


def get_xy(house_html):
    pattern = "codex: '.*?',\s+codey: '.*?'"
    xy_str = re.search(pattern, house_html)
    xy = re.findall('(([0-9]|\.)+)', xy_str.group())
    return [float(xy[1][0]), float(xy[0][0])]


def get_info_from_house_html(href):
    try:
        try:
            house_html = get_redirect_html(href)
            soup = BeautifulSoup(house_html)
            name = get_name(soup)
            price = get_price(soup)
            structure = get_structure(soup)
            area = get_area(soup)
            xy = get_xy(house_html)
        except Exception:
            house_html = get_html(href)
            soup = BeautifulSoup(house_html)
            name = get_name(soup)
            price = get_price(soup)
            structure = get_structure(soup)
            area = get_area(soup)
            xy = get_xy(house_html)
        data_row = {"name": name, "month_price": price, "house_structure": structure, "area": area, "x": xy[0],
                    "y": xy[1]}
        return data_row
    except Exception:
        return None


def run():
    cities = ["hz", "cd", "bj", "sh", "huizhou", "nb"]
    page_nums = [100, 100, 100, 100, 100, 100]
    for ind,city in enumerate(cities):
        house_data = pd.DataFrame(columns=("name", "month_price", "house_structure", "area", "x", "y"))
        csv_path = "./data1207/rent_fang_" + city + ".csv"
        if os.path.exists(csv_path):
            house_data = pd.read_csv(csv_path, index_col=0)
        url_head = "https://" + city + ".zu.fang.com"
        if city == "bj":
            url_head = "https://zu.fang.com/"
        for page_id in range(1, page_nums[ind]+1):
            url = url_head + "/house/i3" + str(page_id) + "/"
            try:
                html = get_redirect_html(url)
                hrefs = get_hrefs(html, url_head)
            except Exception:
                try:
                    html = get_html(url)
                    hrefs = get_hrefs(html, url_head)
                except Exception:
                    print("--------------------------------big error---------------------------")
                    print(url)
                    time.sleep(60)
                    continue
            thread_list = []
            for href in hrefs:
                thread_list += [YhcThread(get_info_from_house_html, args=(href,))]
            for t in thread_list:
                t.start()
            for t in thread_list:
                t.join(10)
                result = t.get_result()
                if result is not None:
                    print(result)
                    house_data = house_data.append(result, ignore_index=True)
                else:
                    print("error....")
                    print(t.get_args())
            house_data.to_csv(csv_path)
            print(city + " " + str(page_id) + ":----saved to " + csv_path + "--------------")
        print(city + "----------------------------finished---------------------------------")
    print("---------------------------------all finished-----------------------------------------")


if __name__ == '__main__':
    run()
