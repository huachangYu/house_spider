from lib.spider_house import get_html
from bs4 import BeautifulSoup
import warnings
import re
import pandas as pd
import time
import os
import requests
from lib.yhc_thread import YhcThread

warnings.filterwarnings("ignore")


def get_redirect_url(url):
    html = get_html(url)
    pattern = "t3='.*?'"
    part_url_str = re.search(pattern, html).group()
    part_url = part_url_str.replace("t3='", "").replace("'", "")
    return url + "?" + part_url


def get_hrefs(html, url_head="http://hz.esf.fang.com"):
    soup = BeautifulSoup(html)
    houses = soup.find(attrs={"class": "shop_list shop_list_4"}).find_all("h4")
    hrefs = []
    for li in houses:
        try:
            href = url_head + li.find("a").attrs["href"]
        except Exception:
            continue
        hrefs += [href]
    return hrefs


def get_name(house_html):
    soup = BeautifulSoup(house_html)
    name_soup = soup.find(attrs={"class": "title floatl"})
    return name_soup.text.strip()


def get_price(house_html):
    soup = BeautifulSoup(house_html)
    price_str = soup.find(attrs={"class": "trl-item price_esf sty1"}).find("i").text
    return float(price_str)


def get_info(house_html):
    soup = BeautifulSoup(house_html)
    info_soup = soup.find(attrs={"class": "tr-line clearfix"})
    return info_soup.find(attrs={"class": "trl-item1 w146"}).text.strip().split("\n")[0], float(
        info_soup.find(attrs={"class": "trl-item1 w182"}).text.strip().split("\n")[0].replace("平米", "")), int(
        info_soup.find(attrs={"class": "trl-item1 w132"}).text.strip().split("\n")[0].replace("元/平米", ""))


def get_xy(html_house):
    soup_house_html = BeautifulSoup(html_house)
    href_map = 'https:' + soup_house_html.find(id="iframeBaiduMap").attrs['data-src']
    html_map = get_html(href_map)
    pattern = '_vars.cityx = \"([0-9]|\.)+\";_vars.cityy = \"([0-9]|\.)+\"'
    result = re.search(pattern, html_map).group()
    pattern_num = '(([0-9]|\.)+)'
    xy = re.findall(pattern_num, result)
    return [float(xy[3][0]), float(xy[1][0])]


def get_type_buildyear(house_html):
    soup = BeautifulSoup(house_html)
    infos_soup = soup.find(attrs={"class": "cont clearfix"}).find_all("div")
    type_str = infos_soup[2].find(attrs={"class": "rcont"}).text.strip()
    year_str = infos_soup[0].find(attrs={"class": "rcont"}).text.strip().replace("年", "")
    return type_str, int(year_str)


def get_data_row(href):
    try:
        if href.find("?") == -1:
            href = get_redirect_url(href)
        house_html = get_html(href)
        name = get_name(house_html)
        price = get_price(house_html)
        structure, area, ave_price = get_info(house_html)
        xy = get_xy(house_html)
        type_str, build_year = get_type_buildyear(house_html)
        data_row = {"name": name, "type": type_str, "build_year": build_year, "year": None,
                    "total_price": price, "average_price": ave_price, "house_structure": structure,
                    "area": area, "x": xy[0], "y": xy[1]}
        return data_row
    except Exception:
        return None


def run():
    cities = ["hz", "cd", "bj", "sh", "huizhou", "nb"]
    for city in cities:
        house_data = pd.DataFrame(columns=(
            "name", "type", "build_year", "year", "total_price", "average_price", "house_structure", "area", "x", "y"))
        csv_path = "./data1207/second_hard_house_fang_" + city + ".csv"
        if os.path.exists(csv_path):
            house_data = pd.read_csv(csv_path, index_col=0)
        url_head = "https://" + city + ".esf.fang.com"
        for page_id in range(1, 101):
            url = "https://" + city + ".esf.fang.com/house/i3" + str(page_id) + "/"
            try:
                url = get_redirect_url(url)
                html = get_html(url)
                hrefs = get_hrefs(html, url_head)
            except Exception:
                print("-------------------big error-------------------")
                print(url)
                time.sleep(60)
                continue
            thread_list = []
            for href in hrefs:
                thread_list += [YhcThread(get_data_row, args=(href,))]
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
            print(city + " " + str(page_id) + ":---------saved to " + csv_path + "---------------------")
        print(city + ":--------------------finished----------------------")
    print("---------------------all finished-------------------------")


if __name__ == '__main__':
    run()
