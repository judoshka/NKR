from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from sync_database import news_collection, spread_collection, meta_collection
import time
from dateutil import parser
from random import randint
import re
import os
import json

BASE_URL = "https://tgstat.ru"
SEARCH_URL = "https://tgstat.ru/search"
STATUSES = ["Не обработан", "Обработан"]
default_example = "https://tgstat.ru/channel/@fighter_bomber/6776"
reposted_url = "https://tgstat.ru/channel/@Pro_viZia/290"

views = 0
forwards = 0

def process_text(text):
    text = text.replace("\n", " ")
    emoj = re.compile("["
                      u"\U0001F600-\U0001F64F"  # emoticons
                      u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                      u"\U0001F680-\U0001F6FF"  # transport & map symbols
                      u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                      u"\U00002500-\U00002BEF"  # chinese char
                      u"\U00002702-\U000027B0"
                      u"\U00002702-\U000027B0"
                      u"\U000024C2-\U0001F251"
                      u"\U0001f926-\U0001f937"
                      u"\U00010000-\U0010ffff"
                      u"\u2640-\u2642"
                      u"\u2600-\u2B55"
                      u"\u200d"
                      u"\u23cf"
                      u"\u23e9"
                      u"\u231a"
                      u"\ufe0f"  # dingbats
                      u"\u3030"
                      "]+", re.UNICODE)
    text = emoj.sub(r'', text)
    VALUE = min(64, len(text))
    new_text = text[:VALUE]
    i = VALUE
    while i < len(text):
        char = text[i]
        if char.isalpha():
            new_text += char
        else:
            break
        i += 1
    return new_text

def get_date(driver, url):
    driver.get(url)
    date_str = driver.find_element(by=By.XPATH, value="//span[@class='channel-post-title']").text
    date_str = date_str.split(" (")[0]
    return date_str

def get_message(driver, url):
    return "Скрипт находится в разработке"
    # driver.get(url)
    # return driver.find_element(by=By.XPATH, value='//div[@class="post-body"]').text

def dfs(driver, url, s_name, s_id):
    driver.get(url)
    WebDriverWait(driver, 2)
    time.sleep(2)
    global views
    global forwards
    try:
        views = driver.find_element(by=By.XPATH,
            value="//a[@data-original-title='Количество просмотров публикации' or "
            "contains(@data-original-title, 'views') ]"
        ).text.strip()
    except NoSuchElementException:
        views = None
    try:
        forwards = driver.find_element(by=By.XPATH,
            value="//span[@data-original-title='Пересылок всего' or "
            "contains(@data-original-title, 'Total shares')]"
        ).text.strip()
    except NoSuchElementException:
        forwards = None
    url += '/quotes'
    driver.get(url)
    WebDriverWait(driver, 2)
    time.sleep(2)
    links = []
    to = []

    xpath = f"//a[contains(text(), 'репостнул') or contains(text(), 'упомянул') or contains(text(), 'mentioned') " \
            f"or contains(text(), 'forwarded')]"
    values = driver.find_elements(by=By.XPATH, value=xpath)
    for value in values:
        link = value.get_attribute('href')
        links.append({
            'type': value.text.strip(),
            'href': link
        })
    repost_dates = []
    xpath = "//div[@class='col col-5 align-items-center text-right']/div/small"
    values = driver.find_elements(by=By.XPATH, value=xpath)
    for value in values:
        date = value.text.strip()
        if date:
            repost_dates.append(date)
    try:
        driver.find_element(by=By.XPATH, value="//a[@href='#mentions-by-chats']").click()
    except NoSuchElementException:
        return to
    xpath = "//div[@class='col col-5 align-items-center text-right']/div/small"
    values = driver.find_elements(by=By.XPATH, value=xpath)
    if not values:
        return to
    for value in values:
        date = value.text.strip()
        if date:
            repost_dates.append(date)
    for i, link in enumerate(links):
        link_type = link.get('type')
        link_href = link.get('href')
        link_url = link_href
        parsed = link_href.split("/")
        channel_name = parsed[-2]
        post_id = parsed[-1]
        message = 'UNKNOWN'
        record = {
            'channel_name': channel_name,
            'news_id': post_id,
            'message': message,
            'date': repost_dates[i],
            'report_type': link_type,
            'from': {'channel_name': s_name, 'news_id': s_id},
            'to': dfs(driver, link_url, channel_name, post_id),
            'views': views,
            'forwards': forwards,
        }
        to.append(record)
    return to

def find_root(driver, url):
    driver.get(url)
    try:
        root = driver.find_element(by=By.XPATH, value="//noindex/a[@rel='nofollow']")
        href = root.get_attribute("href")
        if not re.match("https://t\.me/\s*/\d+", href):
            raise NoSuchElementException
        objects = href.split("/")
        url = BASE_URL + "/channel/" + "@" + objects[-2] + "/" + objects[-1]
        return find_root(driver, url)
    except NoSuchElementException:
        try:
            xpath = "//div[@class='post-from']"
            element = driver.find_element(by=By.XPATH, value=xpath)
            if element:
                is_repost = element.text
                if not ('Repost' in is_repost or 'Репост' in is_repost):
                    raise IndexError
            if element:
                href_element = element.find_element(by=By.XPATH, value='./a')
                source_channel_name = href_element.text
                source_channel_href = href_element.get_attribute('href').split('/')[-1]
                objects = url.split('/')
                meta_collection.insert_one({
                    'source_name': source_channel_name,
                    'source_href': source_channel_href,
                    'target_channel_name': objects[-2],
                    'target_channel_news_id': objects[-1]
                })
                return url
        except IndexError:
            pass
    return url

def get_records(size=20):
    with open("no_sources_found.json", 'r', encoding='utf-8') as f:
        records = json.load(f)

    # with open('filters.json', 'r', encoding='utf-8') as f:
    #     filters = json.load(f)
    # records = []
    # for f_filter in filters:
    #     search_filter = {**f_filter, 'Статус': 'Не обработан'}
    #     search_filter['date'] = parser.parse(search_filter['date'])
    #     record = news_collection.find_one(search_filter)
    #     if record:
    #         records.append(record)

    return records

def get_channel_description():
    path = os.path.join(os.getcwd(), 'report', 'some_desc.json')
    with open(path, 'r', encoding='utf-8') as f:
        desc = json.load(f)

    for key in desc:
        item = desc[key]
        channel_name = "@" + item["link"].split("/")[-1]
        item["name"] = channel_name
        item["url"] = BASE_URL + "/channel/" + channel_name
    keys = list(desc.keys())
    for key in keys:
        desc[int(key)] = desc.pop(key)
    return desc

def update_status(record_id, status):
    news_collection.update_one({"_id": record_id}, {"$set": {"Статус": status}})

def start_parse(driver, records, desc):
    for record in records:
        channel_id = record['source_name']
        channel_name = record['name']
        post_id = record['source_post_id']
        post_url = "/".join(['https://tgstat.ru/en/channel', channel_id, str(post_id)])
        driver.get(post_url)
        message = "UNKNOWN"
        root = post_url
        try:
            views = driver.find_element(by=By.XPATH,
                value="//a[@data-original-title='Количество просмотров публикации' or "
                "contains(@data-original-title, 'views') ]"
            ).text.strip()
        except NoSuchElementException:
            views = None
        try:
            forwards = driver.find_element(by=By.XPATH,
                value="//span[@data-original-title='Пересылок всего' or "
                "contains(@data-original-title, 'Total shares')]"
            ).text.strip()
        except NoSuchElementException:
            forwards = None
        date = driver.find_element(by=By.XPATH, value="//p/small").text
        # channel_id = record.get("peer_id", {}).get("channel_id", 0)
        # post_id = record.get("id", 0)
        # channel_url = desc[channel_id]["url"]
        # if not post_id or not channel_id:
        #     continue
        # post_url = "/".join([channel_url, str(post_id)])
        # fwd_from = record.get("fwd_from")
        # if fwd_from:
        #     root = find_root(driver, post_url)
        # else:
        #     root = post_url

        if root == post_url:
            # channel_name = desc[channel_id]["name"]
            # source = None
            # message = record.get("message", None)
            # date = record.get("date", None)
            # views = record.get("views", None)
            # forwards = record.get("forwards", None)
            pass
        else:
            if root.startswith("https://"):
                post_url = root
            else:
                post_url = BASE_URL + root
            obj = post_url.split("/")
            channel_name = obj[-3]
            post_id = int(obj[-2])
            message = "UNKNOWN"
            date = get_date(driver, post_url)
            try:
                views = driver.find_element(by=By.XPATH,
                    value="//a[@data-original-title='Количество просмотров публикации' or "
                    "contains(data-original-title, 'views') ]"
                ).text.strip()
            except NoSuchElementException:
                views = None
            try:
                forwards = driver.find_element(by=By.XPATH,
                    value="//span[@data-original-title='Пересылок всего' or "
                    "contains(data-original-title, 'Total shares')]"
                ).text.strip()
            except NoSuchElementException:
                forwards = None
        source = {"channel_name": channel_name, "news_id": post_id}
        dissemination_record = {
            "channel_name": channel_name,
            "news_id": post_id,
            "message": message,
            "date": date,
            'views': views,
            'forwards': forwards,
            "from": source,
            "to": dfs(driver, post_url, channel_name, post_id)
        }
        spread_collection.insert_one(dissemination_record)
        # update_status(record["_id"], "Обработан")
        time.sleep(randint(8, 12))

    driver.close()

def init():
    return webdriver.Chrome("/Users/mihailulizko/PycharmProjects/telegram_template/chromedriver")

def init_statuses():
    news_collection.update_many({}, {"$set": {"Статус": "Не обработан"}})

if __name__ == '__main__':
    # init_statuses()
    desc = get_channel_description()
    while True:
        records = get_records()
        if not records:
            break
        driver = init()
        start_parse(driver, records, desc)
        time.sleep(60)
