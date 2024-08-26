import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, errors
from urllib.parse import urljoin
import threading
import time

class VNExpressCrawler:
    def __init__(self, mongo_client, db, collection):
        self.client = mongo_client
        self.db = self.client[db]
        self.collection = self.db[collection]
        self.now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        self.two_weeks_ago = self.now - timedelta(weeks=2)
        self.three_weeks_ago = self.now - timedelta(weeks=3)
        self.page_count = 0
        self.stop_event = threading.Event()
        self.status = 'Not Started'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def crawl(self):
        self.status = 'Running'
        try:
            print("Start crawl VN express")
            response = requests.get('https://vnexpress.net/', headers=self.headers, timeout=30)
            print("Get main page successfully")
            time.sleep(2)  # Delay after the request
            response.raise_for_status()  # Raise an HTTPError for bad responses
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch the main page: {e}")
            self.status = 'Failed'
            return

        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            category_links = soup.select('nav.main-nav ul.parent li a')
            for category_link in category_links:
                if self.stop_event.is_set():
                    self.status = 'Stopped'
                    return
                print("This is category link")
                print(category_link['href'])
                category_url = urljoin('https://vnexpress.net', category_link['href'])
                self.crawl_category(category_url)
                time.sleep(2)  # Delay between each category crawl
        except Exception as e:
            print(f"Error during crawl: {e}")
        finally:
            self.status = 'Completed'

    def crawl_category(self, category_url):
        if self.page_count >= 5:
            return
        try:
            response = requests.get(category_url, headers=self.headers, timeout=30)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            time.sleep(2)  # Delay after the request
        except requests.exceptions.RequestException as e:
            print(f"Failed to load page {category_url}: {e}")
            return

        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            article_links = soup.select('article .title-news a')
            for article_link in article_links:
                if self.stop_event.is_set():
                    return
                article_url = urljoin('https://vnexpress.net', article_link['href'])
                self.crawl_article(article_url)
                time.sleep(2)  # Delay between each article crawl

            next_page_link = soup.select_one('a.next-page')
            if next_page_link:
                self.page_count += 1
                next_page_url = urljoin('https://vnexpress.net', next_page_link['href'])
                self.crawl_category(next_page_url)
                time.sleep(2)  # Delay before going to the next page
        except Exception as e:
            print(f"Error during category crawl: {e}")

    def crawl_article(self, article_url):
        if self.stop_event.is_set():
            return
        try:
            response = requests.get(article_url, headers=self.headers, timeout=30)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            time.sleep(2)  # Delay after the request
        except requests.exceptions.RequestException as e:
            print(f"Failed to load article page {article_url}: {e}")
            return

        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            time_post = soup.select_one('div.header-content span.date')
            if time_post:
                time_post = self.convert_time_format(time_post.text)
                article_date = datetime.fromisoformat(time_post)
            else:
                article_date = self.now

            if not (self.three_weeks_ago <= article_date <= self.now):
                print(f"Article with URL {article_url} is outside the desired date range.")
                return

            if soup.select('article.fck_detail div.item_quiz .tittle_quiz'):
                print(f"Article with URL {article_url} contains a quiz and will not be inserted into MongoDB.")
                return

            author = soup.select_one('p.Normal[style="text-align:right;"] strong')
            tag_list = [tag.text.strip() for tag in soup.select('nav.main-nav ul.parent li.active a')]

            title = soup.select_one('h1.title-detail').text if soup.select_one('h1.title-detail') else None
            summary = soup.select_one('p.description').text if soup.select_one('p.description') else None
            content = " ".join([p.text for p in soup.select('article.fck_detail p.Normal')])

            article_data = {
                'source': 'Báo VnExpress',
                'url': article_url,
                'title': title,
                'summary': summary,
                'author': author.text if author else None,
                'time': article_date,
                'tagList': tag_list,
                'content': content
            }

            try:
                self.collection.insert_one(article_data)
                print(f"Article {title} inserted into MongoDB.")
                time.sleep(2)  # Delay after inserting into MongoDB
            except errors.OperationFailure as e:
                print(f"Failed to insert article {title} into MongoDB: {e}")
        except Exception as e:
            print(f"Error during article crawl: {e}")

    def convert_time_format(self, time_str):
        try:
            time_str = time_str.split(', ', 1)[1].rsplit(' ', 1)[0]
            dt = datetime.strptime(time_str, '%d/%m/%Y, %H:%M')
            dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt).isoformat()
            return dt
        except ValueError as e:
            print(f"Error parsing time: {e}")
            return self.now.isoformat()

    def stop(self):
        self.stop_event.set()

    def get_status(self):
        return self.status
