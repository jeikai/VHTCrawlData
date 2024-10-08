import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, errors
from urllib.parse import urljoin
import threading

class Kenh14Crawler:
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
            response = requests.get('https://kenh14.vn/', headers=self.headers, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')

            category_links = soup.select('.khw-bottom-header ul.kbh-menu-list li.kmli > a')
            for category_link in category_links:
                if category_link['href'] not in ['javascript:;', 'http://video.kenh14.vn/', "/"]:
                    if self.stop_event.is_set():
                        self.status = 'Stopped'
                        return
                    category_url = urljoin('https://kenh14.vn', category_link['href'])
                    self.crawl_category(category_url)
        finally:
            # self.client.close()
            self.status = 'Completed'

    def crawl_category(self, category_url):
        if self.page_count >= 5:
            return
        
        try:
            response = requests.get(category_url, headers=self.headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to load page {category_url}: {e}")
            return

        soup = BeautifulSoup(response.content, 'html.parser')
        
        top_news_urls = [urljoin('https://kenh14.vn', a['href']) for a in soup.select('div.klw-top-news ul.ktnc-list li.ktncli > a')]
        slide_wrapper_urls = [urljoin('https://kenh14.vn', a['href']) for a in soup.select('div.klwfn-slide-wrapper ul.knswli-object-list li.klwfnswn > a')]

        for article_url in top_news_urls + slide_wrapper_urls:
            if self.stop_event.is_set():
                return
            self.crawl_article(article_url)

    def crawl_article(self, article_url):
        if self.stop_event.is_set():
            return
        
        try:
            response = requests.get(article_url, headers=self.headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to load article page {article_url}: {e}")
            return
        
        soup = BeautifulSoup(response.content, 'html.parser')

        time_post = soup.select_one('span.kbwcm-time')
        if time_post:
            time_post = self.convert_time_format(time_post.text)
            article_date = datetime.fromisoformat(time_post)
        else:
            article_date = self.now

        if not (self.three_weeks_ago <= article_date <= self.now):
            print(f"Article with URL {article_url} is outside the desired date range.")
            return

        author_element = soup.select_one('div.kbwc-meta span.kbwcm-author')
        author = author_element.text.strip() if author_element else None
        if author and author.endswith(' ,'):
            author = author[:-2] 
        tag_list = [tag.text.strip() for tag in soup.select('div.kbw-submenu ul.kbws-list li.kbwsli.fr a')]

        if author and tag_list:
            title = soup.select_one('h1.kbwc-title').text if soup.select_one('h1.kbwc-title') else None
            summary = soup.select_one('h2.knc-sapo').text if soup.select_one('h2.knc-sapo') else None
            content = " ".join([p.text for p in soup.select('div.detail-content p')])
            source = soup.select_one('div.kbwc-meta span.kbwcm-source').text
            article_data = {
                'source': source,
                'url': article_url,
                'title': title.strip(),
                'summary': summary.strip(),
                'author': author,
                'time': article_date,
                'tagList': tag_list,
                'content': content
            }

            try:
                self.collection.insert_one(article_data)
                print(f"Article {title} inserted into MongoDB.")
            except errors.OperationFailure as e:
                print(f"Failed to insert article {title} into MongoDB: {e}")

    def convert_time_format(self, time_str):
        try:
            time_str = time_str.strip()
            dt = datetime.strptime(time_str, '%H:%M %d/%m/%Y')
            dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt).isoformat()
            return dt
        except ValueError as e:
            print(f"Error parsing time: {e}")
            return self.now.isoformat()


    def stop(self):
        self.stop_event.set()

    def get_status(self):
        return self.status
