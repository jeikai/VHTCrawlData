from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, errors
from urllib.parse import urljoin

class VNExpressCrawler:
    def __init__(self, mongo_client):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")

        # Path to your ChromeDriver
        self.driver = webdriver.Chrome(service=Service('D:/Download/chromedriver-win64/chromedriver-win64/chromedriver.exe'), options=chrome_options)
        
        self.client = mongo_client
        self.db = self.client['vht']
        self.collection = self.db['test_phuc']
        self.now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        self.two_weeks_ago = self.now - timedelta(weeks=2)
        self.three_weeks_ago = self.now - timedelta(weeks=3)
        self.page_count = 0

    def crawl(self):
        try:
            self.driver.get('https://vnexpress.net/')
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            category_links = soup.select('nav.main-nav ul.parent li a')
            for category_link in category_links:
                category_url = urljoin('https://vnexpress.net', category_link['href'])
                self.crawl_category(category_url)
        finally:
            self.driver.quit()
            self.client.close()

    def crawl_category(self, category_url):
        if self.page_count >= 5:
            return

        self.driver.get(category_url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        article_links = soup.select('article .title-news a')
        for article_link in article_links:
            article_url = urljoin('https://vnexpress.net', article_link['href'])
            self.crawl_article(article_url)

        next_page_link = soup.select_one('a.next-page')
        if next_page_link:
            self.page_count += 1
            next_page_url = urljoin('https://vnexpress.net', next_page_link['href'])
            self.crawl_category(next_page_url)

    def crawl_article(self, article_url):
        self.driver.get(article_url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

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

        if author and tag_list:
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
            except errors.OperationFailure as e:
                print(f"Failed to insert article {title} into MongoDB: {e}")

    def convert_time_format(self, time_str):
        try:
            time_str = time_str.split(', ', 1)[1].rsplit(' ', 1)[0]
            dt = datetime.strptime(time_str, '%d/%m/%Y, %H:%M')
            dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt).isoformat()
            return dt
        except ValueError as e:
            print(f"Error parsing time: {e}")
            return self.now.isoformat()

# Usage example:
mongo_client = MongoClient('mongodb+srv://phuongvv:kjnhkjnh@vht.w3g8gh9.mongodb.net/?retryWrites=true&w=majority&appName=VHT')
crawler = VNExpressCrawler(mongo_client)
crawler.crawl()
