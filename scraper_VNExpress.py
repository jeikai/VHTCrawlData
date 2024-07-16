import scrapy
import hashlib
from datetime import datetime
import pytz
from pymongo import MongoClient

class VNExpressSpider(scrapy.Spider):
    name = 'vnexpress-spider'
    start_urls = ['https://vnexpress.net/']

    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
        },
        'RETRY_TIMES': 10,
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504, 522, 524, 408],
        'FAKEUSERAGENT_PROVIDERS': [
            'scrapy_fake_useragent.providers.FakeUserAgentProvider',
            'scrapy_fake_useragent.providers.FakerProvider',
            'scrapy_fake_useragent.providers.FixedUserAgentProvider',
        ],
    }

    def __init__(self):
        super().__init__()
        self.client = MongoClient('mongodb+srv://phuongvv:kjnhkjnh@vht.w3g8gh9.mongodb.net/?retryWrites=true&w=majority&appName=VHT')
        self.db = self.client['vht']
        self.collection = self.db['test']

    def parse(self, response):
        CATEGORY_SELECTOR = 'nav.main-nav ul.parent li a::attr(href)'
        for category_url in response.css(CATEGORY_SELECTOR).extract():
            yield response.follow(category_url, self.parse_category)

    def parse_category(self, response):
        ARTICLE_SELECTOR = 'article .title-news a::attr(href)'

        for article_url in response.css(ARTICLE_SELECTOR).extract():
            yield response.follow(article_url, self.parse_article)

        NEXT_PAGE_SELECTOR = 'a.next-page::attr(href)'
        next_page = response.css(NEXT_PAGE_SELECTOR).extract_first()
        if next_page:
            yield response.follow(next_page, self.parse_category)

    def parse_article(self, response):
        TIME_SELECTOR = 'div.header-content span.date::text'
        TITLE_SELECTOR = 'h1.title-detail::text'
        SUMMARY_SELECTOR = 'p.description::text'
        AUTHOR_SELECTOR = 'p.Normal[style="text-align:right;"] strong::text'
        DETAIL_SELECTOR = 'article.fck_detail p.Normal::text'
        CATEGORY_SELECTOR = 'nav.main-nav ul.parent li.active a::text'
        QUIZ_SELECTOR = 'article.fck_detail div.item_quiz .tittle_quiz::text'
        time_post = response.css(TIME_SELECTOR).extract_first()
        if time_post:
            time_post = self.convert_time_format(time_post)
        else:
            time_post = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).isoformat()
        
        if len(response.css(QUIZ_SELECTOR).extract()) > 0:
            self.log(f"Article with URL {response.url} contains a quiz and will not be inserted into MongoDB.")
            return
        
        author = response.css(AUTHOR_SELECTOR).extract_first()
        tag_list = response.css(CATEGORY_SELECTOR).extract()

        if author and tag_list:
            title = response.css(TITLE_SELECTOR).extract_first()
            content = " ".join(response.css(DETAIL_SELECTOR).extract())



            id = hashlib.md5((title + time_post).encode()).hexdigest()

            article_data = {
                'id': id,
                'source': 'Báo VnExpress',
                'url': response.url,
                'title': title,
                'summary': response.css(SUMMARY_SELECTOR).extract_first(),
                'author': author,
                'time': time_post,
                'tagList': tag_list,
                'content': content
            }

            try:
                self.collection.insert_one(article_data)
                self.log(f"Article {title} inserted into MongoDB.")
            except Exception as e:
                self.log(f"Failed to insert article {title} into MongoDB: {e}")

    def convert_time_format(self, time_str):
        try:
            time_str = time_str.split(', ', 1)[1].rsplit(' ', 1)[0]
            dt = datetime.strptime(time_str, '%d/%m/%Y, %H:%M')
            dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt).isoformat()
            return dt
        except ValueError as e:
            self.log(f"Error parsing time: {e}")
            return datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).isoformat()

    def closed(self, reason):
        self.client.close()
