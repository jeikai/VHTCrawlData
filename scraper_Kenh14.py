import scrapy
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient

class Kenh14Spider(scrapy.Spider):
    name = 'vnexpress-spider'
    start_urls = ['https://kenh14.vn/']

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
        self.collection = self.db['test_phuc']
        self.now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        self.two_weeks_ago = self.now - timedelta(weeks=2)
        self.three_weeks_ago = self.now - timedelta(weeks=3)
        self.page_count = 0

    def parse(self, response):
        CATEGORY_SELECTOR = 'khw-bottom-header ul.kbh-menu-list li a::attr(href)'
        for category_url in response.css(CATEGORY_SELECTOR).extract():
            yield response.follow(category_url, self.parse_category)

    def parse_category(self, response):
        TOP_NEWS_SELECTOR = 'div.klw-top-news h3.knswli-title > a::attr(href)'
        SLIDE_WRAPPER_SELECTOR = 'div.klwfn-slide-wrapper h3.knswli-title > a::attr(href)'

        top_news_urls = response.css(TOP_NEWS_SELECTOR).extract()
        slide_wrapper_urls = response.css(SLIDE_WRAPPER_SELECTOR).extract()

        for article_url in top_news_urls + slide_wrapper_urls:
            yield response.follow(article_url, self.parse_article)

    def parse_article(self, response):
        TIME_SELECTOR = 'span.kbwcm-time::text'
        TITLE_SELECTOR = 'h1.kbwc-title::text'
        SUMMARY_SELECTOR = 'h2.knc-sapo::text'
        AUTHOR_SELECTOR = 'p[style*="text-align:right"] strong::text'
        DETAIL_SELECTOR = 'div.knc-content p::text'
        CATEGORY_SELECTOR = 'div.kbwc-breadcrumb a::text'
        QUIZ_SELECTOR = 'div.quiz-container'

        time_post = response.css(TIME_SELECTOR).extract_first()
        if time_post:
            time_post = self.convert_time_format(time_post)
            article_date = datetime.fromisoformat(time_post)
        else:
            article_date = self.now
        
        if not (self.three_weeks_ago <= article_date <= self.now):
            self.log(f"Article with URL {response.url} is outside the desired date range.")
            return
        
        if len(response.css(QUIZ_SELECTOR).extract()) > 0:
            self.log(f"Article with URL {response.url} contains a quiz and will not be inserted into MongoDB.")
            return
        
        author = response.css(AUTHOR_SELECTOR).extract_first()
        tag_list = response.css(CATEGORY_SELECTOR).extract()

        if author and tag_list:
            title = response.css(TITLE_SELECTOR).extract_first()
            content = " ".join(response.css(DETAIL_SELECTOR).extract())

            article_data = {
                'source': 'Báo Kenh14',
                'url': response.url,
                'title': title,
                'summary': response.css(SUMMARY_SELECTOR).extract_first(),
                'author': author,
                'time': article_date,  # Insert datetime object
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
            dt = datetime.strptime(time_str, '%H:%M %d/%m/%Y')
            dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt).isoformat()
            return dt
        except ValueError as e:
            self.log(f"Error parsing time: {e}")
            return self.now.isoformat()

    def closed(self, reason):
        self.client.close()
