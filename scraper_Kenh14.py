import scrapy
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient, errors

class Kenh14Spider(scrapy.Spider):
    name = 'kenh14-spider'
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

    def __init__(self, mongo_client, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo_client = mongo_client
        self.client = mongo_client
        self.db = self.client['vht']
        self.collection = self.db['test_phuc']
        self.client_closed = False
        self.now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        self.two_weeks_ago = self.now - timedelta(weeks=2)
        self.three_weeks_ago = self.now - timedelta(weeks=3)

    def parse(self, response):
        if self.client_closed:
            self.crawler.engine.close_spider(self, "MongoDB client is closed.")
            return
        CATEGORY_SELECTOR = '.khw-bottom-header ul.kbh-menu-list li.kmli > a::attr(href)'
        for category_url in response.css(CATEGORY_SELECTOR).extract():
            if category_url != 'javascript:;' and category_url != 'http://video.kenh14.vn/' and category_url != "/":
                self.log(f"Following category URL: {category_url}")
                yield response.follow(category_url, self.parse_category)

    def parse_category(self, response):
        if self.client_closed:
            self.crawler.engine.close_spider(self, "MongoDB client is closed.")
            return
        TOP_NEWS_SELECTOR = 'div.klw-top-news ul.ktnc-list li.ktncli > a::attr(href)'
        SLIDE_WRAPPER_SELECTOR = 'div.klwfn-slide-wrapper ul.knswli-object-list li.klwfnswn > a::attr(href)'

        top_news_urls = response.css(TOP_NEWS_SELECTOR).extract()
        slide_wrapper_urls = response.css(SLIDE_WRAPPER_SELECTOR).extract()

        for article_url in top_news_urls + slide_wrapper_urls:
            yield response.follow(article_url, self.parse_article)

    def parse_article(self, response):
        if self.client_closed:
            self.crawler.engine.close_spider(self, "MongoDB client is closed.")
            return
        TIME_SELECTOR = 'span.kbwcm-time::text'
        TITLE_SELECTOR = 'h1.kbwc-title::text'
        SUMMARY_SELECTOR = 'h2.knc-sapo *::text'
        AUTHOR_SELECTOR = 'div.kbwc-meta span.kbwcm-author::text'
        DETAIL_SELECTOR = 'div.detail-content p::text'
        CATEGORY_SELECTOR = 'div.kbw-submenu ul.kbws-list li.kbwsli.fr a::text'
        SOURCE_SELECTOR = 'div.kbwc-meta span.kbwcm-source::text'

        time_post = response.css(TIME_SELECTOR).extract_first()
        if time_post:
            time_post = self.convert_time_format(time_post)
            article_date = datetime.fromisoformat(time_post)
        else:
            article_date = self.now

        if not (self.three_weeks_ago <= article_date <= self.now):
            self.log(f"Article with URL {response.url} is outside the desired date range.")
            return

        author = response.css(AUTHOR_SELECTOR).extract_first()
        tag_list = response.css(CATEGORY_SELECTOR).extract()
        summary = " ".join(response.css(SUMMARY_SELECTOR).extract()).strip()
        content = " ".join(response.css(DETAIL_SELECTOR).extract())
        title = response.css(TITLE_SELECTOR).extract_first()
        source = response.css(SOURCE_SELECTOR).extract_first()
        if source and source.startswith("Theo "):
            source = source[5:].strip()

        if author and tag_list and summary and content and title:
            article_data = {
                'source': source,
                'url': response.url,
                'title': title.strip(),
                'summary': summary,
                'author': author,
                'time': article_date,
                'tagList': tag_list,
                'content': content
            }

            try:
                self.collection.insert_one(article_data)
                self.log(f"Article {title} inserted into MongoDB.")
            except errors.OperationFailure as e:
                self.log(f"Failed to insert article {title} into MongoDB: {e}")
                if "Cannot use MongoClient after close" in str(e):
                    self.client_closed = True
                    self.crawler.engine.close_spider(self, "MongoDB client is closed.")
            except Exception as e:
                self.log(f"Failed to insert article {title} into MongoDB: {e}")

    def convert_time_format(self, time_str):
        try:
            time_str = time_str.strip()
            dt = datetime.strptime(time_str, '%H:%M %d/%m/%Y')
            dt = pytz.timezone('Asia/Ho_Chi_Minh').localize(dt).isoformat()
            return dt
        except ValueError as e:
            self.log(f"Error parsing time: {e}")
            return self.now.isoformat()

