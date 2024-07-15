import scrapy

# class inherite from Scrapy class to define how a website will be scraped
class VNExpressSpider(scrapy.Spider):
    # name for the spider
    name = 'vnexpress-spider'
    # URL the scraper will begin 
    start_urls = ['https://vnexpress.net/']

    def parse(self, response):
        CATEGORY_SELECTOR = '' 
        
    
    def parse_detail(self, response):
        
        SECTION_SELECTOR = 'section.section.page-detail.top-detail div.container div.sidebar-1'
        TIME_SELECTOR = 'div.header-content span.date::text'
        TITLE_SELECTOR = 'h1.title-detail::text'
        DESCRIPTION_SELECTOR = 'p.description::text'
        DETAIL_SELECTOR = 'article.fck_detail p.Normal::text'
        # Loop through each quote
        for container in response.css(SECTION_SELECTOR):
            yield {
                # Extract first instance of text found by SELECTOR
                'time_post': container.css(TIME_SELECTOR).extract_first(),
                'title': container.css(TITLE_SELECTOR).extract_first(),
                'description': container.css(DESCRIPTION_SELECTOR).extract_first(),
                'detail': container.css(DETAIL_SELECTOR).extract()
            }

