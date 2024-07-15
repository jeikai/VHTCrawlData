import scrapy

# class inherite from Scrapy class to define how a website will be scraped
class QuoteSpider(scrapy.Spider):
    # name for the spider
    name = 'quote-spider'
    # URL the scraper will begin 
    start_urls = ['https://quotes.toscrape.com']
    # define how the data is extracted
    def parse(self, response):
        # extract data by looking for class name in CSS
        # For example: class name quote
        QUOTE_SELECTOR = '.quote'
        # ::text means that find text within elements class text
        TEXT_SELECTOR = '.text::text'
        
        AUTHOR_SELECTOR = '.author::text'
        
        # + means that selecting the next sibling element next to class author
        # ::attr('href') means that extract the value of the href attribute
        ABOUT_SELECTOR = '.author + a::attr("href")'
        
        # > means that selects direct child elements
        TAGS_SELECTOR = '.tags > .tag::text'
        
        NEXT_SELECTOR = '.next a::attr("href")'
        
        # Loop through each quote
        for quote in response.css(QUOTE_SELECTOR):
            yield {
                # Extract first instance of text found by SELECTOR
                'text': quote.css(TEXT_SELECTOR).extract_first(),
                'author': quote.css(AUTHOR_SELECTOR).extract_first(),
                'about': 'https://quotes.toscrape.com' + 
                        quote.css(ABOUT_SELECTOR).extract_first(),
                'tags': quote.css(TAGS_SELECTOR).extract(),
            }
        
        next_page = response.css(NEXT_SELECTOR).extract_first()
        if next_page:
            # scrapy.Request means that it should fetch and parse next
            yield scrapy.Request(response.urljoin(next_page))
