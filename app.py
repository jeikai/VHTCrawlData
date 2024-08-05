from flask import Flask, jsonify, request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from pymongo import MongoClient
from scraper_VNExpress import VNExpressSpider  # Replace 'scraper_VNExpress' with the name of your spider file

app = Flask(__name__)

# Initialize MongoDB client
mongo_client = MongoClient('mongodb+srv://phuongvv:kjnhkjnh@vht.w3g8gh9.mongodb.net/?retryWrites=true&w=majority&appName=VHT')

# Global variable to hold the CrawlerProcess
crawler_process = None
spider_running = False

@app.route('/crawl_multiple_articles_VNExpress', methods=['POST'])
def start_spider():
    global spider_running
    if spider_running:
        return jsonify({"status": "error", "message": "Spider is already running"}), 400
    
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(VNExpressSpider, mongo_client=mongo_client)
    process.start(False) 
    spider_running = True
    return jsonify({"status": "success", "message": "Spider started"}), 200

if __name__ == '__main__':
    app.run(debug=True)
