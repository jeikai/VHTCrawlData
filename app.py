from flask import Flask, jsonify, request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient
from scrapy.signalmanager import dispatcher
from scrapy import signals
from scraper_VNExpress import VNExpressSpider  # Replace 'scraper_VNExpress' with the name of your VNExpress spider file
from scraper_Kenh14 import Kenh14Spider  # Replace 'scraper_Kenh14' with the name of your Kenh14 spider file

app = Flask(__name__)

# Initialize MongoDB client
mongo_client = MongoClient('mongodb+srv://phuongvv:kjnhkjnh@vht.w3g8gh9.mongodb.net/?retryWrites=true&w=majority&appName=VHT')

# Global variables to hold the CrawlerProcess and status
crawler_process = None
spider_running = False
spiders_status = {
    'VNExpress': False,
    'Kenh14': False
}

def spider_closed(spider):
    global spider_running
    spiders_status[spider.name] = False
    if not any(spiders_status.values()):
        spider_running = False

dispatcher.connect(spider_closed, signal=signals.spider_closed)

@app.route('/crawl_VNExpress', methods=['GET'])
def start_vnexpress_spider():
    global spider_running, crawler_process
    if spider_running:
        return jsonify({"status": "error", "message": "Spider is already running"}), 400
    
    settings = get_project_settings()
    crawler_process = CrawlerProcess(settings)
    crawler_process.crawl(VNExpressSpider, mongo_client=mongo_client)
    crawler_process.start(False)
    spider_running = True
    spiders_status['VNExpress'] = True
    return jsonify({"status": "success", "message": "VNExpress spider started"}), 200

@app.route('/crawl_Kenh14', methods=['GET'])
def start_kenh14_spider():
    global spider_running, crawler_process
    if spider_running:
        return jsonify({"status": "error", "message": "Spider is already running"}), 400
    
    settings = get_project_settings()
    crawler_process = CrawlerProcess(settings)
    crawler_process.crawl(Kenh14Spider, mongo_client=mongo_client)
    crawler_process.start(False)
    spider_running = True
    spiders_status['Kenh14'] = True
    return jsonify({"status": "success", "message": "Kenh14 spider started"}), 200

@app.route('/crawl_both', methods=['GET'])  
def start_both_spiders():
    global spider_running, crawler_process
    if spider_running:
        return jsonify({"status": "error", "message": "Spiders are already running"}), 400

    settings = get_project_settings()
    crawler_process = CrawlerProcess(settings)
    crawler_process.crawl(VNExpressSpider, mongo_client=mongo_client)
    crawler_process.crawl(Kenh14Spider, mongo_client=mongo_client)
    crawler_process.start(False)
    spider_running = True
    spiders_status['VNExpress'] = True
    spiders_status['Kenh14'] = True
    return jsonify({"status": "success", "message": "Both spiders started"}), 200

@app.route('/crawl_status', methods=['GET'])
def get_crawl_status():
    return jsonify({"spiders_status": spiders_status, "spider_running": spider_running}), 200

@app.route('/stop_crawl', methods=['GET'])
def stop_crawl():
    global crawler_process, spider_running
    if not spider_running:
        return jsonify({"status": "error", "message": "No spiders are running"}), 400
    
    if crawler_process:
        crawler_process.stop()
    spider_running = False
    spiders_status['VNExpress'] = False
    spiders_status['Kenh14'] = False
    return jsonify({"status": "success", "message": "All spiders stopped"}), 200

if __name__ == '__main__':
    app.run(debug=True)
