import logging
from flask import Flask, jsonify, request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient
from scrapy.signalmanager import dispatcher
from scrapy import signals
import threading
from scraper_VNExpress import VNExpressSpider  
from scraper_Kenh14 import Kenh14Spider 

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mongo_client = MongoClient('mongodb+srv://phuongvv:kjnhkjnh@vht.w3g8gh9.mongodb.net/?retryWrites=true&w=majority&appName=VHT')

spider_running = False
spiders_status = {
    'VNExpress': False,
    'Kenh14': False
}
spiders_count = 0
crawler_process = None

def spider_closed(spider):
    global spider_running, spiders_count
    spiders_status[spider.name] = False
    spiders_count -= 1
    if spiders_count == 0:
        spider_running = False
        mongo_client.close() 

dispatcher.connect(spider_closed, signal=signals.spider_closed)

def get_crawler_process():
    settings = get_project_settings()
    return CrawlerProcess(settings)

@app.route('/crawl_VNExpress', methods=['GET'])
def start_vnexpress_spider():
    global spider_running, crawler_process
    try:
        if spider_running:
            return jsonify({"status": "error", "message": "Spider is already running"}), 400
        
        spider_running = True
        spiders_status['VNExpress'] = True
        
        crawler_process = get_crawler_process()
        threading.Thread(target=crawl, args=(crawler_process, VNExpressSpider,)).start()
        
        return jsonify({"status": "success", "message": "VNExpress spider started"}), 200
    except Exception as e:
        logger.error(f"Error starting VNExpress spider: {e}")
        spider_running = False
        spiders_status['VNExpress'] = False
        return jsonify({"status": "error", "message": "Failed to start VNExpress spider"}), 500

@app.route('/crawl_Kenh14', methods=['GET'])
def start_kenh14_spider():
    global spider_running, crawler_process
    try:
        if spider_running:
            return jsonify({"status": "error", "message": "Spider is already running"}), 400
        
        spider_running = True
        spiders_status['Kenh14'] = True
        
        crawler_process = get_crawler_process()
        threading.Thread(target=crawl, args=(crawler_process, Kenh14Spider,)).start()
        
        return jsonify({"status": "success", "message": "Kenh14 spider started"}), 200
    except Exception as e:
        logger.error(f"Error starting Kenh14 spider: {e}")
        spider_running = False
        spiders_status['Kenh14'] = False
        return jsonify({"status": "error", "message": "Failed to start Kenh14 spider"}), 500

@app.route('/crawl_both', methods=['GET'])
def start_both_spiders():
    global spider_running, spiders_count, crawler_process
    try:
        if spider_running:
            return jsonify({"status": "error", "message": "Spiders are already running"}), 400
        
        spider_running = True
        spiders_status['VNExpress'] = True
        spiders_status['Kenh14'] = True
        spiders_count = 2
        
        crawler_process = get_crawler_process()
        threading.Thread(target=crawl_both_spiders, args=(crawler_process,)).start()
        
        return jsonify({"status": "success", "message": "Both spiders started"}), 200
    except Exception as e:
        logger.error(f"Error starting both spiders: {e}") 
        spider_running = False
        spiders_status['VNExpress'] = False
        spiders_status['Kenh14'] = False
        return jsonify({"status": "error", "message": "Failed to start both spiders"}), 500

def crawl(crawler_process, spider_cls):
    try:
        crawler_process.crawl(spider_cls, mongo_client=mongo_client)
        crawler_process.start()
    except Exception as e:
        logger.error(f"Error during crawling with {spider_cls.__name__}: {e}")

def crawl_both_spiders(crawler_process):
    try:
        crawler_process.crawl(VNExpressSpider, mongo_client=mongo_client)
        crawler_process.crawl(Kenh14Spider, mongo_client=mongo_client)
        crawler_process.start()
    except Exception as e:
        logger.error(f"Error during crawling both spiders: {e}")
    finally:
        global spider_running
        spider_running = False
        spiders_status['VNExpress'] = False
        spiders_status['Kenh14'] = False 

@app.route('/crawl_status', methods=['GET'])
def get_crawl_status():
    try:
        return jsonify({"spiders_status": spiders_status, "spider_running": spider_running}), 200
    except Exception as e:
        logger.error(f"Error getting crawl status: {e}")
        return jsonify({"status": "error", "message": "Failed to get crawl status"}), 500

@app.route('/stop_crawl', methods=['GET'])
def stop_crawl():
    global spider_running, crawler_process
    try:
        if not spider_running:
            return jsonify({"status": "error", "message": "No spiders are running"}), 400
        
        if crawler_process:
            crawler_process.stop()
            spider_running = False
            spiders_status['VNExpress'] = False
            spiders_status['Kenh14'] = False
            crawler_process = None
            return jsonify({"status": "success", "message": "All spiders stopped"}), 200
        else:
            return jsonify({"status": "error", "message": "Crawler process not found"}), 400
    except Exception as e:
        logger.error(f"Error stopping crawl: {e}")
        return jsonify({"status": "error", "message": "Failed to stop crawl"}), 500

if __name__ == '__main__':
    app.run(debug=True)
