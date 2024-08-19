from flask import Flask, jsonify, request
from threading import Thread
from pymongo import MongoClient
from scraper_VNExpress import VNExpressCrawler
from scraper_Kenh14 import Kenh14Crawler
from text_process import DuplicateRemover

app = Flask(__name__)

mongo_client = MongoClient('mongodb+srv://phuongvv:kjnhkjnh@vht.w3g8gh9.mongodb.net/?retryWrites=true&w=majority&appName=VHT')
vnexpress_crawler = VNExpressCrawler(mongo_client)
kenh14_crawler = Kenh14Crawler(mongo_client)

vnexpress_thread = None
kenh14_thread = None

@app.route('/start_crawl', methods=['GET'])
def start_crawl():
    global vnexpress_thread, kenh14_thread

    if vnexpress_thread is None or not vnexpress_thread.is_alive():
        vnexpress_thread = Thread(target=vnexpress_crawler.crawl)
        vnexpress_thread.start()

    if kenh14_thread is None or not kenh14_thread.is_alive():
        kenh14_thread = Thread(target=kenh14_crawler.crawl)
        kenh14_thread.start()

    return jsonify({"message": "Crawlers started"}), 200

@app.route('/status', methods=['GET'])
def status():
    vnexpress_status = vnexpress_crawler.get_status()
    kenh14_status = kenh14_crawler.get_status()
    return jsonify({
        "VNExpress": vnexpress_status,
        "Kenh14": kenh14_status
    }), 200

@app.route('/stop_crawl', methods=['POST'])
def stop_crawl():
    vnexpress_crawler.stop()
    kenh14_crawler.stop()

    return jsonify({"message": "Crawlers stopped"}), 200

@app.route('/remove_duplicates', methods=['POST'])
def remove_duplicates():
    duplicate_remover = DuplicateRemover(mongo_client, 'vht', 'test_phuc')
    duplicates_removed = duplicate_remover.remove_duplicates()
    return jsonify({"message": f"{duplicates_removed} groups of duplicates removed."}), 200

if __name__ == '__main__':
    app.run(debug=True)
