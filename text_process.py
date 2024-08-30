from pymongo import MongoClient
import re
import logging
from collections import deque

logging.basicConfig(filename='preprocessing.log', level=logging.INFO, format='%(asctime)s - %(message)s')

class DataPreprocessor:
    def __init__(self, mongo_client, db_name, collection_name):
        self.client = mongo_client
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def preprocess(self):
        all_records = list(self.collection.find())

        seen = set()
        duplicates = []
        queue = deque(all_records)  # Initialize the queue with all records

        # BFS to find duplicates
        while queue:
            record = queue.popleft()

            original_title = record.get('title', '')
            original_summary = record.get('summary', '')
            original_content = record.get('content', '')

            cleaned_title = self.clean_text(original_title)
            cleaned_summary = self.clean_text(original_summary)
            cleaned_content = self.clean_text(original_content)

            unique_key = (cleaned_title, cleaned_summary, record['author'], record['time'])

            if unique_key in seen:
                duplicates.append(record['_id'])
                logging.info(f'Duplicate record found and marked for deletion: {record["_id"]}')
            else:
                seen.add(unique_key)

        # Delete duplicates
        if duplicates:
            self.collection.delete_many({"_id": {"$in": duplicates}})
            logging.info(f'Records deleted: {duplicates}')

        # Clean remaining records
        remaining_records = self.collection.find()
        for record in remaining_records:
            original_title = record.get('title', '')
            original_summary = record.get('summary', '')
            original_content = record.get('content', '')

            cleaned_title = self.clean_text(original_title)
            cleaned_summary = self.clean_text(original_summary)
            cleaned_content = self.clean_text(original_content)

            if (original_title != cleaned_title or
                original_summary != cleaned_summary or
                original_content != cleaned_content):

                self.collection.update_one(
                    {'_id': record['_id']},
                    {'$set': {
                        'title': cleaned_title,
                        'summary': cleaned_summary,
                        'content': cleaned_content
                    }}
                )
                logging.info(f'Record updated: {record["_id"]}')

    def clean_text(self, text):
        if text is None:
            return ''  # Return an empty string if the text is None

        if not isinstance(text, str):
            text = str(text)  # Convert to string if not already a string

        text = re.sub(r'\n', ' ', text)
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return text
