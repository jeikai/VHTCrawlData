from pymongo import MongoClient

class DuplicateRemover:
    def __init__(self, mongo_client, db_name, collection_name):
        self.client = mongo_client
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def remove_duplicates(self):
        pipeline = [
            {"$group": {
                "_id": {
                    "source": "$source",
                    "url": "$url",
                    "title": "$title",
                    "summary": "$summary",
                    "author": "$author",
                    "time": "$time",
                    "tagList": "$tagList",
                    "content": "$content"
                },
                "unique_ids": {"$addToSet": "$_id"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]

        duplicates = list(self.collection.aggregate(pipeline))

        # Xóa tất cả các bản ghi trùng lặp, giữ lại duy nhất một bản ghi
        for duplicate in duplicates:
            ids_to_delete = duplicate["unique_ids"][1:]  # Giữ lại bản ghi đầu tiên
            self.collection.delete_many({"_id": {"$in": ids_to_delete}})

        return len(duplicates)  # Trả về số lượng nhóm bản ghi trùng lặp đã xử lý
