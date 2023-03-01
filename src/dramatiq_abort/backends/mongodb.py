from datetime import datetime, timedelta
import time
from typing import Iterable, Optional

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from ..backend import Event, EventBackend


class MongoDBBackend(EventBackend):
    """An event backend for MongoDB_.

    :param database: The `Database`_ instance in pymongo.
    :param collection_name: Collection to prefix keys with.
    """

    def __init__(self, *, collection: Collection) -> None:        
        self.collection = collection
        self.collection.create_index([("key", pymongo.HASHED)])
        self.collection.create_index("remove_at", expireAfterSeconds=0)

    @classmethod
    def create_backend(cls, host: str, port: int, db_name: str, collection_name: str):
        client = MongoClient(host=host, port=port)

        db = client[db_name]
        collection = db[collection_name]

        return cls(collection=collection)

    def wait_many(self, keys: Iterable[str], timeout: int) -> Optional[Event]:
        assert timeout is None or timeout >= 1000, "wait timeouts must be >= 1000"

        start_time = time.time()

        while True:
            result = self.collection.find_one_and_update(
                {"key": {"$in": keys}, "acknowledged": False},
                {"$set": {"acknowledged": True}}
            )

            if result:
                return Event(result["key"], result["value"])

            if time.time() > start_time + timeout / 1000:
                break
            
            time.sleep(1)

        return None

    def poll(self, key: str) -> Optional[Event]:
        entry = self.collection.find_one_and_update({
            "key": key, "acknowledged": False,
        }, {
            "$set": {
                "acknowledged": True,
            }
        })
        if entry is None:
            return None
        value = entry["value"]
        return Event(key, value)

    def notify(self, events: Iterable[Event], ttl: int) -> None:
        for event in events:
            self.collection.insert_one(
                {
                    "key": event.key,
                    "value": event.params,
                    "acknowledged": False,
                    "remove_at": datetime.now() + timedelta(milliseconds=ttl), 
                }
            )
