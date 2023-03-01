from dramatiq_abort import Event
from dramatiq_abort.backends import MongoDBBackend


def test_mongodb_collection(mongodb_event_backend: MongoDBBackend) -> None:
    event = Event(key="test_key", params=dict())
    mongodb_event_backend.notify([event], 1000)
    assert mongodb_event_backend.collection.find_one({"key": "test_key", "acknowledged": False})["value"] == {}
    assert mongodb_event_backend.wait_many([event.key], timeout=1000) == event


def test_mongodb_nonexist_key(mongodb_event_backend: MongoDBBackend):
    assert mongodb_event_backend.poll("non-exist") == None
    assert mongodb_event_backend.wait_many(["non", "exist", "key"], 2000) == None
