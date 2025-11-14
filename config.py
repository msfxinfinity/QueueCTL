# config.py
from storage import Storage

_storage = Storage()

def get_config(key: str, default=None):
    return _storage.get_config(key, default)

def set_config(key: str, value: str):
    _storage.set_config(key, value)
