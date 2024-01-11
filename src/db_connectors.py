from abc import ABC, abstractmethod

import sqlite3
from cachetools import cached, TTLCache

from settings import (
    DB_NAME,
    DB_TABLE_NAME,
    DB_MULTIPLIER_COLUMN,
    DB_NAME_COLUMN,
    CACHE_TIMEOUT,
    CACHE_MAXSIZE
)


__all__ = ['ISQLQueryManager', 'SQLQueryManager', 'IDbConnector', 'SQLiteConnector']


cache = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TIMEOUT)


class IDbConnector(ABC):
    
    @abstractmethod
    def fetchone(self, query):
        pass
    
    @abstractmethod
    def close_connection(self):
        pass


class SQLiteConnector(IDbConnector):

    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
    
    def fetchone(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def close_connection(self):
        self.connection.close()

    def __str__(self) -> str:
        return f"DBConnector: connected to ({self.db_name})"


class ISQLQueryManager(ABC):
        
    @abstractmethod
    def get_multiplier(self, instrument: str) -> float:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class SQLQueryManager(ISQLQueryManager):
    """
    Manages the queries to the database and caches the results.
    """

    def __init__(self, db: IDbConnector):
        self.db = db

    @cached(cache)
    def get_multiplier(self, instrument: str) -> float:
        query = 'SELECT {} FROM {} where {} = "{}"'.format(
            DB_MULTIPLIER_COLUMN,
            DB_TABLE_NAME,
            DB_NAME_COLUMN,
            instrument
        )
        result = self.db.fetchone(query)
        return result[0] if result else 1
    
    def close(self) -> None:
        self.db.close_connection()
