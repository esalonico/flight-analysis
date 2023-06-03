# author: Emanuele Salonico, 2023
from pymongo import MongoClient
import pandas as pd

class Database:
    def __init__(self, db_url, db_name, collection_name):
        self.db_url = db_url
        self.db_name = db_name
        self.collection_name = collection_name
        
        self.client = self.connect_to_mongo()
        self.db = self.get_db()
        self.collection = self.get_collection()
        
        self.create_indexes()
        
    def __repr__(self):
        return f"Database [{self.db_name}], collection [{self.collection_name}]"
        
        
    def connect_to_mongo(self):
        """
        Connect to MongoDB Atlas and return a MongoClient object.
        """
        try:
            return MongoClient(self.db_url)
        except Exception as e:
            raise ConnectionError(e)
        
    def get_db(self):
        """
        Get a database from a MongoClient object.
        """
        # check if db exists
        if self.db_name not in self.client.list_database_names():
            raise ValueError(f"Database {self.db_name} not found.")
        
        return self.client[self.db_name]
    
    def get_collection(self):
        """
        Get a collection from a database.
        """
        # check if collection exists
        if self.collection_name not in self.db.list_collection_names():
            raise ValueError(f"Collection {self.collection_name} not found.")
        
        return self.db[self.collection_name]
    
    def add_pandas_df(self, df):
        """
        Adds a Pandas dataframe to the database.
        # TODO: check for duplicate rows
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a Pandas dataframe.")
        if df.empty:
            raise ValueError("Input dataframe is empty.")
        
        df_dict = df.to_dict(orient="records")
        self.collection.insert_many(df_dict)
        
        print(f"Added {len(df_dict)} rows to collection [{self.collection_name}]")
        
    def create_indexes(self):
        """
        Create indexes on the collection.
        # TODO: add more useful indexes
        """
        indexes = {"origin": "origin_index",
                   "destination": "destination_index"}
        
        for new_idx, new_idx_name in indexes.items():
            # if index does not exist, create it
            if not new_idx_name in self.collection.list_indexes():
                self.collection.create_index(new_idx, name=new_idx_name)
                print(f"Created index [{new_idx_name}] on collection [{self.collection_name}]")