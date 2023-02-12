import pandas as pd
from insurance.exception import InsuranceException
from insurance.logger import logging
import os, sys
from insurance.config import mongo_client

def get_collection_as_dataframe(database_name:str, collection_name:str)->pd.DataFrame:
    """
    Description: This function return collection as dataframe
    =========================================================
    Params:
    database_name: database name
    collection_name: collection name
    =========================================================
    return Pandas dataframe of a collection
    """
    try:
        logging.info(f"Reading data from Database: {database_name} and Collection: {collection_name}")
        df = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f"Available columns in the dataframe: {df.columns}")
        if "_id" in df.columns:
            logging.info(f"Dropping _id column from the dataframe")
            df = df.drop("_id", axis=1)
        logging.info(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
        return df

    except Exception as e:
        raise InsuranceException(e, sys)
    