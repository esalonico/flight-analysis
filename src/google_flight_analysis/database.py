# author: Emanuele Salonico, 2023

import psycopg2
import pandas as pd
import numpy as np
import ast
import psycopg2.extras as extras



class Database:
    def __init__(self, db_host, db_name, db_user, db_pw, db_table):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_table = db_table
        self.__db_pw = db_pw

        self.conn = self.connect_to_postgresql()
        self.conn.autocommit = True

    def __repr__(self):
        return f"Database: {self.db_name}"

    def connect_to_postgresql(self):
        """
        Connect to Postgresql and return a connection object.
        """
        try:
            conn = psycopg2.connect(host=self.db_host,
                                    database=self.db_name,
                                    user=self.db_user,
                                    password=self.__db_pw)
            return conn
        except Exception as e:
            raise ConnectionError(e)

    def list_all_databases(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT datname FROM pg_database WHERE datistemplate = false;")
        result = cursor.fetchall()
        cursor.close()

        return [x[0] for x in result]

    def list_all_tables(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM information_schema.tables WHERE table_schema = 'public';")
        result = cursor.fetchall()
        cursor.close()

        return [x[2] for x in result]

    def create_db(self):
        """
        Creates a new database for flight_analysis data.
        """
        cursor = self.conn.cursor()
        query = """CREATE DATABASE flight_analysis WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1 IS_TEMPLATE = False;"""
        cursor.execute(query)
        cursor.close()

        print("Database [flight_analysis] created.")

    def create_scraped_table(self, overwrite):
        query = ""
        if overwrite:
            query += "DROP TABLE IF EXISTS public.scraped;\n"

        query += """
            CREATE TABLE IF NOT EXISTS public.scraped
            (
                id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                departure_datetime timestamp with time zone,
                arrival_datetime timestamp with time zone,
                airlines text[] COLLATE pg_catalog."default",
                travel_time smallint NOT NULL,
                origin character(3) COLLATE pg_catalog."default"  NOT NULL,
                destination character(3) COLLATE pg_catalog."default"  NOT NULL,
                layover_n smallint NOT NULL,
                layover_time numeric,
                layover_location text COLLATE pg_catalog."default",
                price_eur smallint NOT NULL,
                price_trend text COLLATE pg_catalog."default",
                price_value text COLLATE pg_catalog."default",
                access_date timestamp with time zone NOT NULL,
                one_way boolean NOT NULL,
                has_train boolean NOT NULL,
                days_advance smallint NOT NULL
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.scraped OWNER to postgres;
            """
            
        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

    def prepare_db_and_tables(self, overwrite_table=False):
        # create table
        if self.db_name not in self.list_all_databases():
            self.create_db()
        else:
            print(f"Database [{self.db_name}] already exists.")

        # create table
        self.create_scraped_table(overwrite_table)
        
    def transform_and_clean_df(self, df):
        """
        Some necessary cleaning and transforming operations to the df
        before sending its content to the database
        """

        df["airlines"] = df.airlines.apply(lambda x: np.array(ast.literal_eval(str(x).replace("[", '"{').replace("]", '}"'))))
        df['layover_time'] = df['layover_time'].fillna(-1)
        df["layover_location"] = df["layover_location"].fillna(np.nan).replace([np.nan], [None])
        df["price_value"] = df["price_value"].fillna(np.nan).replace([np.nan], [None])

        return df
        
    def add_pandas_df_to_db(self, df):
        # clean df
        df = self.transform_and_clean_df(df)
        
        # Create a list of tuples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
    
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
    
        cursor = self.conn.cursor()
    
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES %%s" % (self.db_table, cols)
        try:
            extras.execute_values(cursor, query, tuples)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
        
        print("execute_values() done")
        cursor.close()
        
        # fix layover time
        # TODO: improve this
        cursor = self.conn.cursor()
        query = f"""
            UPDATE {self.db_table}
            SET layover_time = CASE
            WHEN layover_time = -1 THEN null ELSE layover_time END;

            ALTER TABLE public.scraped 
            ALTER COLUMN layover_time TYPE smallint;
        """
        cursor.execute(query)
        cursor.close()