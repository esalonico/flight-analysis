# author: Emanuele Salonico, 2023

import psycopg2
import psycopg2.extras as extras
import os
import logging
from datetime import datetime
import subprocess

# logging
logger_name = os.path.basename(__file__)
logger = logging.getLogger(logger_name)


class Database:
    def __init__(self, db_host, db_name, db_user, db_pw):
        # connection
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_port = 5432
        self.__db_pw = db_pw

        # tables
        self.table_scraped = "scraped"
        self.table_scraped_airlines = "scraped_airlines"
        self.table_scraped_layovers = "scraped_layovers"

        self.conn = self.connect_to_postgresql()
        self.conn.autocommit = True

        # backups
        self.backup_folder_name = "db_backups"
        self.backup_folder = os.path.join(
            os.path.abspath(os.curdir), self.backup_folder_name
        )
        self.n_backups_to_keep = 2  # TODO: change to 5

    def __repr__(self):
        return f"Database: {self.db_name}"

    def connect_to_postgresql(self):
        """
        Connect to Postgresql and return a connection object.
        """
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.__db_pw,
            )
            return conn
        except Exception as e:
            raise ConnectionError(e)

    def list_all_databases(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        result = cursor.fetchall()
        cursor.close()

        return [x[0] for x in result]

    def list_all_tables(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM information_schema.tables WHERE table_schema = 'public';"
        )
        result = cursor.fetchall()
        cursor.close()

        return [x[2] for x in result]

    def create_flight_analysis_db(self):
        """
        Creates a new database for flight_analysis data.
        """
        cursor = self.conn.cursor()
        query = """CREATE DATABASE flight_analysis WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1 IS_TEMPLATE = False;"""
        cursor.execute(query)
        cursor.close()

        logger.info("Database [flight_analysis] created.")

    def create_scraped_table(self):
        query = ""
        query += f"""
            CREATE TABLE IF NOT EXISTS public.{self.table_scraped}
            (
                uuid uuid NOT NULL,
                departure_datetime timestamp with time zone,
                arrival_datetime timestamp with time zone,
                travel_time smallint NOT NULL,
                origin character(3) COLLATE pg_catalog."default" NOT NULL,
                destination character(3) COLLATE pg_catalog."default" NOT NULL,
                layover_n smallint NOT NULL,
                layover_time smallint,
                layover_location text COLLATE pg_catalog."default",
                price_eur smallint NOT NULL,
                price_trend text COLLATE pg_catalog."default",
                price_value text COLLATE pg_catalog."default",
                access_date timestamp with time zone NOT NULL,
                one_way boolean NOT NULL,
                has_train boolean NOT NULL,
                days_advance smallint NOT NULL,
                CONSTRAINT scraped_pkey PRIMARY KEY (uuid)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.{self.table_scraped} OWNER to postgres;
            """

        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

    def create_scraped_airlines_table(self):
        query = ""
        query += f"""
            CREATE TABLE IF NOT EXISTS public.{self.table_scraped_airlines}
            (
                uuid uuid NOT NULL DEFAULT gen_random_uuid(),
                flight_uuid uuid NOT NULL,
                airline text COLLATE pg_catalog."default",
                CONSTRAINT scraped_airlines_pkey PRIMARY KEY (uuid),
                CONSTRAINT flight_uuid FOREIGN KEY (flight_uuid)
                REFERENCES public.{self.table_scraped} (uuid) MATCH SIMPLE
                ON UPDATE CASCADE
                ON DELETE CASCADE
)

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.{self.table_scraped_airlines} OWNER to postgres;
            
            CREATE INDEX IF NOT EXISTS fki_flight_uuid
                ON public.{self.table_scraped_airlines} USING btree
                (flight_uuid ASC NULLS LAST)
                TABLESPACE pg_default;
            """

        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

    def create_scraped_layovers_table(self):
        query = ""
        query += f"""
            CREATE TABLE IF NOT EXISTS public.{self.table_scraped_layovers}
            (
                uuid uuid NOT NULL DEFAULT gen_random_uuid(),
                flight_uuid uuid NOT NULL,
                layover_location text COLLATE pg_catalog."default",
                CONSTRAINT scraped_layovers_pkey PRIMARY KEY (uuid),
                CONSTRAINT flight_uuid FOREIGN KEY (flight_uuid)
                REFERENCES public.{self.table_scraped} (uuid) MATCH SIMPLE
                ON UPDATE CASCADE
                ON DELETE CASCADE
    )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public.{self.table_scraped_layovers} OWNER to postgres;
            
            CREATE INDEX IF NOT EXISTS fki_flight_uuid
                ON public.{self.table_scraped_layovers} USING btree
                (flight_uuid ASC NULLS LAST)
                TABLESPACE pg_default;
            """

        cursor = self.conn.cursor()
        cursor.execute(query)
        cursor.close()

    
    def prepare_db_and_tables(self):
        """
        Creates the database and the table if they don't exist.
        """
        # create database
        if self.db_name not in self.list_all_databases():
            self.create_db()

        # create scraped table
        if self.table_scraped not in self.list_all_tables():
            self.create_scraped_table()

        # create scraped_airlines table
        if self.table_scraped_airlines not in self.list_all_tables():
            self.create_scraped_airlines_table()

    def add_pandas_df_to_db(self, df, table_name):
        extras.register_uuid()

        # Create a list of tuples from the dataframe values
        if table_name == self.table_scraped:
            df = df.reset_index() # otherwise the index (uuid) is not added to the table
        
        tuples = [tuple(x) for x in df.to_numpy()]

        # Comma-separated dataframe columns
        cols = ",".join(list(df.columns))

        cursor = self.conn.cursor()

        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
        try:
            extras.execute_values(cursor, query, tuples)
            logger.info("{} rows added to table [{}]".format(len(df), table_name))
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error: %s" % error)
            self.conn.rollback()

        cursor.close()

        # fix layover time
        # TODO: improve this
        if table_name == self.table_scraped:
            cursor = self.conn.cursor()
            query = f"""
                UPDATE {self.table_scraped}
                SET layover_time = CASE
                WHEN layover_time = -1 THEN null ELSE layover_time END;

                ALTER TABLE public.{self.table_scraped}
                ALTER COLUMN layover_time TYPE smallint;
            """
            cursor.execute(query)
            cursor.close()

    def dump_database_to_file(self):
        """
        Dump the database to a .dump file.
        Returns: the path to the dumped .dump file (archived file, recoverable with pg_restore)
        """

        # create the backup folder if it doesn't exist
        if not os.path.exists(self.backup_folder):
            os.makedirs(self.backup_folder)

        # specify backup filename
        date_str = datetime.now().strftime("%Y%m%d%H%M")
        backup_file = os.path.join(
            self.backup_folder, f"{date_str}_{self.db_name}.dump"
        )

        # run the pg_dump command to create a backup
        try:
            logger.info(f"Dumping database to file: {backup_file}")

            subprocess.run(
                [
                    "pg_dump",
                    f"--dbname=postgresql://{self.db_user}:{self.__db_pw}@{self.db_host}:{self.db_port}/{self.db_name}",
                    "-Fc",
                    "-f",
                    backup_file,
                    "-v",
                ]
            )

            logger.info(f"Database dumped to file: {backup_file}")
            return backup_file

        except Exception as e:
            logger.error(f"Error while dumping database to file: {e}")

    def rotate_database_backups(self):
        """
        Rotate database backups.
        """
        # get list of all backup files
        all_backups = os.listdir(self.backup_folder)
        all_backups = [os.path.join(self.backup_folder, x) for x in all_backups]
        all_backups = [x for x in all_backups if x.endswith(".dump")]

        # if there are no backups, return
        if not all_backups or len(all_backups) == 0:
            return

        # sort the list of backup files by creation time
        all_backups.sort(key=os.path.getctime)

        # ff the number of backups is greater than the rotation limit, delete the oldest file
        while len(all_backups) > self.n_backups_to_keep:
            os.remove(all_backups[0])
            del all_backups[0]

        logger.info(
            f"Rotated database backups. Current number of backups: {len(all_backups)}"
        )
