import pymysql
import threading
from sql_pgrm.log_class import Logger
import pandas as pd

log = Logger()

# Database Configuration
db_config = {
    "host": '127.0.0.1',
    "user": 'root',
    "password": 'sqlPass@3',
    "database": 'mydb',
    "port": 3306
}


class SqlOperation:

    def create_connection(self):
        try:
            conn = pymysql.connect(**db_config)
            log.info('Database connection established successfully.')
            return conn
        except pymysql.MySQLError as e:
            log.error(f'Database connection error: {e}')
            return None

    def create_table(self):
        """Creates a table if it does not exist."""
        query = '''
                 CREATE TABLE IF NOT EXISTS Retail_Market_Sales (
                     Transaction_ID INT,
                     Date TIMESTAMP,
                     Customer_ID Varchar(20),
                     Gender Varchar(10),
                     Age INT,     
                     Product_Category VARCHAR(255),
                     Quantity INT,
                     Price_per_Unit INT,
                     Total_Amount INT
                 )
                 '''
        conn = self.create_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                conn.commit()
                log.info("Table `Retail_Market_Sales` created successfully in MySQL.")
            except pymysql.MySQLError as e:
                log.error(f"Error creating table: {e}")
            finally:
                conn.close()

    @log.track_execution_time
    def __insert_batch_data(self, batch_data):
        """Inserts data from DataFrame into MySQL table."""
        query = (
            '''
            INSERT INTO Retail_Market_Sales 
            (Transaction_ID, Date, Customer_ID, Gender, Age, Product_Category, Quantity, Price_per_Unit, 
            Total_Amount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''')
        conn = self.create_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(query, batch_data)
                conn.commit()
                log.info(f"Inserted batch of {len(batch_data)} records successfully.")
            except pymysql.MySQLError as e:
                log.error(f"Error inserting batch: {e}")
            finally:
                conn.close()

    @log.track_execution_time
    def insert_data_multithreaded(self, df, num_threads):
        """Splits data into batches and inserts using multiple thread_list."""
        batch_size = len(df) // num_threads  # Divide data into equal batches
        thread_list = []

        for i in range(num_threads):
            start_idx = i * batch_size
            end_idx = None if i == num_threads - 1 else (i + 1) * batch_size  # Handle last batch
            data_batch = [
                # (row.name, row.age, row.city, self.parse_date(row.dob))  # Parse data before inserting
                tuple(row)
                for row in df.iloc[start_idx:end_idx].itertuples(index=False, name=None)
            ]
            thread = threading.Thread(target=self.__insert_batch_data, args=(data_batch,))
            thread_list.append(thread)
            thread.start()  # Start the thread

        for thread in thread_list:
            thread.join()  # Wait for all thread_list to finish

    def fetch_limited_data(self, query, output_file):
        """
        Fetches a limited number of records from MySQL and stores them in a CSV file.

        :param db_config: Dictionary with database connection parameters
        :param query: SQL query to fetch limited data
        :param output_file: File path to save the data
        """
        # Create connection
        conn = self.create_connection()
        cursor = conn.cursor()
        try:
            # Execute query
            cursor.execute(query)
            rows = cursor.fetchall()

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert to DataFrame and save as CSV
            df = pd.DataFrame(rows, columns=columns)
            df.to_csv(output_file, index=False)

            log.info(f"{len(df)} Data fetched from DB and saved to {output_file}")

        except pymysql.MySQLError as e:
            log.error(f"MySQL Error: {e}")

        finally:
            # Close connection
            cursor.close()
            conn.close()

    def remove_duplicates_sql(self):
        multi_query = '''
        create table temp as
        select distinct * from retail_market_sales;
        drop table retail_market_sales;
        rename table temp to retail_market_;
        '''
        try:
            with self.create_connection() as conn:
                with conn.cursor() as cursor:
                    for query in multi_query.strip().split(";"):  # Split queries by ';'
                        if query.strip():  # Ignore empty queries
                            cursor.execute(query)
                conn.commit()  # Commit changes after execution
            print("All queries executed successfully.")
        except pymysql.Error as e:
            print(f"Error executing queries: {e}")
