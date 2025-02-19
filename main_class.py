from sql_class import SqlOperation
from sql_pgrm.log_class import Logger
from sql_pgrm.data_preprocess_class import ProcessData


class Main:
    log = Logger()
    sql_obj = SqlOperation()
    data_obj = ProcessData()

    file_path = "retail_market_sales_dataset.csv"

    sql_obj.create_table()
    df_clean = data_obj.read_csv_data(file_path)

    log.info("Starting multi-threading data insertion...")
    sql_obj.insert_data_multithreaded(df_clean, num_threads=5)

    # Fetch limited records and store in a CSV
    sql_query = "SELECT * FROM Retail_Market_Sales LIMIT 500;"
    sql_obj.fetch_limited_data(sql_query, "db_output_data.csv")

    log.info('------------Program Completed--------------\n')
