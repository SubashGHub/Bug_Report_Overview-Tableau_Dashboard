import pandas as pd
from sql_pgrm.log_class import Logger
from datetime import datetime

log = Logger()


class ProcessData:

    @log.track_execution_time
    def read_csv_data(self, file_path):
        """Reads data from a CSV file into a DataFrame."""
        df = pd.read_csv(file_path)
        log.info('CSV Data Read')
        df_clean = self.__fill_NaN_data(df)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
        return df_clean

    def __fill_NaN_data(self, df):
        # Drop columns where all values are NaN
        df.dropna(axis=1, how='all', inplace=True)

        # Numeric columns: Fill with median
        num_cols = df.select_dtypes(include=['int64', 'float64']).columns
        df[num_cols] = df[num_cols].apply(lambda col: col.fillna(col.median()) if col.notna().sum() > 0 else col)

        # Categorical columns: Fill with mode
        cat_cols = df.select_dtypes(include=['object']).columns
        df[cat_cols] = df[cat_cols].apply(lambda col: col.fillna(col.mode()[0]) if not col.mode().empty else col)
        log.info('Data Cleaned. Filled NaN Values.')
        return df

    def __parse_date(self, date_str):
        """ Convert date string from CSV to MySQL DATE format. """
        if pd.isna(date_str) or date_str in ["", "NULL"]:  # Handle empty values
            return None
        formats_to_try = ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d']  # List of possible formats to try
        for fmt in formats_to_try:
            try:
                return datetime.strptime(date_str, fmt).date()  # Adjust format as per CSV
            except ValueError:
                pass  # Ignore and try the next format
        log.warning(f"Invalid date format: {date_str}")
        return None  # Return None if no format matches
