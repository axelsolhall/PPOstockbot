import psycopg2
import pandas as pd
import numpy as np


class DB:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor()

        self.close_other_connections(dbname)

    def close_other_connections(self, dbname):
        try:
            # Query active connections to the specified database
            self.cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid();
            """, (dbname,))
            self.conn.commit()
        except Exception as e:
            print(f"Error closing other connections: {e}")
            self.conn.rollback()

    def create_table_from_df(self, table_name, df):
        # Convert Pandas dtypes to PostgreSQL types
        dtype_mapping = {
            "int64": "BIGINT",
            "float64": "DOUBLE PRECISION",
            "object": "TEXT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP" 
        }

        # Build the CREATE TABLE statement
        columns = ", ".join([f"{col} {dtype_mapping.get(str(df[col].dtype), 'TEXT')}" for col in df.columns])

        # Create the table
        self.cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        self.conn.commit()


    def insert_from_df(self, table_name, df):
        for _, row in df.iterrows():
            values = []
            for x in row:
                if isinstance(x, pd.Timestamp):  # Convert datetime to DATE
                    values.append(f"'{x.date()}'")  # Convert to YYYY-MM-DD format
                elif isinstance(x, str):  # Strings should be quoted
                    values.append(f"'{x}'")
                else:  # Numbers stay raw
                    values.append(str(x))

            query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(values)})"
            self.cur.execute(query)

        self.conn.commit()

    def get_ticker_data(self, table_name, ticker, start_date=None, end_date=None, limit=None):
        query = f"SELECT * FROM {table_name} WHERE ticker = '{ticker}'"
        if start_date:
            query += f" AND date >= '{start_date}'"

        if end_date:
            query += f" AND date <= '{end_date}'"

        query += " ORDER BY date"
        
        if limit:
            query += f" LIMIT {limit}"


        self.cur.execute(query)
        # Make into a DataFrame
        df = pd.DataFrame(self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description])

        return df
    
    def add_rsi_column(self, table_name, ticker, window_size):
        # Get the stock data for the specified ticker
        df = self.get_ticker_data(table_name, ticker)
        
        # Calculate the price changes
        delta = df['close'].diff(1)
        
        # Separate the gains and losses
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        
        # Calculate the average gain and loss over the window size
        avg_gain = pd.Series(gain).rolling(window=window_size, min_periods=1).mean()
        avg_loss = pd.Series(loss).rolling(window=window_size, min_periods=1).mean()
        
        # Calculate the relative strength (RS)
        rs = avg_gain / (avg_loss + 1e-10)  # Avoid division by zero
        
        # Calculate the RSI
        rsi_values = 100 - (100 / (1 + rs))
        
        # Add the column to the database
        self.cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS rsi_{window_size} DOUBLE PRECISION")
        
        # Update the first `window_size - 1` rows with NULL (not valid RSI values yet)
        for i in range(window_size - 1):
            self.cur.execute(f"UPDATE {table_name} SET rsi_{window_size} = NULL WHERE date = '{df.iloc[i]['date']}' AND ticker = '{ticker}'")
        
        # Update the table with the valid RSI values
        for i in range(window_size - 1, len(df)):
            self.cur.execute(f"UPDATE {table_name} SET rsi_{window_size} = {rsi_values.iloc[i]} WHERE date = '{df.iloc[i]['date']}' AND ticker = '{ticker}'")

    def get_ticker_dates(self, table_name, ticker):
        # Return first and last date
        self.cur.execute(f"SELECT MIN(date), MAX(date) FROM {table_name} WHERE ticker = '{ticker}'")
        return self.cur.fetchone()
    
    def list_tables(self):
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        return self.cur.fetchall()
    
    def drop_nans(self, table_name):
        # Get the column names from the table
        self.cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        columns = [row[0] for row in self.cur.fetchall()]

        # Construct the condition to check for NULLs in any of the columns
        condition = " OR ".join([f"{col} IS NULL" for col in columns])

        # Delete rows with NULL values in any column
        self.cur.execute(f"DELETE FROM {table_name} WHERE {condition}")
        self.conn.commit()

    def drop_table(self, table_name):
        self.cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()