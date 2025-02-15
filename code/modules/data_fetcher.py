import yfinance as yf
import pandas as pd


class DataFetcher:
    def __init__(self, db):
        self.db = db
        self.new_load_threshhold_ratio = 0.05

    def fetch_stock_data(self, ticker, start_date, end_date):
        data = yf.download(ticker, start=start_date, end=end_date)
        data = data.reset_index()
        data.insert(1, "Ticker", ticker)
        
        # Remove ticker row
        data.columns = [col[0] for col in data.columns]

        return data
    
    def ingest_stock_data_to_db(self, table_name, ticker, start_date, end_date):
        ticker_first_date, ticker_last_date = self.db.get_ticker_dates(table_name, ticker)

        # If the ticker is already in the database, only fetch new data
        # Take weekends into account
        if ticker_first_date and ticker_last_date:
            found_day_count = (ticker_last_date - ticker_first_date).days + 1

            # If the found data is more than 5% of the expected data, fetch new data
            expected_day_count = (end_date - start_date).days + 1

            if 1 - found_day_count / expected_day_count > self.new_load_threshhold_ratio:
                # Load new for start_date to ticker_first_date
                new_data = self.fetch_stock_data(ticker, start_date, ticker_first_date)
                if len(new_data) > 0:
                    self.db.insert_from_df(table_name, new_data)
                    print(f"Loaded {len(new_data)} new rows for {ticker} from {start_date} to {ticker_first_date}")

                # Load new for ticker_last_date to end_date
                new_data = self.fetch_stock_data(ticker, ticker_last_date, end_date)
                if len(new_data) > 0:
                    self.db.insert_from_df(table_name, new_data)
                    print(f"Loaded {len(new_data)} new rows for {ticker} from {ticker_last_date} to {end_date}")

        else:
            # Fetch all data
            data = self.fetch_stock_data(ticker, start_date, end_date)
            self.db.insert_from_df(table_name, data)


