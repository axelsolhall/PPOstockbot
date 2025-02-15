import pandas as pd
import numpy as np

class TradingEnv:
    def __init__(self, db, table_name, tickers, window_size, simulation_length, n_envs, headers):
        self.db = db
        self.table_name = table_name
        self.tickers = tickers
        self.window_size = window_size
        self.simulation_length = simulation_length
        self.n_envs = n_envs
        self.headers = headers


    def build_dataset(self):
        # Build data array for stock data


        # Build data array for state information

        pass