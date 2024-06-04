import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def add_randomized_columns(df, base_date_str):
    base_date = datetime.strptime(base_date_str, "%Y-%m-%d")
    
    def random_time(base_date):
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return base_date + timedelta(hours=hour, minutes=minute, seconds=second)
    
    def random_processing_time():
        return timedelta(minutes=random.randint(2, 60))
    
    df['date'] = [random_time(base_date) for _ in range(len(df))]
    df['process_time'] = [random_processing_time() for _ in range(len(df))]
    df['closed_date'] = df.apply(lambda row: row['date'] + row['process_time'] if row['STATUS'] == 'CLOSED' else pd.NaT, axis=1)
    
    return df