import random
import pandas as pd
import numpy as np

data = pd.DataFrame({
    'id': range(10),
    'name': [random.choice(['Mary', 'Esther', 'John']) for turn in range(10)],
    'age': [random.randint(12, 20) for turn in range(10)],
    'creation_timestamp': pd.date_range('2025-05-01', periods = 10, freq = 'D'),
    'balance': np.random.randint(200, 3450, 10)
})


data.to_csv('data/data.csv', index = False, header = True)