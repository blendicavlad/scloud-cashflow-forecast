from datetime import timedelta
import pandas as pd
import os
import s3fs

print(timedelta(-365 * 2))
print(os.environ.get('key'))
print(os.environ.get('secret'))
print(os.environ.get('session'))
print('test')


data = {'Name': ['Tom', 'nick', 'krish', 'jack'],
        'Age': [20, 21, 19, 18]}

# Create DataFrame
df = pd.DataFrame(data)

df.to_csv('s3://ml-cashflow-forecast/dev_data/test/test.csv',
          storage_options={'key': os.environ.get('key'),
                           'secret': os.environ.get('secret'),
                           'token': os.environ.get('session')}
          )

df2 = pd.read_csv('s3://ml-cashflow-forecast/dev_data/test/test.csv',
          storage_options={'key': os.environ.get('key'),
                           'secret': os.environ.get('secret'),
                           'token': os.environ.get('session')}
          )

dict = {
    'test1' : 1,
    'test2': 2,
    'test3': {
        'test3_1': 31
    }
}

print(str(dict))
