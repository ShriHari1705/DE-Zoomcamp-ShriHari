import sys
import pandas as pd

print('arguments',sys.argv)
month = int(sys.argv[1])
df = pd.DataFrame({'days':[1,2,3],'no_passengers':[4,5,6]})
df['month'] = month
print(df.head())

df.to_parquet(F"output_{month}.parquet")

print(f'hello from pipeline, month={month}')