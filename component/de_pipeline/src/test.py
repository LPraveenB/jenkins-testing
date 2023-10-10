import pandas as pd

df = pd.read_parquet('model1.parquet')
print(df.columns)

print("model 2***** ")
df = pd.read_parquet('model2.parquet')
print(df.columns)