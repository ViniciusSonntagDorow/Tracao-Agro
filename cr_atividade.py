import pandas as pd

d = {'cd_atividade': [1, 2], 'atividade': ["Agrécola","Pecuária"]}
df = pd.DataFrame(data=d)
df.to_csv("./output_data/creditorural/cr_atividade.csv", index=False)
df.to_parquet("./output_data/creditorural/cr_atividade.parquet", index=False)