import pandas as pd

d = {'cd_finalidade': [1, 2, 3, 4], 'finalidade': ["custeio", "Investimento", "Comercializacao", "Industrializacao"]}
df = pd.DataFrame(data=d)
df.to_csv("./output_data/creditorural/cr_finalidade.csv", index=False)
df.to_parquet("./output_data/creditorural/cr_finalidade.parquet", index=False)