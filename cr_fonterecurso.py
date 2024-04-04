import pandas as pd

def get_data():
    df = pd.read_csv("./input_data/creditorural/FonteRecursos.csv", encoding="cp1252")
    return df

def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    df = df[["#CODIGO","DESCRICAO"]].rename(columns={"#CODIGO":"cdFonteRecurso","DESCRICAO":"FonteRecurso"})
    return df

def save_data(df:pd.DataFrame, tipo:str):
    if tipo == "csv":
        df.to_csv(f"./output_data/creditorural/cr_fonterecurso.csv", index=False)
    
    if tipo == "parquet":
        df.to_parquet(f"./output_data/creditorural/cr_fonterecurso.parquet", index=False)

def autoexec():
    df = get_data()
    df_final = transform_data(df)
    save_data(df_final, "parquet")

autoexec()