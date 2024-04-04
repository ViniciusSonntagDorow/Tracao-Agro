import pandas as pd

def get_data():
    df = pd.read_csv("./input_data/creditorural/Subprogramas.csv", encoding="cp1252", sep=";")
    return df

def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    df = df[["#CODIGO_SUBPROGRAMA","DESCRICAO_SUBPROGRAMA"]].rename(columns={"#CODIGO_SUBPROGRAMA":"cdSubPrograma","DESCRICAO_SUBPROGRAMA":"SubPrograma"})
    return df

def save_data(df:pd.DataFrame, tipo:str):
    if tipo == "csv":
        df.to_csv(f"./output_data/creditorural/cr_subprograma.csv", index=False)
    
    if tipo == "parquet":
        df.to_parquet(f"./output_data/creditorural/cr_subprograma.parquet", index=False)

def autoexec():
    df = get_data()
    df_final = transform_data(df)
    save_data(df_final, "csv")

autoexec()