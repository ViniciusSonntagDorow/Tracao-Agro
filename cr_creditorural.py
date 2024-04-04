import pandas as pd

pd.set_option('display.max_columns', 500)

def read_data(path:str, encoding:str, sep:str) -> pd.DataFrame: 
    dados = pd.read_csv(path, encoding=encoding, sep=sep)
    dados["AnoEmissao"] = dados["AnoEmissao"].astype(int)
    dados = dados.loc[dados["AnoEmissao"] > 2021]
    return dados

def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    # Muda o tipo das colunas para str
    columns = ["QtdCusteio","VlCusteio","QtdInvestimento","VlInvestimento","QtdComercializacao","VlComercializacao","QtdIndustrializacao","VlIndustrializacao","AnoEmissao","MesEmissao"]
    for column in columns:
        df[column] = df[column].astype("str")
    
    # Une as colunas de mesmo tema
    newColumns = ["Custeio","Investimento","Comercializacao","Industrializacao"]
    for newColumn in newColumns:
        df[newColumn] = df[f"Qtd{newColumn}"].str.cat(df[f"Vl{newColumn}"],sep="-")

    df = ( df
        .drop(columns=["QtdCusteio","VlCusteio","QtdInvestimento","VlInvestimento","QtdComercializacao","VlComercializacao","QtdIndustrializacao","VlIndustrializacao","#cdEstado","nomeUF","cdMunicipio","Municipio", "AreaInvestimento", "AreaCusteio"])
        .set_index(["codMunicIbge", "MesEmissao","AnoEmissao","cdPrograma","cdSubPrograma","cdFonteRecurso","Atividade", "cnpjIF", "nomeIF"])
        .stack()
        .reset_index()
        .rename(columns={"level_9":"Finalidade"})
    )
    
    df["Quantidade"] = df[0].str.split("-").str[0]
    df["Valor"] = df[0].str.split("-").str[1]

    df["Mes/Ano"] = df["MesEmissao"].str.cat(df["AnoEmissao"], sep="/")
    
    df["Quantidade"] = df["Quantidade"].astype("int")
    df["Valor"] = df["Valor"].astype("float")

    df["codMunicIbge"] = df["codMunicIbge"].fillna(5300108)
    df["codMunicIbge"] = df["codMunicIbge"].astype("int")

    df["Finalidade"] = df["Finalidade"].replace({"Custeio":1,"Investimento":2,"Comercializacao":3,"Industrializacao":4})

    df = ( df
            .loc[df["Quantidade"] != 0]
            .drop(columns=[0,"AnoEmissao","MesEmissao"])
    )

    return df

def save_data(df:pd.DataFrame, tipo:str) -> None:
    
    if tipo == "csv":
        df.to_csv(f"./output_data/creditorural/creditorural.csv", index=False, decimal=",")
    
    if tipo == "parquet":
        df.to_parquet(f"./output_data/creditorural/creditorural.parquet", index=False)

def autoexec() -> None:
    df = read_data("./input_data/creditorural/SICOR_CONTRATOS_MUNICIPIO.csv", "cp1252", "|")
    
    df_final = transform_data(df)

    save_data(df_final, "csv")

autoexec()