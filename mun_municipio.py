import pandas as pd

def get_data(url:str) -> pd.DataFrame:
    df = pd.read_csv(url)
    return df

def transform_data(uf:pd.DataFrame,meso:pd.DataFrame,micro:pd.DataFrame,muni:pd.DataFrame,coords:pd.DataFrame) -> pd.DataFrame:
    df = muni.merge(
        micro,
        how="left",
        left_on=muni.id_microrregiao,
        right_on=micro.id,
        suffixes=("_mun","_micro")
    ).drop(columns=["key_0","id_micro","id_mesorregiao_micro","id_estado_micro"]).merge(
        meso,
        how="left",
        left_on=muni.id_mesorregiao,
        right_on=meso.id,
        suffixes=("_mun","_meso")
    ).drop(columns=["key_0","id","id_estado"]).merge(
        uf,
        how="left",
        left_on=muni.id_estado,
        right_on=uf.id,
        suffixes=("_muni","_uf")
    ).drop(columns=["key_0","id"]).rename(columns={"id_mun":"cd_munibge",
                                               "id_microrregiao":"cd_microibge",
                                               "id_mesorregiao_mun":"cd_mesoibge",
                                               "id_estado_mun":"cd_ufibge",
                                               "nome_mun":"municipio",
                                               "nome_micro":"microrregiao",
                                               "nome_muni":"mesorregiao",
                                               "nome_uf":"uf"
                                               }).merge(
                                                   coords,
                                                   how="left",
                                                   left_on=muni.id,
                                                   right_on=coords.IBGE).drop(
                                                       columns=["key_0","ID","Cidade","Estado","IBGE"]
                                                   ).rename(columns={"uf":"uf_nome","Sigla":"uf","Latitude":"lat","Longitude":"lon"})
    return df

def save_data(df:pd.DataFrame, tipo:str):
    if tipo == "csv":
        df.to_csv(f"./output_data/municipios/cr_municipios.csv", index=False)
    
    if tipo == "parquet":
        df.to_parquet(f"./output_data/municipios/cr_municipios.parquet", index=False)

def autoexec():
    uf = get_data("./input_data/municipios/brasil-estados.csv")
    meso = get_data("./input_data/municipios/brasil-mesorregioes.csv")
    micro = get_data("./input_data/municipios/brasil-microrregioes.csv")
    muni = get_data("./input_data/municipios/brasil-municipios.csv")
    coords = get_data("./input_data/municipios/Municipios.csv")
    df_final = transform_data(uf, meso, micro, muni, coords)
    save_data(df_final, "parquet")

autoexec()