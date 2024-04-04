import pandas as pd

def get_data() -> list[pd.DataFrame]:
    prod_pec = pd.read_csv("./input_data/pecuaria/producao pecuaria.csv",sep=";",skiprows=4)
    prod_pec = prod_pec.loc[~prod_pec["Leite (Mil litros)"].isna()]
    
    prod_aqui = pd.read_csv("./input_data/pecuaria/producao aquicultura.csv",sep=";",skiprows=4)
    prod_aqui = prod_aqui.loc[~prod_aqui["Carpa (Quilogramas)"].isna()]
    
    return [prod_pec,prod_aqui]

def transform_data_pecuaria(lista:list[pd.DataFrame]) -> pd.DataFrame:
    prod_pec = lista[0]

    prod_pec.columns = ("cod_munibge", "municipio", "Leite", "Ovos de galinha", "Ovos de codorna", "Mel de abelha", "Casulos do bicho-da-seda", "Lã")
    
    prod_pec = ( prod_pec
        .drop(columns=["municipio"])
        .set_index(["cod_munibge"])
        .stack()
        .reset_index()
        .rename(columns={"level_1":"produto",0:"quantidade"})
        )

    for i in ["cod_munibge","quantidade"]:
        prod_pec[i] = prod_pec[i].replace({"-":0,"...":0})
        prod_pec[i] = prod_pec[i].astype("int")

    return prod_pec

def transform_data_aquicultura(lista:list[pd.DataFrame]) -> pd.DataFrame:
    prod_aqui = lista[1]

    colunas = ('cod_munibge','municipio','Carpa','Curimatã, Curimbatá','Dourado','Jatuarana, Piabanha e Piracanjuba','Lambari','Matrinxã','Pacu e Patinga','Piau, Piapara, Piauçu, Piava','Pintado, Cachara, Cachapira e Pintachara, Surubim','Pirapitinga', 'Pirarucu','Tambacu, Tambatinga', 'Tambaqui','Tilápia', 'Traíra e Trairão','Truta', 'Tucunaré','Outros peixes', 'Alevinos','Camarão', 'Larvas e pós-larvas de camarão','Ostras, Vieiras e Mexilhões','Sementes de moluscos','Outros produtos')
    
    prod_aqui.columns = colunas
    prod_aqui = ( prod_aqui
        .drop(columns=['municipio'])
        .set_index(["cod_munibge"])
        .stack()
        .reset_index()
        .rename(columns={"level_1":"produto",0:"quantidade"})
    )

    for i in ["cod_munibge","quantidade"]:
        prod_aqui[i] = prod_aqui[i].replace({"-":0,"...":0})
        prod_aqui[i] = prod_aqui[i].astype("int")

    return prod_aqui

def union_data(pecuaria, aquicultura):
    df = pd.concat([pecuaria, aquicultura])

    produtos = df[["produto"]]
    produtos = produtos.drop_duplicates().reset_index()
    produtos["cod_produto"] = produtos.index
    produtos["cod_produto"] = produtos["cod_produto"].apply(lambda x: x + 1)
    produtos = produtos[["cod_produto","produto"]]

    return produtos

def save_data(df:pd.DataFrame,tipo:str)->None:
    if tipo == "csv":
        df.to_csv("./output_data/pecuaria/pec_produtos.csv", index=False, sep=";")
    if tipo == "parquet":
        df.to_parquet("./output_data/pecuaria/pec_produtos.parquet", index=False)

def auto_exec():
    lista = get_data()
    pecuaria = transform_data_pecuaria(lista)
    aquicultura = transform_data_aquicultura(lista)
    full_df = union_data(pecuaria,aquicultura)
    save_data(full_df,"parquet")

auto_exec()
