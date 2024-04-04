import pandas as pd

def get_data(ano:str) -> list[pd.DataFrame]:
    prod_pec = pd.read_csv(f"./input_data/pecuaria/producao_pecuaria_{ano}.csv",sep=";",skiprows=4)
    prod_pec = prod_pec.loc[~prod_pec["Leite (Mil litros)"].isna()]
    
    val_prod_pec = pd.read_csv(f"./input_data/pecuaria/valor_producao_pecuaria_{ano}.csv",sep=";",skiprows=4)
    val_prod_pec = val_prod_pec.loc[~val_prod_pec["Leite"].isna()]
    
    prod_aqui = pd.read_csv(f"./input_data/pecuaria/producao_aquicultura_{ano}.csv",sep=";",skiprows=4)
    prod_aqui = prod_aqui.loc[~prod_aqui["Carpa (Quilogramas)"].isna()]
    
    val_prod_aqui = pd.read_csv(f"./input_data/pecuaria/valor_producao_aquicultura_{ano}.csv",sep=";",skiprows=4)
    val_prod_aqui = val_prod_aqui.loc[~val_prod_aqui["Carpa"].isna()]
    
    return [prod_pec,val_prod_pec,prod_aqui,val_prod_aqui]

def transform_data_pecuaria(lista:list[pd.DataFrame]) -> pd.DataFrame:
    prod_pec = lista[0]
    val_prod_pec = lista[1]

    prod_pec.columns = ("cod_munibge", "municipio", "Leite", "Ovos de galinha", "Ovos de codorna", "Mel de abelha", "Casulos do bicho-da-seda", "Lã")
    prod_pec = ( prod_pec
        .drop(columns=["municipio"])
        .set_index(["cod_munibge"])
        .stack()
        .reset_index()
        .rename(columns={"level_1":"produto",0:"quantidade"})
        )

    val_prod_pec.columns = ("cod_munibge", "municipio", "Leite", "Ovos de galinha", "Ovos de codorna", "Mel de abelha", "Casulos do bicho-da-seda", "Lã")
    val_prod_pec = ( val_prod_pec
        .drop(columns=["municipio"])
        .set_index(["cod_munibge"])
        .stack()
        .reset_index()
        .rename(columns={"level_1":"produto",0:"valor"})
        )
    
    pecuaria = prod_pec.merge(val_prod_pec, how="left", on=["cod_munibge","produto"])

    for i in ["cod_munibge","quantidade","valor"]:
        pecuaria[i] = pecuaria[i].replace({"-":0,"...":0})
        pecuaria[i] = pecuaria[i].astype("int")

    return pecuaria

def transform_data_aquicultura(lista:list[pd.DataFrame]) -> pd.DataFrame:
    prod_aqui = lista[2]
    val_prod_aqui = lista[3]

    colunas = ('cod_munibge','municipio','Carpa','Curimatã, Curimbatá','Dourado','Jatuarana, Piabanha e Piracanjuba','Lambari','Matrinxã','Pacu e Patinga','Piau, Piapara, Piauçu, Piava','Pintado, Cachara, Cachapira e Pintachara, Surubim','Pirapitinga', 'Pirarucu','Tambacu, Tambatinga', 'Tambaqui','Tilápia', 'Traíra e Trairão','Truta', 'Tucunaré','Outros peixes', 'Alevinos','Camarão', 'Larvas e pós-larvas de camarão','Ostras, Vieiras e Mexilhões','Sementes de moluscos','Outros produtos')
    
    prod_aqui.columns = colunas
    prod_aqui = ( prod_aqui
        .drop(columns=['municipio'])
        .set_index(["cod_munibge"])
        .stack()
        .reset_index()
        .rename(columns={"level_1":"produto",0:"quantidade"})
        )
    
    val_prod_aqui.columns = colunas
    val_prod_aqui = ( val_prod_aqui
        .drop(columns=["municipio"])
        .set_index(["cod_munibge"])
        .stack()
        .reset_index()
        .rename(columns={"level_1":"produto",0:"valor"})
        )

    aquicultura = prod_aqui.merge(val_prod_aqui, how="left", on=["cod_munibge","produto"])


    for i in ["cod_munibge","quantidade","valor"]:
        aquicultura[i] = aquicultura[i].replace({"-":0,"...":0})
        aquicultura[i] = aquicultura[i].astype("int")

    return aquicultura

def union_data(pecuaria:pd.DataFrame, aquicultura:pd.DataFrame, ano:int) -> pd.DataFrame:
    df = pd.concat([pecuaria, aquicultura])

    produtos = df[["produto"]]
    produtos = produtos.drop_duplicates().reset_index()
    produtos["cod_produto"] = produtos.index
    produtos["cod_produto"] = produtos["cod_produto"].apply(lambda x: x + 1)
    produtos = produtos[["cod_produto","produto"]]

    df = df.merge(produtos, "left", on=["produto"]).drop(columns=["produto"])

    df["ano"] = ano

    df["valor"] =  df["valor"].apply(lambda x: x * 1000)

    return df

def union_anos(df21, df22):
    df = pd.concat([df21, df22])
    return df

def save_data(df:pd.DataFrame,tipo:str) -> None:
    if tipo == "csv":
        df.to_csv("./output_data/pecuaria/pec_pecuaria.csv", index=False, decimal=",")
    if tipo == "parquet":
        df.to_parquet("./output_data/pecuaria/pec_pecuaria.parquet", index=False)

def auto_exec():
    lista_22 = get_data("2022")
    pecuaria_22 = transform_data_pecuaria(lista_22)
    aquiultura_22 = transform_data_aquicultura(lista_22)
    df_22 = union_data(pecuaria_22,aquiultura_22,2022)

    lista_21 = get_data("2021")
    pecuaria_21 = transform_data_pecuaria(lista_21)
    aquiultura_21 = transform_data_aquicultura(lista_21)
    df_21 = union_data(pecuaria_21,aquiultura_21,2021)

    final = union_anos(df_21, df_22)

    save_data(final,"parquet")

auto_exec()