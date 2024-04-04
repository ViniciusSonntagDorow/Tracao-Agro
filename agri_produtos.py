import pandas as pd



def get_data() -> pd.DataFrame:
    df = pd.read_csv("./input_data/agricultura/area_plantada_sul.csv",skiprows=4,sep=";")
    df = df.loc[~df["Abacate"].isna()]
    return df



def tabela_produtos(df:pd.DataFrame) -> pd.DataFrame:
    colunas = ('Cód.', 'Município', 'Abacate', 'Abacaxi', 'Açaí', 'Alfafa fenada',
       'Algodão arbóreo', 'Algodão herbáceo', 'Alho',
       'Amendoim', 'Arroz', 'Aveia',
       'Azeitona', 'Banana', 'Batata-doce', 'Batata-inglesa',
       'Borracha', 'Borracha',
       'Cacau', 'Café Total', 'Café Arábica',
       'Café Canephora', 'Caju', 'Cana-de-açúcar',
       'Cana para forragem', 'Caqui', 'Castanha de caju', 'Cebola',
       'Centeio', 'Cevada', 'Chá-da-índia',
       'Coco-da-baía', 'Dendê', 'Erva-mate',
       'Ervilha', 'Fava', 'Feijão', 'Figo',
       'Fumo', 'Girassol', 'Goiaba', 'Guaraná',
       'Juta', 'Laranja', 'Limão', 'Linho', 'Maçã',
       'Malva', 'Mamão', 'Mamona', 'Mandioca', 'Manga',
       'Maracujá', 'Marmelo', 'Melancia', 'Melão', 'Milho',
       'Noz', 'Palmito', 'Pera', 'Pêssego', 'Pimenta-do-reino',
       'Rami', 'Sisal ou agave', 'Soja',
       'Sorgo', 'Tangerina', 'Tomate', 'Trigo',
       'Triticale', 'Tungue', 'Urucum',
       'Uva')
    
    df.columns = colunas
    
    df = ( df
        .set_index(["Cód.","Município"])
        .stack()
        .reset_index()
        .rename(columns={"Cód.":"cod_munibge","level_2":"produto",0:"valor"})
        .drop(columns=["Município"]))
    # codigo produto
    prod = (
        df
        .drop(columns=["cod_munibge","valor"])
        .drop_duplicates())
    prod["cod_produto"] = prod.index
    prod["cod_produto"] = prod["cod_produto"].apply(lambda x: x + 1)
    return prod



def save_data(df:pd.DataFrame, tipo:str) -> None:
    if tipo == "csv":
        df.to_csv("./output_data/agricultura/agri_produtos.csv", index=False)
    if tipo == "parquet":
        df.to_parquet("./output_data/agricultura/agri_produtos.parquet", index=False)



def auto_exec():
    df = get_data()
    prod = tabela_produtos(df)
    save_data(prod,"csv")



auto_exec()

df = get_data()
df.columns