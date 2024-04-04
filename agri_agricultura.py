import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

def get_data() -> list[pd.DataFrame]:
    plantada = pd.read_csv("./input_data/agricultura/area_plantada_sul.csv",skiprows=4,sep=";")
    colhida = pd.read_csv("./input_data/agricultura/area_colhida_sul.csv",skiprows=4,sep=";")
    qtdproducao = pd.read_csv("./input_data/agricultura/quantidade_produzida_sul.csv",skiprows=4,sep=";")
    valproducao = pd.read_csv("./input_data/agricultura/valor_producao_sul.csv",skiprows=4,sep=";")
    return [plantada, colhida, qtdproducao, valproducao]

def transform_data(lista:list[pd.DataFrame]) -> pd.DataFrame:
    plantada = lista[0]
    colhida = lista[1]
    qtdproducao = lista[2]
    valproducao = lista[3]

    colunas = ('cod_munibge','município','Abacate','Abacaxi','Açaí','Alfafa','Algodão arbóreo','Algodão herbáceo','Alho','Amendoim','Arroz','Aveia','Azeitona','Banana','Batata-doce','Batata-inglesa','Borracha (látex coagulado)','Borracha (látex líquido)','Cacau','Café Total','Café Arábica','Café Canephora','Caju','Cana-de-açúcar','Cana para forragem','Caqui','Castanha de caju','Cebola','Centeio','Cevada','Chá-da-índia','Coco-da-baía','Dendê','Erva-mate','Ervilha','Fava','Feijão','Figo','Fumo','Girassol','Goiaba','Guaraná','Juta','Laranja','Limão','Linho','Maçã','Malva','Mamão','Mamona','Mandioca','Manga','Maracujá','Marmelo','Melancia','Melão','Milho','Noz','Palmito','Pera','Pêssego','Pimenta-do-reino','Rami','Sisal ou agave','Soja','Sorgo','Tangerina','Tomate','Trigo','Triticale','Tungue','Urucum','Uva')

    plantada.columns = colunas
    plantada = ( plantada
            .drop(columns=['município'])
            .set_index(["cod_munibge"])
            .stack()
            .reset_index()
            .rename(columns={"level_1":"produto",0:"area plantada"})
    )

    colhida.columns = colunas
    colhida = ( colhida
            .drop(columns=['município'])
            .set_index(["cod_munibge"])
            .stack()
            .reset_index()
            .rename(columns={"level_1":"produto",0:"area colhida"})
    )

    qtdproducao.columns = colunas
    qtdproducao = ( qtdproducao
            .drop(columns=['município'])
            .set_index(["cod_munibge"])
            .stack()
            .reset_index()
            .rename(columns={"level_1":"produto",0:"quantidade produzida"})
    )

    valproducao.columns = colunas
    valproducao = ( valproducao
            .drop(columns=['município'])
            .set_index(["cod_munibge"])
            .stack()
            .reset_index()
            .rename(columns={"level_1":"produto",0:"valor da producao"})
    )

    agricultura = (plantada
            .merge(colhida,"left",on=["cod_munibge","produto"])
            .merge(qtdproducao,"left",on=["cod_munibge","produto"])
            .merge(valproducao,"left",on=["cod_munibge","produto"])
        )

    produtos = agricultura[["produto"]]
    produtos = produtos.drop_duplicates()
    produtos["cod_produto"] = produtos.index
    produtos["cod_produto"] = produtos["cod_produto"].apply(lambda x: x + 1)

    agricultura = agricultura.merge(produtos, "left", on=["produto"]).drop(columns=["produto"])

    for i in ["cod_munibge","area plantada","area colhida","quantidade produzida","valor da producao","cod_produto"]:
        agricultura[i] = agricultura[i].replace({"-":0,"...":0})
        agricultura[i] = agricultura[i].astype("int")

    return agricultura

def save_data(df:pd.DataFrame, tipo:str) -> None:
    if tipo == "csv":
        df.to_csv("./output_data/agricultura/agri_agriculura.csv", index=False)
    if tipo == "parquet":
        df.to_parquet("./output_data/agricultura/agri_agriculura.parquet", index=False)

def auto_exec():
    df = get_data()
    df_final = transform_data(df)
    save_data(df_final,"parquet")

auto_exec()
