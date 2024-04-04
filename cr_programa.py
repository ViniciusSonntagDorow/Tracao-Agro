import pandas as pd

def get_data():
    df = pd.read_csv("./input_data/creditorural/Programa.csv", encoding="cp1252")
    return df

def transform_data(df:pd.DataFrame) -> pd.DataFrame:
    df = df[["#CODIGO","DESCRICAO"]].rename(columns={"#CODIGO":"codPrograma", "DESCRICAO":"Programa"})
    df["Programa"] = df["Programa"].replace({"PRONAF - PROGRAMA NACIONAL DE FORTALECIMENTO DA AGRICULTURA FAMILIAR":"PRONAF",
                                                        "PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO":"PRLC-BA",
                                                        "PRODECER III - PROG COOP NIPO-BRASILEIRA P DESENV DOS CERRADOS - ENCERRADO":"PRODECER III",
                                                        "PROCAP-AGRO (PROGRAMA DE CAPITALIZAÇÃO DAS COOPERATIVAS DE PRODUÇÃO AGROPECUÁRIAS)":"PROCAP-AGRO",
                                                        "PROIRRIGA - antigo Moderinfra, alterado em 01/07/2021":"PROIRRIGA",
                                                        "MODERAGRO - PROGRAMA DE MODERNIZAÇÃO DA AGRICULTURA E CONSERVAÇÃO DE RECURSOS NATURAIS":"MODERAGRO",
                                                        "MODERFROTA - PROGRAMA DE MODERNIZAÇÃO DA FROTA DE TRATORES AGRÍCOLAS E IMPL ASSOC E COLHEITADEIRAS":"MODERFROTA",
                                                        "PRODECOOP - PROGRAMA DE DESENVOLVIMENTO COOPERATIVO PARA AGREGAÇÃO DE VALOR À PRODUÇÃO AGROPECUÁRIA":"PRODECOOP",
                                                        "ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono":"ABC +",
                                                        "PSI-RURAL - PROG SUSTENTAÇÃO  INVESTIMENTO ENCERRADO":"PSI-RURAL",
                                                        "PROCAP-CRED (PROG CAPIT COOP CRÉDITO) ENCERRADO":"PROCAP-CRED",
                                                        "PRONAMP - PROGRAMA NACIONAL DE APOIO AO MÉDIO PRODUTOR RURAL":"PRONAMP",
                                                        "MODERMAQ - PROG MOD PARQUE IND NACIONAL - ENCERRADO":"MODERMAQ",
                                                        "PROCERA - PROG ESPECIAL DE CRÉDITO PARA A REFORMA AGRÁRIA - ENCERRADO":"PROCERA",
                                                        "ANF - ATIVIDADE NÃO FINANCIADA ENQUADRADA NO PROAGRO":"ANF",
                                                        "PRI - PROGRAMA DE REFORÇO DO INVESTIMENTO (CIRC 3.745) - ENCERRADO":"PRI",
                                                        "PRORENOVA-RURAL- PROG APOIO  RENOV IMPLANTAÇÃO NOVOS CANAVIAIS- ENCERRADO":"PRORENOVA-RURAL",
                                                        "INOVAGRO - Programa de Incentivo à Inovação Tecnológica na Produção Agropecuária":"INOVAGRO",
                                                        "PCA - Programa para Construção e Ampliação de Armazéns":"PCA",
                                                        "PROGRAMA NACIONAL DE CRÉDITO FUNDIÁRIO (FTRA)":"FTRA",
                                                        "PRORENOVA-IND- PROG APOIO RENOV IMPLANT NOVOS CANAVIAIS - ENCERRADO":"PRORENOVA-IND",
                                                        "PROAQÜICULTURA-PROG APOIO DESENVSETOR AQUÍCOLA - ENCERRADO":"PROAQÜICULTURA",
                                                        "FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA)":"FUNCAFÉ",
                                                        "FNO-ABC (PROG FINANC AGRICULTURA BAIXO CARBONO) ENCERRADO":"FNO-ABC",
                                                        "RenovAgro - Programa de Financiamento a Sistemas de Produção Agropecuária Sustentáveis":"RenovAgro",
                                                        "FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO":"NÃO ESPECIFICADO"})
    return df

def save_data(df:pd.DataFrame, tipo:str):
    if tipo == "csv":
        df.to_csv(f"./output_data/creditorural/cr_programa.csv", index=False)
    
    if tipo == "parquet":
        df.to_parquet(f"./output_data/creditorural/cr_programa.parquet", index=False)

def autoexec():
    df = get_data()
    df_final = transform_data(df)
    save_data(df_final, "csv")

autoexec()