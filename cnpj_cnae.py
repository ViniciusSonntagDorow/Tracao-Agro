import pandas as pd


def read_data():
    cnaes = pd.read_csv(
        "./input_data/cnpj/cnaes.csv",
        sep=";",
        encoding="cp1252",
        names=["cnae_codigo", "cnae"],
        dtype={"cnae_codigo": str, "cnae": str},
    )
    return cnaes

def transform_data(data):
    def categorizar_cnae(codigo):
        if codigo.startswith(("03","02","01")):
            return "Agropecuária, Aquicultura e Produção Florestal"
        elif codigo.startswith(("05","06","07","08","09")):
            return "Indústrias extrativas"
        elif codigo.startswith(tuple([str(i) for i in range(10, 34)])):
            return "Indústrias de transformação"
        elif codigo.startswith("35"):
            return "Eletricidade e Gás"
        elif codigo.startswith(tuple([str(i) for i in range(36, 40)])):
            return "Água, Esgoto, Gestão de resíduos e Descontaminação"
        elif codigo.startswith(tuple([str(i) for i in range(41, 44)])):
            return "Construção"
        elif codigo.startswith(tuple([str(i) for i in range(45, 48)])):
            return "Comércio e Reparação de veículos"
        elif codigo.startswith(tuple([str(i) for i in range(49, 54)])):
            return "Transporte, Armazenagem e Correio"
        elif codigo.startswith(tuple([str(i) for i in range(55, 57)])):
            return "Alojamento e alimentação"
        elif codigo.startswith(tuple([str(i) for i in range(58, 64)])):
            return "Informação e Comunicação "
        elif codigo.startswith(tuple([str(i) for i in range(64, 67)])):
            return "Financeiras, Seguros e Relacionados"
        elif codigo.startswith("68"):
            return "Imobiliárias"
        elif codigo.startswith(tuple([str(i) for i in range(69, 76)])):
            return "Profissionais, Científicas e Técnicas"
        elif codigo.startswith(tuple([str(i) for i in range(77, 83)])):
            return "Administração e Serviços complementares"
        elif codigo.startswith("84"):
            return "Administração pública, Defesa e Seguridade social"
        elif codigo.startswith("85"):
            return "Educação"
        elif codigo.startswith(tuple([str(i) for i in range(86, 89)])):
            return "Saúde humana e serviços sociais"
        elif codigo.startswith(tuple([str(i) for i in range(90, 94)])):
            return "Artes, Cultura, Esporte e Recreação"
        elif codigo.startswith(tuple([str(i) for i in range(94, 97)])):
            return "Outras atividades de serviços"
        elif codigo.startswith("97"):
            return "Serviços domésticos"
        elif codigo.startswith("99"):
            return "Organismos internacionais e outras instituições extraterritoriais"
        else: 
            return "Erro"


    data["cnae_secao"] = data["cnae_codigo"].apply(categorizar_cnae)
    
    return data

def save_data(data, tipo):
    if tipo == "parquet":
        data.to_parquet("./output_data/cnpj/cnpj_cnae.parquet",index = False)
    if tipo == "csv":
        data.to_csv("./output_data/cnpj/cnpj_cnae.csv",index = False)

def auto_exec():

    data = read_data()

    data_clean = transform_data(data)

    save_data(data_clean, "parquet")

auto_exec()