import json
import requests
import boto3
from datetime import datetime
import pytz

local_tz = pytz.timezone('America/Recife')
listaDadosInmet = []
#Requisição buscando os dados de acordo com data e hora.
def getResponse(date, hour):
    response = requests.get(
        f'https://apitempo.inmet.gov.br/estacao/dados/{date}/{hour}')
    print(f'https://apitempo.inmet.gov.br/estacao/dados/{date}/{hour}')
    return response

#Validação da Responsta. Status code OK(true) ou ERRO 4** 5** (False).
def isResponseValid(response):
    if response.status_code == 200:
        return True
    else:
        return print("Error - Status code: " + str(response.status_code))

#Filtro pela UF -> PE (pernambuco).
def filtraEstadoPE(response):
    listaFiltrada = filter(lambda x: x["UF"] == "PE", response.json())
    return listaFiltrada

def criaListaDadosFiltrados(dadosFiltrados, horaMinuto):
    for dados in dadosFiltrados:
        dir = {
            'DC_NOME': dados['DC_NOME'],
            'CD_ESTACAO': dados['CD_ESTACAO'],
            'PRECIPITACAO': dados['CHUVA'],
            'TEMP_MAX': dados['TEM_MAX'],
            'TEMP_INST': dados['TEM_INS'],
            'TEMP_MIN': dados['TEM_MIN'],
            'UMD_INST': dados['UMD_INS'],
            'HORA': horaMinuto
        }
        listaDadosInmet.append(dir)

def enviaDados(listaDadosInmet):
    client = boto3.client("kinesis", "us-east-1")
    client.put_records(
        Records=[{
            'Data': json.dumps({"message_type": listaDadosInmet}),
            'PartitionKey': 'key'
        }],
        StreamName="streamRecebeDadosInmet",
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Dados enviados com sucesso.')
    }

def lambda_handler(event, context):
    dataHora = datetime.now().astimezone(local_tz)
    dia = dataHora.strftime('%Y-%m-%d')
    horaMinuto = dataHora.strftime('%H00')
    response = getResponse(dia, horaMinuto)
    if (isResponseValid(response)):
        filterData = filtraEstadoPE(response)
        criaListaDadosFiltrados(filterData, horaMinuto)
        print(listaDadosInmet)
        retorno = enviaDados(listaDadosInmet)
        return retorno
