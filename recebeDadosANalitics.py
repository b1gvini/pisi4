import json
import base64
import requests
import boto3
from datetime import datetime
import pytz

listaDados = []


def getEvents(event, client):
    # cria Nome do arquivo dinamico
    local_tz = pytz.timezone('America/Recife')
    dataHora = datetime.now().astimezone(local_tz)
    dia = dataHora.strftime('%Y-%m-%d')
    horaMinuto = dataHora.strftime('%H00')
    nome = f'{dia}-{horaMinuto}.txt'

    # Definindo info do bucket

    caminhoArquivoLocal = f'/tmp/{dia}-{horaMinuto}.txt'
    nomeDoArquivoLocal = f'{dia}-{horaMinuto}.txt'
    nomeDoS3Bucket = "dadosanalisados"

    # Cria arquivo

    arquivo = open(caminhoArquivoLocal, 'w')

    records = event.get('Records')
    for item in records:
        dados = item['kinesis']['data']
        message = json.loads(base64.b64decode(dados))
        for dado in message['message_type']:
            listaDados.append(str(dado))
            arquivo.write(str(dado) + ",")
        print(listaDados)
    arquivo.close()
    with open(caminhoArquivoLocal, "rb") as f:
        client.upload_fileobj(f, nomeDoS3Bucket, nomeDoArquivoLocal)


def lambda_handler(event, context):
    client = boto3.client('s3')
    getEvents(event, client)
    return {
        'statusCode': 200,
        'body': json.dumps({"message_type": listaDados})
    }
