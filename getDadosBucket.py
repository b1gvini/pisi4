import json
import base64
import requests
import boto3
from datetime import datetime
import pytz


def getNomeArquivo():
    local_tz = pytz.timezone('America/Recife')
    dataHora = datetime.now().astimezone(local_tz)
    dia = dataHora.strftime('%Y-%m-%d')
    horaMinuto = dataHora.strftime('%H00')
    nome = f'{dia}-{horaMinuto}.txt'
    return nome


def lambda_handler(event, context):
    nome_do_arquivo_download = getNomeArquivo()
    nome_do_s3 = 'dadosanalisados'

    s3 = boto3.resource('s3')
    obj = s3.Object(nome_do_s3, nome_do_arquivo_download)
    body = obj.get()['Body'].read().decode()
    print(body[:-1])

    return {
        'statusCode': 200,
        'body': json.dumps(body)
    }

