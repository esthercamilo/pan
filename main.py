import json
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import textwrap
from datetime import datetime
import numpy as np

load_dotenv()


def sqlengine():
    db_user = os.environ.get('DB_USER', 'postgres')
    db_password = os.environ['DB_PASSWORD']
    db_host = 'localhost'
    db_port = '5432'
    db_name = 'bancopan'
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    return engine


def carregar_arquivo(arq):
    if arq.endswith('.xlsx'):
        result = pd.read_excel(arq)
    elif arq.endswith('.xlsb'):
        result = pd.read_excel(arq, engine='pyxlsb')
    elif arq.endswith('.csv'):
        result = pd.read_csv(arq)
    else:
        raise ValueError('Formato de arquivo não suportado')

    dt = 'dt_nascimento'
    if dt in result.columns:
        result[dt] = pd.to_datetime(result[dt], origin='1899-12-30', unit='D', errors='coerce')

    if 'cpf' in result.columns:
        result['cpf'] = result['cpf'].astype(str)

    return result


def enviar_para_postgres(df, tabela):

    df.to_sql(tabela, sqlengine(), if_exists='replace', index=False)
    print(f"Dados enviados para a tabela {tabela}")


def transfer():
    data = {
        'base1': 'data/Base 1_anonimizada_nome - light.xlsb',
        'base2': 'data/base2.csv',
        'base3': 'data/Base 3_geral - light v2.xlsb',
        'case': 'data/Case_para_entrevista_-_Banco_Pan_-_vEnvio.xlsx'
    }

    for base, path in data.items():
        df = carregar_arquivo(path)
        try:
            enviar_para_postgres(df, base)
        except Exception as e:
            print(f'Não foi possível enviar o arquivo {path}. Detalhes do erro: {e}')


def plot_analysis_1():
    # Passo 1: Contar a frequência de cada origem dos ministérios, ignorando valores nulos e espaços vazios
    fulldata = pd.read_sql('select * from fulldata', sqlengine())

    # Remover valores nulos e aqueles que são apenas espaços em branco
    fulldata = fulldata[fulldata['orgsup_lotacao_instituidor_pensao'].notna()]  # Remover NaNs
    fulldata = fulldata[fulldata['orgsup_lotacao_instituidor_pensao'].str.strip() != '']  # Remover espaços em branco

    origens_contagem = fulldata['orgsup_lotacao_instituidor_pensao'].value_counts()

    # Quebrar os textos longos no eixo x
    origens_contagem.index = [textwrap.fill(texto, width=20) for texto in origens_contagem.index]

    # Passo 2: Criar o gráfico
    plt.figure(figsize=(14, 8))  # Aumentar o tamanho da figura para acomodar os rótulos
    origens_contagem.plot(kind='bar', color='skyblue')

    plt.title('Distribuição das Origens Ministeriais das Pensões (Escala Logarítmica)')
    plt.xlabel('Origem')
    plt.ylabel('Frequência (Escala Logarítmica)')
    plt.xticks(rotation=45, ha='right', fontsize=10)  # Rotação de 45 graus e ajuste do tamanho da fonte
    plt.yscale('log')  # Aplicar escala logarítmica no eixo y
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()

    # Verificar se o diretório "plots" existe e criar se necessário
    if not os.path.exists('plots'):
        os.makedirs('plots')

    # Passo 3: Salvar o gráfico
    plt.savefig('plots/distribuicao_origens_ministerios_log.png')
    plt.close()  # Fechar a plotagem para liberar memória


def plot_analysis_2():
    # Passo 1: Carregar os dados e filtrar pelos ministérios específicos
    fulldata = pd.read_sql('select * from fulldata', sqlengine())

    # Filtrar pelos ministérios desejados
    ministerios_desejados = ["Ministério da Defesa", "Ministério da Economia", "Ministério da Educação"]
    fulldata = fulldata[fulldata['orgsup_lotacao_instituidor_pensao'].isin(ministerios_desejados)]

    # Remover valores nulos e espaços em branco na coluna 'UF'
    fulldata = fulldata[fulldata['uf'].notna()]  # Remover NaNs
    fulldata = fulldata[fulldata['uf'].str.strip() != '']  # Remover espaços em branco

    # Contar a frequência de cada estado (UF)
    estados_contagem = fulldata['uf'].value_counts()

    # Passo 2: Criar o gráfico
    plt.figure(figsize=(12, 6))
    estados_contagem.plot(kind='bar', color='skyblue')

    plt.title('Distribuição dos Estados de Origem (UF) para Ministérios Específicos')
    plt.xlabel('Estado (UF)')
    plt.ylabel('Frequência')
    plt.xticks(rotation=45, ha='right', fontsize=10)  # Rotação e ajuste dos rótulos no eixo x
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()

    # Verificar se o diretório "plots" existe e criar se necessário
    if not os.path.exists('plots'):
        os.makedirs('plots')

    # Passo 3: Salvar o gráfico
    plt.savefig('plots/distribuicao_estados_ministerios.png')
    plt.close()  # Fechar a plotagem para liberar memória


def plot_analysis_3(m):
    # Passo 1: Carregar os dados e filtrar pelos ministérios específicos
    fulldata = pd.read_sql('select * from fulldata', sqlengine())

    # Filtrar pelos ministérios desejados
    fulldata = fulldata[fulldata['orgsup_lotacao_instituidor_pensao'] == m]

    # Remover valores nulos e espaços em branco na coluna 'dt_nascimento'
    fulldata = fulldata[fulldata['dt_nascimento'].notna()]  # Remover NaNs
    fulldata['dt_nascimento'] = pd.to_datetime(fulldata['dt_nascimento'], errors='coerce')  # Converter para datetime

    # Calcular a idade com base na data atual
    fulldata['idade'] = fulldata['dt_nascimento'].apply(
        lambda x: (datetime.now() - x).days // 365 if pd.notnull(x) else None)

    fulldata['renda'] = pd.to_numeric(fulldata['renda'], errors='coerce')

    # Definir as faixas etárias
    bins = [0, 18, 30, 40, 50, 60, 70, 80, 100]  # Definir as faixas etárias
    labels = ['0-18', '19-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81+']

    fulldata['faixa_etaria'] = pd.cut(fulldata['idade'], bins=bins, labels=labels, include_lowest=True)

    # Contar a frequência das faixas etárias
    faixa_etaria_contagem = fulldata['faixa_etaria'].value_counts().sort_index()

    # Passo 2: Criar o gráfico
    plt.figure(figsize=(10, 6))
    faixa_etaria_contagem.plot(kind='bar', color='lightgreen')

    plt.title(f'Distribuição das Faixas Etárias para {m}')
    plt.xlabel('Faixa Etária')
    plt.ylabel('Frequência')
    plt.xticks(rotation=0, fontsize=10)  # Rótulos alinhados e sem rotação
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()

    # Verificar se o diretório "plots" existe e criar se necessário
    if not os.path.exists('plots'):
        os.makedirs('plots')

    # Salvar renda
    with open('plots/renda.txt', 'a') as f:
        renda_total = fulldata['renda'].sum()
        f.write(f"{m}: {renda_total}\n")

    # Passo 3: Salvar o gráfico
    plt.savefig(f'plots/distribuicao_faixa_etaria_{m}.png')
    plt.close()  # Fechar a plotagem para liberar memória


def plot_case():

    result = {}

    case = pd.read_sql('select * from "case";', sqlengine())
    # Convertendo a coluna 'Data' para datetime (se ainda não estiver)
    case['Data'] = pd.to_datetime(case['Data'])

    # Contagem de atendimentos por Canal
    contagem_canal = case['Canal'].value_counts()
    result['canal'] = contagem_canal.to_dict()

    # Contagem de atendimentos por Segmento de Cliente
    contagem_segmento = case['Segmento cliente'].value_counts()
    result['segmento'] = contagem_segmento.to_dict()

    # Contagem de atendimentos por Motivo de Contato
    contagem_motivo = case['Motivo de contato'].value_counts()
    result['motivo'] = contagem_motivo.to_dict()

    # Criando gráficos
    fig, axs = plt.subplots(3, 1, figsize=(10, 15))

    # Gráfico por Canal
    axs[0].bar(contagem_canal.index, contagem_canal.values, color='skyblue')
    axs[0].set_title('Número de Atendimentos por Canal')
    axs[0].set_xlabel('Canal')
    axs[0].set_ylabel('Número de Atendimentos')
    axs[0].grid(axis='y', linestyle='--', alpha=0.7)

    # Gráfico por Segmento de Cliente
    axs[1].bar(contagem_segmento.index, contagem_segmento.values, color='lightgreen')
    axs[1].set_title('Número de Atendimentos por Segmento de Cliente')
    axs[1].set_xlabel('Segmento de Cliente')
    axs[1].set_ylabel('Número de Atendimentos')
    axs[1].grid(axis='y', linestyle='--', alpha=0.7)

    # Gráfico por Motivo de Contato
    axs[2].bar(contagem_motivo.index, contagem_motivo.values, color='salmon')
    axs[2].set_title('Número de Atendimentos por Motivo de Contato')
    axs[2].set_xlabel('Motivo de Contato')
    axs[2].set_ylabel('Número de Atendimentos')
    axs[2].grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()

    # Salvar o gráfico
    plt.savefig('plots/atendimentos_por_canal_segmento_motivo.png')

    # Gerar um sumário dos dados
    with open('plots/sumario_dados.json', 'w') as f:
        f.write(json.dumps(result))


if __name__ == '__main__':
    # transfer()
    # plot_analysis_1()
    # plot_analysis_2()
    #
    ministerios_desejados = ["Ministério da Defesa", "Ministério da Economia", "Ministério da Educação", "Ministério da Saúde"]
    for m in ministerios_desejados:
        plot_analysis_3(m)
    # plot_case()