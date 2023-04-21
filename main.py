import os
import re
import csv
import requests
import pandas as pd
from tqdm import tqdm
from lxml import etree
from typing import List
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}

def scrape_url(url: str, session: requests.Session, headers: dict[str, str]) -> BeautifulSoup:
    response = session.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def get_urls(term: str, uf: str, city: str, headers, session) -> List[str]:
    print('Buscando por registros, por favor aguarde...')
    links = []
    full_urls = []
    url = f'http://casadosdados.com.br/solucao/cnpj?q={term}&uf={uf}&municipio={city}'
    url = url.replace(' ', '%20')
    page = session.get(url=url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')

    try:
        max_page_index = soup.find(
            'ul', {'class': 'pagination-list'}).find_all('li')[-1].text
    except IndexError:
        max_page_index = 0
        print(
            f'Nenhum registro encontrado com o termo "{term}" e os filtros escolhidos!')
        os.system('pause')
        quit()


    for i in range(1, int(max_page_index)+1):
        full_urls.append(f'http://casadosdados.com.br/solucao/cnpj?q={term}&uf={uf}&municipio={city}&page={i}')

    for url in tqdm(full_urls, ncols=70, desc='Progresso: '):
        page = session.get(url=url, headers=HEADERS)
        soup = BeautifulSoup(page.text, 'html.parser')
        for article in soup.find_all('article'):
            a = article.find('a')
            try:
                if 'pesquisa-avancada' not in a.get('href'):
                    links.append(a.get('href'))
            except AttributeError:
                pass
    return links

def get_data(urls: List[str], session, headers) -> List[List[str]]:
    all = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = [executor.submit(scrape_url, f'http://casadosdados.com.br{url}', session, headers) for url in urls]

        for f in tqdm(results, ncols=70, desc='Progresso: '):
            soup = f.result()
            dom = etree.HTML(str(soup))

            cnpj = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[1]/p[2]')
            cnpj = cnpj[0].text if cnpj else 'None'

            razao_social = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[2]/p[2]')
            razao_social = razao_social[0].text if razao_social else 'None'

            nome_fantasia = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[3]/p[2]/text()')
            nome_fantasia = nome_fantasia[0] if nome_fantasia else 'None'
            if nome_fantasia == 'MATRIZ':
                nome_fantasia = 'None'

            logradouro = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[1]/p[2]')
            logradouro = logradouro[0].text if logradouro else 'None'

            numero = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[2]/p[2]')
            numero = numero[0].text if numero else 'None'

            complemento = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[3]/p[2]')
            complemento = complemento[0].text if complemento else 'None'

            cep = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[4]/p[2]')
            cep = cep[0].text if cep else 'None'

            bairro = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[5]/p[2]')
            bairro = bairro[0].text if bairro else 'None'

            municipio = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[6]/p[2]/a')
            municipio = municipio[0].text if municipio else 'None'

            uf = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[7]/p[2]/a')
            uf = uf[0].text if uf else 'None'

            telefone = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[2]/a')
            telefone = telefone[0].text if telefone else 'None'

            email = dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[2]/p[2]/a')
            email = email[0].text if email else 'None'

            quadro_societario = [p for p in dom.xpath(
                '//*[@id="__layout"]/div/div[2]/section[1]/div/div/div[4]/div[1]/div[4]/div')]

            socios = []
            try:
                for i in range(1, len(quadro_societario[0].getchildren())):
                    socios.append(quadro_societario[0].getchildren()[i].getchildren()[0].text)
            except IndexError:
                pass

            all.append([cnpj, razao_social, nome_fantasia, logradouro, numero, re.sub(
                '\s\s+', ' ', str(complemento)), cep, bairro, str(municipio).strip(), str(uf).strip(), telefone, email, str(socios)])
    return all


filename = datetime.now()
filename = datetime.strftime(filename, "%d-%m-%Y_%H.%M.%S")

term = str(input('Digite o termo da busca: '))
uf_es = str(input('Utilizar filtro por estado? s/n: ')).upper()

if uf_es == 'S':
    uf = str(input('Digite a silga do estado (exemplo "SP"): ')).upper()
elif uf_es == 'N':
    uf = ''
else:
    print('Opção invalida!')
    print('Finalizando...')
    quit()

city_es = str(input('Utilizar filtro por município? s/n: ')).upper()
if city_es == 'S':
    city = str(input('Digite o nome do município (exemplo "SAO PAULO"): ')).upper()
elif city_es == 'N':
    city = ''
else:
    print('Opção invalida!')
    print('Finalizando...')
    quit()


with requests.Session() as session:
    links = get_urls(term=term, uf=uf, city=city, headers=HEADERS, session=session)

    print(f'Foram encontrados {len(links)} registros!')
    print('Coletando os dados, por favor aguarde...')
    
    if not os.path.exists(f'{os.getcwd()}\dados'):
        os.system('mkdir dados')

    with open(f'dados/Dados-coletados-{filename}.csv', 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['CNPJ', 'Razão social', 'Nome fantasia', 'Logradouro', 'Número', 'Complemento', 'CEP', 'Bairro', 'Município', 'UF', 'Telefone', 'Email', 'Quadro Societário'])
        data = get_data(links, session=session, headers=HEADERS)
        for d in data:
            writer.writerow(d)


print('\nDados coletados com sucesso!')
print('Gerando arquivo de tabelas, por favor agurade...')


df = pd.read_csv(f'dados\\Dados-coletados-{filename}.csv')
df.to_excel(f'dados\\tabela-de-dados{str(filename)}.xlsx', index=False)
print('Arquivo gerado com sucesso!')
print(f'Arquivos .csv e .xlsx salvos no diretorio {os.getcwd()}\dados \n')

os.system('pause')
