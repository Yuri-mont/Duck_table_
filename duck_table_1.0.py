import pandas as pd
from sqlalchemy import create_engine 
import sqlalchemy as sa
from time import sleep
from os import walk,getcwd,system,replace
from random import randint


GREEN = '\033[32m'
system(f"echo '{GREEN}'")

arquivo = []
pasta = []
user = ''
erro = False
used = []
dfs = []
using = ['red','green','yellow','blue','magenta','cyan','white']
COLORS = {
    'red'    : '\033[31m',
    'green'  : '\033[32m',
    'yellow' : '\033[33m',
    'magenta': '\033[35m',
    'cyan'   : '\033[36m',
    'white'  : '\033[37m',
    'reset'  : '\033[0m'  # resets the color back to default
}
CODIGOS = {
    '1':'red'     ,
    '2':'green'   ,
    '3':'yellow'  ,
    '4':'magenta' ,
    '5':'cyan'    ,
    '6':'white'  

}
con = sa.engine.URL.create(
    drivername="postgresql",
    username="",
    password="",
    host="",
    database="",
)
meta = sa.MetaData(schema="")

#🦆
#txt
system('cls')
path_pasta = getcwd()
print(path_pasta)
for (dirpath, dirnames, filenames) in walk(f"{path_pasta}/tabelas"):
    for file in filenames:
        if file.endswith(".xlsx") or file.endswith(".csv") or file.endswith(".txt"):
            pasta.append(file)
    break
print(len(pasta))
print('\n')



def rais(arquivo,path_pasta):
    caminho = f'{path_pasta}{arquivo}'

    with open(f'{caminho}', 'r') as file:
        lines = file.readlines()

    lines[0] = lines[0].replace('á','a')
    lines[0] = lines[0].replace('ã','a')
    lines[0] = lines[0].replace('é','e')
    lines[0] = lines[0].replace('ê','e')
    lines[0] = lines[0].replace('í','i')
    lines[0] = lines[0].replace('ó','o')
    lines[0] = lines[0].replace('ô','o')
    lines[0] = lines[0].replace('õ','o')
    lines[0] = lines[0].replace('ç','c')

    with open(f'{caminho}', 'r') as infile, open(f'{path_pasta}tmp{arquivo}', 'r+') as outfile:
        first_line = lines[0]
        outfile.write(first_line)
        for line in infile.readlines()[1:]:
            outfile.write(line)
    
    replace(f'{path_pasta}tmp{arquivo}', f'{caminho}')
 

for i in pasta:
    cor = CODIGOS[str(randint(1,len(CODIGOS)))]
    if not cor in used:
        used.append(cor)
        if not len(using) == 0:
            using.remove(cor)
        
    if pasta.index(i)+1 <10:
        print(COLORS[f'{cor}'],f'🦆[0{pasta.index(i)+1}]  {i} ')
    else:
        print(COLORS[f'{cor}'],f'🦆[{pasta.index(i)+1}]  {i} ')
print(COLORS['reset'])

try:
    while True:
        files = []
        arquivo = (input('\nNúmero do(s) arquivo(s): ').split(' '))
        if len(arquivo) <= len(pasta):
            break
        
    for i in arquivo:
        files.append(pasta[int(i)-1])
    
    print(f'\nVoce escolheu o(s) arquivo(s): {files}')
    for tabelas in files:
        if tabelas.endswith(".xlsx"):
            print(f'\nProcessando {tabelas}...')
            df = pd.read_excel(f'{path_pasta}/tabelas/{tabelas}')
            dfs.append(df)
        elif tabelas.endswith(".txt"):
            if 'Rais' in tabelas:
                rais(tabelas,path_pasta)
            print(f'\nProcessando {tabelas}...')
            df = pd.read_csv(f'{path_pasta}/tabelas/{tabelas}', delimiter=';')
            dfs.append(df)
        else:
            print(f'\nProcessando {tabelas}...')
            df = pd.read_csv(f'{path_pasta}/tabelas/{tabelas}')   
            dfs.append(df)

except(FileNotFoundError):
    print(f'\nArquivo {tabelas} não encontrado\n')
    erro  = True
except(UnicodeDecodeError):
    df = pd.read_csv(f'{path_pasta}/tabelas/{tabelas}', delimiter=';',encoding='ISO-8859-1')
    print(f'\nSua tabela não é utf-8\n')
except(ValueError):
    print(f'\nNúmero inválido\n')
    erro  = True
except(IndexError):
    print('\nNúmero inválido\n')
    erro  = True

if erro == False:
    for tabelas in files:
        arquivo = str(tabelas)
        #🦆
        engine = create_engine(con)
        nome = tabelas

        nome = nome.replace('.xlsx','')
        nome = nome.replace('.txt','')
        nome = nome.replace('.csv','')
        nome = nome.replace(' ', '_')
        nome = nome.replace('.','_')
        nome = nome.lower()
        
        df  = dfs[files.index(tabelas)]
        df.to_sql(f'{nome}', con=engine, if_exists='append', index=False,schema='') #type: ignore
        print(f'\n\ntabela salva como {nome}')
    print(f'\nEssa janela vai se fechar em 10 segundos\n')
    sleep(9)
    # cria conexao com o sql define nome se existe ela nao apaga os dados ela "append" e especifica o squema 

sleep(4)
#🦆

