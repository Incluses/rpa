# Projeto de Atualização de Banco de Dados

Este projeto consiste na atualização de dados em um banco de dados PostgreSQL, utilizando a biblioteca `psycopg2` para gerenciar a conexão e o `pandas` para manipulação de dados. Os dados são extraídos de um banco de dados do 1º ano e inseridos ou atualizados em um banco de dados do 2º ano e também os dados são extraídos de um banco de dados do 2º ano e inseridos ou atualizados em um banco de dados do 1º ano, utilizando um conjunto de tabelas predefinidas.

## Requisitos

- Python 3.x
- Bibliotecas:
  - `psycopg2`
  - `pandas`
  - `python-dotenv`

## Instalação

1. Clone este repositório:
```
git clone https://github.com/Incluses/rpa.git
```


2. Instale as dependências necessárias:
```
pip install psycopg2 pandas python-dotenv
```


3. Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:
```
DB_HOST_1ano=<seu_host_1ano> 
DB_PORT_1ano=<sua_porta_1ano> 
DB_USER_1ano=<seu_usuario_1ano> 
DB_PASSWORD_1ano=<sua_senha_1ano> 
DB_NAME_1ano=<seu_banco_1ano> 
DATABASE_URL_1ano=<sua_url_do_banco_1ano>


DB_HOST_2ano=<seu_host_2ano> 
DB_PORT_2ano=<sua_porta_2ano> 
DB_USER_2ano=<seu_usuario_2ano> 
DB_PASSWORD_2ano=<sua_senha_2ano>
DB_NAME_2ano=<seu_banco_2ano> 
DATABASE_URL_2ano=<sua_url_do_banco_2ano>
```


## Uso

Para executar o script, você pode usar o seguinte comando:

```
python rpa_1o_to_2o.py
python rpa_2o_to_1o.py
```


O script realiza as seguintes operações:

Conecta ao banco de dados do 1º ano e ao banco de dados do 2º ano.
Extrai os dados das seguintes tabelas do banco do 1º ano:
- situacao_trabalhista
- tipo_vaga
- setor
- tipo_arquivo
- permissao_curso
- permissao_vaga
Atualiza ou insere os dados correspondentes no banco de dados do 2º ano.
Extrai os dados das seguintes tabelas do banco do 2º ano:
- situacao_trabalhista
- tipo_vaga
- setor
- tipo_arquivo
- permissao_curso
- permissao_vaga
Atualiza ou insere os dados correspondentes no banco de dados do 1º ano.

Após a execução, você verá uma mensagem indicando que os dados foram atualizados.

## Tratamento de Erros

O script possui tratamento básico de erros, exibindo mensagens no console caso ocorra algum problema ao se conectar aos bancos de dados ou ao executar as queries.


## Feito por

[Luca Almeida Lucareli](https://github.com/LucaLucareli)

[Olivia Farias Domingues](https://github.com/oliviaworks)
