# Imports
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Funções para sincronizar os bancos
def tipo_vaga():
    # Lendo os valores da tabela tipo_vaga do 2ano
    df_tipo_vaga_bd2 = pd.read_sql_query("SELECT * FROM tipo_vaga;", conn_bd2)


    for i in range(len(df_tipo_vaga_bd2)):
        # Lendo os valores de cada linha do dataframe que contém as informações da tabela tipo_vaga do 2ano
        values2o = (df_tipo_vaga_bd2['nome'][i], df_tipo_vaga_bd2['id'][i])
        
        # Query para atualizar a tabela do 1o com as informações do 2o
        update_query = "UPDATE public.tipo_vaga SET nome = %s WHERE id = %s;"

        # Executando a query de atualização
        cursor_bd1.execute(update_query, values2o)
        
        # Se a query de atualização não tiver nenhuma linha alterada, então significa que os valores na tabela do 2o são novos
        if cursor_bd1.rowcount == 0:
            # query para inserir
            insert_query = "INSERT INTO public.tipo_vaga(nome, id) VALUES (%s, %s);"

            # Executando a query de insert
            cursor_bd1.execute(insert_query, values2o)

    conn_bd1.commit()  # Commit das alterações no bd1

def permissao_vaga():
    # Lendo os valores da tabela tipo_vaga do 2ano
    df_tipo_vaga_bd2 = pd.read_sql_query("SELECT * FROM permissao_vaga;", conn_bd2)

    for i in range(len(df_tipo_vaga_bd2)):

        # Lendo os valores de cada linha do dataframe que contém as informações da tabela tipo_vaga do 2ano
        values2o = (bool(df_tipo_vaga_bd2['permissao'][i]), df_tipo_vaga_bd2['id'][i], df_tipo_vaga_bd2['fk_vaga_id'][i])

        # Query para atualizar a tabela do 1o com as informações do 2o
        update_query = "UPDATE public.permissao_vaga SET permissao = %s WHERE id = %s;"

        # Executando a query de atualização
        cursor_bd1.execute(update_query, (values2o[0], values2o[1]))

        # Se a query de atualização não tiver nenhuma linha alterada, então significa que os valores na tabela do 2o são novos
        if cursor_bd1.rowcount == 0:
            # query para inserir
            insert_query = "INSERT INTO public.permissao_vaga(permissao, id, fk_vaga_id) VALUES (%s, %s, %s);"

            # Executando a query de insert
            cursor_bd1.execute(insert_query, values2o)

    # Queries para pegar os IDs da 
    cursor_bd2.execute("SELECT id FROM permissao_vaga;")
    ids_bd2 = [id_[0] for id_ in cursor_bd2.fetchall()]

    # Verificando a quantidade de IDs para deletar registros
    if ids_bd2:
        if len(ids_bd2) == 1:
            # Caso de um único ID, sem parênteses extras para evitar a vírgula
            delete_query = f"DELETE FROM public.permissao_vaga WHERE id != '{ids_bd2[0]}'"
        else:
            # Caso de múltiplos IDs, utilizando IN com a tupla
            delete_query = f"DELETE FROM public.permissao_vaga WHERE id NOT IN {tuple(ids_bd2)}"
        
        cursor_bd1.execute(delete_query)

    conn_bd1.commit()  # Commit das alterações no bd1

def situacao_trabalhista():

    # Lendo os valores da tabela tipo_vaga do 2ano
    df_tipo_vaga_bd2 = pd.read_sql_query("SELECT * FROM situacao_trabalhista;", conn_bd2)


    for i in range(len(df_tipo_vaga_bd2)):
        # Lendo os valores de cada linha do dataframe que contém as informações da tabela tipo_vaga do 2ano
        values2o = (df_tipo_vaga_bd2['nome'][i], df_tipo_vaga_bd2['id'][i])
        
        # Query para atualizar a tabela do 1o com as informações do 2o
        update_query = "UPDATE public.situacao_trabalhista SET nome = %s WHERE id = %s;"

        # Executando a query de atualização
        cursor_bd1.execute(update_query, values2o)

        # Se a query de atualização não tiver nenhuma linha alterada, então significa que os valores na tabela do 2o são novos
        if cursor_bd1.rowcount == 0:
            # query para inserir
            insert_query = "INSERT INTO public.situacao_trabalhista(nome, id) VALUES (%s, %s);"

            # Executando a query de insert
            cursor_bd1.execute(insert_query, values2o)

    conn_bd1.commit()  # Commit das alterações no bd1

def setor():

    # Lendo os valores da tabela tipo_vaga do 2ano
    df_tipo_vaga_bd2 = pd.read_sql_query("SELECT * FROM setor;", conn_bd2)

    for i in range(len(df_tipo_vaga_bd2)):
        # Lendo os valores de cada linha do dataframe que contém as informações da tabela tipo_vaga do 2ano
        values2o = (df_tipo_vaga_bd2['nome'][i], df_tipo_vaga_bd2['id'][i])
        
        # Query para atualizar a tabela do 1o com as informações do 2o
        update_query = "UPDATE public.setor SET nome = %s WHERE id = %s;"

        # Executando a query de atualização
        cursor_bd1.execute(update_query, values2o)

        # Se a query de atualização não tiver nenhuma linha alterada, então significa que os valores na tabela do 2o são novos
        if cursor_bd1.rowcount == 0:
            # query para inserir
            insert_query = "INSERT INTO public.setor(nome, id) VALUES (%s, %s);"

            # Executando a query de insert
            cursor_bd1.execute(insert_query, values2o)

    conn_bd1.commit()  # Commit das alterações no bd1

def tipo_arquivo():

    # Lendo os valores da tabela tipo_vaga do 2ano
    df_tipo_vaga_bd2 = pd.read_sql_query("SELECT * FROM tipo_arquivo;", conn_bd2)

    for i in range(len(df_tipo_vaga_bd2)):
        # Lendo os valores de cada linha do dataframe que contém as informações da tabela tipo_vaga do 2ano
        values2o = (df_tipo_vaga_bd2['nome'][i], df_tipo_vaga_bd2['id'][i])

        # Query para atualizar a tabela do 1o com as informações do 2o
        update_query = "UPDATE public.tipo_arquivo SET nome = %s WHERE id = %s;"

        # Executando a query de atualização
        cursor_bd1.execute(update_query, values2o)

        # Se a query de atualização não tiver nenhuma linha alterada, então significa que os valores na tabela do 2o são novos
        if cursor_bd1.rowcount == 0:
            # query para inserir
            insert_query = "INSERT INTO public.tipo_arquivo(nome, id) VALUES (%s, %s);"

            # Executando a query de insert
            cursor_bd1.execute(insert_query, values2o)
            
        conn_bd1.commit()  # Commit das alterações no bd1

    conn_bd2.close()
    conn_bd1.close()

def permissao_curso():

    # Lendo os valores da tabela permissao_curso do 2ano
    df_permissao_curso = pd.read_sql_query("SELECT * FROM permissao_curso;", conn_bd2)

    for i in range(len(df_permissao_curso)):
        # Lendo os valores de cada linha do dataframe
        values2o = (bool(df_permissao_curso['permissao'][i]), df_permissao_curso['id'][i], df_permissao_curso['fk_curso_id'][i])

        # Query para atualizar a tabela do 1o com as informações do 2o
        update_query = "UPDATE public.permissao_curso SET permissao = %s WHERE id = %s;"
        cursor_bd1.execute(update_query, (values2o[0], values2o[1]))

        # Se a query de atualização não tiver nenhuma linha alterada, insira os novos valores
        if cursor_bd1.rowcount == 0:
            insert_query = "INSERT INTO public.permissao_curso (permissao, id, fk_curso_id) VALUES (%s, %s, %s);"
            cursor_bd1.execute(insert_query, values2o)

    # Pegando os IDs da tabela permissao_vaga no 2o banco
    cursor_bd2.execute("SELECT id FROM permissao_curso;")
    ids_bd2 = [id_[0] for id_ in cursor_bd2.fetchall()]

    # Verificando a quantidade de IDs para deletar registros
    if ids_bd2:
        if len(ids_bd2) == 1:
            # Caso de um único ID, sem parênteses extras para evitar a vírgula
            delete_query = f"DELETE FROM public.permissao_curso WHERE id != '{ids_bd2[0]}'"
        else:
            # Caso de múltiplos IDs, utilizando IN com a tupla
            delete_query = f"DELETE FROM public.permissao_curso WHERE id NOT IN {tuple(ids_bd2)}"
        
        cursor_bd1.execute(delete_query)

    # Commit das alterações no bd1
    conn_bd1.commit()

# Carregando envs
load_dotenv()

host_1ano = os.getenv("DB_HOST_1ano")
port_1ano = int(os.getenv("DB_PORT_1ano"))
user_1ano = os.getenv("DB_USER_1ano")
password_1ano = os.getenv("DB_PASSWORD_1ano")
database_1ano = os.getenv("DB_NAME_1ano")
db_url_1ano = os.getenv("DATABASE_URL_1ano")

host_2ano = os.getenv("DB_HOST_2ano")
port_2ano = int(os.getenv("DB_PORT_2ano"))
user_2ano = os.getenv("DB_USER_2ano")
password_2ano = os.getenv("DB_PASSWORD_2ano")
database_2ano = os.getenv("DB_NAME_2ano")
db_url_2ano = os.getenv("DATABASE_URL_2ano")

try:
    # conectando-se ao banco do 1ano
    conn_bd1 = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )

    # conectando-se ao banco do 2ano
    conn_bd2 = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Criando cursores
    cursor_bd1 = conn_bd1.cursor()
    cursor_bd2 = conn_bd2.cursor()

    # Chamando as funções
    tipo_vaga()
    permissao_vaga()
    permissao_curso()
    tipo_arquivo()
    tipo_vaga()
    setor()
    situacao_trabalhista()

except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    conn_bd2.close()
    conn_bd1.close()