import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Esse comando carrega as variáveis de ambiente do arquivo .env para o ambiente Python, permitindo o uso seguro de credenciais e configurações sensíveis.
load_dotenv()

# Trando as valores para a conexão do bancoDB, partir do arquivo .env.
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
    # Conecta ao banco de dados do 1º ano.
    conn_1ano = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )
    # Conecta ao banco de dados do 2º ano.
    conn_2ano = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Trazendo todos os registros da tabela 'situacao_trabalhista' no banco do 1º ano e armazena em um DataFrame.
    df_situacao_trabalhista_1ano = pd.read_sql_query('''SELECT * FROM situacao_trabalhista''', conn_1ano)

    # Cria um cursor para executar comandos no banco de dados do 2º ano.
    cursor_2ano = conn_2ano.cursor()

    for i in range(len(df_situacao_trabalhista_1ano)):
        # Define os valores para a atualização da tabela 'situacao_trabalhista' no banco do 2º ano (nome e id).
        values = (df_situacao_trabalhista_1ano['nome'][i], df_situacao_trabalhista_1ano['id'][i])

        # Prepara e executa a query de atualização no banco de dados do 2º ano, atualizando o nome baseado no id.
        update_query = """UPDATE public.situacao_trabalhista
                          SET nome = %s
                          WHERE id = %s;"""
        cursor_2ano.execute(update_query, values)

        # Se não tiver atualização, insere um novo registro com id e permissão.
        if cursor_2ano.rowcount == 0:  
            insert_query = """INSERT INTO public.situacao_trabalhista (id, nome)
                              VALUES (%s, %s);"""
            cursor_2ano.execute(insert_query, (df_situacao_trabalhista_1ano['id'][i], df_situacao_trabalhista_1ano['nome'][i]))

    # Confirma as alterações no banco de dados do 2º ano.
    conn_2ano.commit()

    # Fecha as conexões com os dois bancos de dados.
    conn_2ano.close()
    conn_1ano.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)

try:
    # Conecta ao banco de dados do 1º ano.
    conn_1ano = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )
    # Conecta ao banco de dados do 2º ano.
    conn_2ano = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Trazendo todos os registros da tabela 'tipo_vaga' no banco do 1º ano e armazena em um DataFrame.
    df_tipo_vaga_1ano = pd.read_sql_query('''SELECT * FROM tipo_vaga''', conn_1ano)

    # Cria um cursor para executar comandos no banco de dados do 2º ano.
    cursor_2ano = conn_2ano.cursor()

    for i in range(len(df_tipo_vaga_1ano)):
        # Define os valores para a atualização da tabela 'tipo_vaga' no banco do 2º ano (nome e id).
        values = (df_tipo_vaga_1ano['nome'][i], df_tipo_vaga_1ano['id'][i])

        # Prepara e executa a query de atualização no banco de dados do 2º ano, atualizando o nome baseado no id.
        update_query = """UPDATE public.tipo_vaga
                          SET nome = %s
                          WHERE id = %s;"""
        cursor_2ano.execute(update_query, values)

        # Se não tiver atualização, insere um novo registro com id e permissão.
        if cursor_2ano.rowcount == 0:  
            insert_query = """INSERT INTO public.tipo_vaga (id, nome)
                              VALUES (%s, %s);"""
            cursor_2ano.execute(insert_query, (df_tipo_vaga_1ano['id'][i], df_tipo_vaga_1ano['nome'][i]))

    # Confirma as alterações no banco de dados do 2º ano.
    conn_2ano.commit()

    # Fecha as conexões com os dois bancos de dados.
    conn_2ano.close()
    conn_1ano.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)


try:
    # Conecta ao banco de dados do 1º ano.
    conn_1ano = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )
    # Conecta ao banco de dados do 2º ano.
    conn_2ano = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Trazendo todos os registros da tabela 'setor' no banco do 1º ano e armazena em um DataFrame.
    df_setor_1ano = pd.read_sql_query('''SELECT * FROM setor''', conn_1ano)

    # Cria um cursor para executar comandos no banco de dados do 2º ano.
    cursor_2ano = conn_2ano.cursor()

    for i in range(len(df_setor_1ano)):
        # Define os valores para a atualização da tabela 'setor' no banco do 2º ano (nome e id).
        values = (df_setor_1ano['nome'][i], df_setor_1ano['id'][i])

        # Prepara e executa a query de atualização no banco de dados do 2º ano, atualizando o nome baseado no id.
        update_query = """UPDATE public.setor
                          SET nome = %s
                          WHERE id = %s;"""
        cursor_2ano.execute(update_query, values)

        # Se não tiver atualização, insere um novo registro com id e permissão.
        if cursor_2ano.rowcount == 0:  
            insert_query = """INSERT INTO public.setor (id, nome)
                              VALUES (%s, %s);"""
            cursor_2ano.execute(insert_query, (df_setor_1ano['id'][i], df_setor_1ano['nome'][i]))

    # Confirma as alterações no banco de dados do 2º ano.
    conn_2ano.commit()

    # Fecha as conexões com os dois bancos de dados.
    conn_2ano.close()
    conn_1ano.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)

try:
    # Conecta ao banco de dados do 1º ano.
    conn_1ano = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )
    # Conecta ao banco de dados do 2º ano.
    conn_2ano = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Trazendo todos os registros da tabela 'tipo_arquivo' no banco do 1º ano e armazena em um DataFrame.
    df_tipo_arquivo_1ano = pd.read_sql_query('''SELECT * FROM tipo_arquivo''', conn_1ano)

    # Cria um cursor para executar comandos no banco de dados do 2º ano.
    cursor_2ano = conn_2ano.cursor()

    for i in range(len(df_tipo_arquivo_1ano)):
        # Define os valores para a atualização da tabela 'tipo_arquivo' no banco do 2º ano (nome e id).
        values = (df_tipo_arquivo_1ano['nome'][i], df_tipo_arquivo_1ano['id'][i])

        # Prepara e executa a query de atualização no banco de dados do 2º ano, atualizando o nome baseado no id.
        update_query = """UPDATE public.tipo_arquivo
                          SET nome = %s
                          WHERE id = %s;"""
        cursor_2ano.execute(update_query, values)

        # Se não tiver atualização, insere um novo registro com id e permissão.
        if cursor_2ano.rowcount == 0:  
            insert_query = """INSERT INTO public.tipo_arquivo (id, nome)
                              VALUES (%s, %s);"""
            cursor_2ano.execute(insert_query, (df_tipo_arquivo_1ano['id'][i], df_tipo_arquivo_1ano['nome'][i]))

    # Confirma as alterações no banco de dados do 2º ano.
    conn_2ano.commit()

    # Fecha as conexões com os dois bancos de dados.
    conn_2ano.close()
    conn_1ano.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)

try:
    # Conecta ao banco de dados do 1º ano.
    conn_1ano = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )
    # Conecta ao banco de dados do 2º ano.
    conn_2ano = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Trazendo todos os registros da tabela 'permissao_curso' no banco do 1º ano e armazena em um DataFrame.
    df_permissao_curso_1ano = pd.read_sql_query('''SELECT * FROM permissao_curso''', conn_1ano)

    # Cria um cursor para executar comandos no banco de dados do 2º ano.
    cursor_2ano = conn_2ano.cursor()

    for i in range(len(df_permissao_curso_1ano)):
        # Define os valores para a atualização da tabela 'permissao_curso' no banco do 2º ano (nome e id).
        values = (df_permissao_curso_1ano['nome'][i], df_permissao_curso_1ano['id'][i])

        # Prepara e executa a query de atualização no banco de dados do 2º ano, atualizando o nome baseado no id.
        update_query = """UPDATE public.permissao_curso
                          SET permissao = %s
                          WHERE id = %s;"""
        cursor_2ano.execute(update_query, values)

        # Se não tiver atualização, insere um novo registro com id e permissão.
        if cursor_2ano.rowcount == 0:  
            insert_query = """INSERT INTO public.permissao_curso (id, permissao)
                              VALUES (%s, %s);"""
            cursor_2ano.execute(insert_query, (df_permissao_curso_1ano['id'][i], df_permissao_curso_1ano['permissao'][i]))

    # Confirma as alterações no banco de dados do 2º ano.
    conn_2ano.commit()

    # Fecha as conexões com os dois bancos de dados.
    conn_2ano.close()
    conn_1ano.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)

try:
    # Conecta ao banco de dados do 1º ano.
    conn_1ano = psycopg2.connect(
        user=user_1ano,
        host=host_1ano,
        port=port_1ano,
        password=password_1ano,
        database=database_1ano,
    )
    # Conecta ao banco de dados do 2º ano.
    conn_2ano = psycopg2.connect(
        user=user_2ano,
        host=host_2ano,
        port=port_2ano,
        password=password_2ano,
        database=database_2ano,
    )

    # Trazendo todos os registros da tabela 'permissao_vaga' no banco do 1º ano e armazena em um DataFrame.
    df_permissao_vaga_1ano = pd.read_sql_query('''SELECT * FROM permissao_vaga''', conn_1ano)

    # Cria um cursor para executar comandos no banco de dados do 2º ano.
    cursor_2ano = conn_2ano.cursor()

    for i in range(len(df_permissao_vaga_1ano)):
        # Define os valores para a atualização da tabela 'permissao_vaga' no banco do 2º ano (nome e id).
        values = (df_permissao_vaga_1ano['nome'][i], df_permissao_vaga_1ano['id'][i])

        # Prepara e executa a query de atualização no banco de dados do 2º ano, atualizando o nome baseado no id.
        update_query = """UPDATE public.permissao_vaga
                          SET permissao = %s
                          WHERE id = %s;"""
        cursor_2ano.execute(update_query, values)

        # Se não tiver atualização, insere um novo registro com id e permissão.
        if cursor_2ano.rowcount == 0:  
            insert_query = """INSERT INTO public.permissao_vaga (id, permissao)
                              VALUES (%s, %s);"""
            cursor_2ano.execute(insert_query, (df_permissao_vaga_1ano['id'][i], df_permissao_vaga_1ano['permissao'][i]))

    # Confirma as alterações no banco de dados do 2º ano.
    conn_2ano.commit()

    # Fecha as conexões com os dois bancos de dados.
    conn_2ano.close()
    conn_1ano.close()

except Exception as e:
    print("Error connecting to PostgreSQL:", e)


print("Dados atualizados")