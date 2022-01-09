try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.postgres_operator import PostgresOperator
    from datetime import datetime
    from sqlalchemy import create_engine
    from sqlalchemy_utils import database_exists, create_database
    import pandas as pd

    print('All Dag modules are ok ......')

except Exception as e:
    print(f'Error  {e}')


def first_function_execute(**kwargs):
    print('Extracción de datos')
    url = kwargs.get('url')
    nba_player_stats_per_game = pd.read_html(url, header=0)
    nba_player_stats_per_game = nba_player_stats_per_game[0]
    print('@'*50)
    print(nba_player_stats_per_game.head(10))
    print('@'*50)
    kwargs['ti'].xcom_push(key='df', value=nba_player_stats_per_game)
    return 'Extracción completa'


def second_function_execute(**kwargs):
    print('Limpieza de datos')
    df = kwargs.get('ti').xcom_pull(key='df')
    df = df.drop(df[df['Age'] == 'Age'].index)
    df = df[['Player', 'Age']]
    print('_'*50)
    print(df.head(50))
    print('_'*50)
    kwargs['ti'].xcom_push(key='df', value=df)
    return 'Datos limpios y filtrados'


def third_function_execute(**kwargs):
    print('Inserción en base de datos')
    print('Leyendo ENGINE URL')
    engine_url = kwargs.get('engine_url')
    print('Leyendo DataFrame filtrado')
    df = kwargs.get('ti').xcom_pull(key='df')
    print('Verificando existencia de Tabla')
    engine = create_engine(engine_url)

    print('¿Existe ya la base de datos?', database_exists(engine.url))

    if not database_exists(engine.url):
        print('La base de datos no existe, creando...')
        create_database(engine.url)

    if database_exists(engine.url):
            data_type = str(engine.url).rsplit('/', 1)[1]
            print('Llenando la base de datos con información de:', data_type)
            df.to_sql(data_type, engine)


args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2022, 1, 1), }

with DAG(dag_id='first_dag', schedule_interval='@daily', default_args=args,
         catchup=False) as dag:

    first_function_execute = PythonOperator(
        task_id='primera_tarea',
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={'url': 'https://www.basketball-reference.com/leagues/NBA_2021_per_game.html'})

    second_function_execute = PythonOperator(
        task_id='segunda_tarea',
        python_callable=second_function_execute,
        provide_context=True)

    third_function_execute = PythonOperator(
        task_id='tercera_tarea',
        python_callable=third_function_execute,
        provide_context=True,
        op_kwargs={'engine_url': 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'}
    )

first_function_execute >> second_function_execute >> third_function_execute
