from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum
import os
from os.path import join
import pandas as pd

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2022, 8, 22, tz="UTC"),
    schedule_interval='0 0 * * 1',  # executar toda segunda feira
) as dag:

    tarefa_1 = BashOperator(
        task_id='criar_pasta',
        bash_command='mkdir -p "semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_infos_clima(**kwargs):
        data_interval_start = kwargs['ds']
        city = 'RiodeJaneiro'
        key = '6LC77KW7CF2UTV5ZL6UCDEQ8J'
    
        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
                    f'{city}/{data_interval_start}/{ds_add(data_interval_start, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')
    
        dados = pd.read_csv(URL)
        print(dados.head())
    
        file_path = f'semana={data_interval_start}/'
        os.makedirs(file_path, exist_ok=True)  # Create the directory if it doesn't exist
    
        dados.to_csv(os.path.join(file_path, 'dados_brutos.csv'))
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(os.path.join(file_path, 'temperaturas.csv'))
        dados[['datetime', 'description', 'icon']].to_csv(os.path.join(file_path, 'condicoes.csv'))

    tarefa_2 = PythonOperator(
        task_id='extrair_dados_climaticos',
        python_callable=extrai_infos_clima,
        provide_context=True,  # This is important to provide the context to the callable function
    )

    tarefa_1 >> tarefa_2  # Set the task dependencies
