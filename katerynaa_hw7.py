from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
from datetime import datetime
import random
import time


#Tasks

def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)

def random_medal_choice():
    return random.choice(["Gold", "Silver", "Bronze"])

def delay_execution():
    time.sleep(10)   

# завдання для випадкового вибору медалі
def pick_medal(ti):
    medals = ["Bronze", "Silver", "Gold"]
    return random.choice(medals)

# завдання для branch operator, який визначає наступне завдання відповідно до медалі, що випала
def select_next_task(ti):
    medal = ti.xcom_pull(task_ids = "pick_medal")
    if medal == "Bronze":
        return "count_bronze_medals"
    elif medal == "Silver":
        return "count_silver_medals"
    else:
        return "count_gold_medals"
    

#Arguments/parameters

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 31, 0, 0),
}

#ідентифікатор для підключення до бази даних
connection_name = "goit_mysql_db_katerynaa"


#DAGs, Operators

with DAG(
    "katerynaa_hw7_dag",
    default_args=default_args,
    schedule=None,              #інтервал виконання для DAGу, для навчального завдання запускатимемо вручну
    catchup=False,              #вимкнення запуску пропущених задач
    tags=["katerynaa"]          #тег для позначення DAGу
) as dag:

    #ініціалізація MySQL оператора для завдання створення таблиці
    create_table_task = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS katerynaa_medals (
        id INT PRIMARY KEY AUTO_INCREMENT,
        medal_type VARCHAR(20),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    #ініціалізація Python оператора для завдання вибору медалі
    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    #ініціалізація Python оператора розгалудження для завдання визначення, яке завдання виконуватимитесь наступним
    select_task_branch = BranchPythonOperator(
        task_id = "select_next_task",
        python_callable = select_next_task,
    )

    #ініціалізація MySQL операторів для завдань підрахунку кількості медалей відповідно до типу медалі
    count_bronze_task = MySqlOperator(
        task_id = "count_bronze_medals",
        mysql_conn_id=connection_name,
        sql = """
        INSERT INTO olympic_dataset.katerynaa_medals (medal_type, count)
        SELECT 'Bronze', COUNT(1)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )
    count_silver_task = MySqlOperator(
        task_id = "count_silver_medals",
        mysql_conn_id=connection_name,
        sql = """
        INSERT INTO olympic_dataset.katerynaa_medals (medal_type, count)
        SELECT 'Silver', COUNT(1)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )
    count_gold_task = MySqlOperator(
        task_id = "count_gold_medals",
        mysql_conn_id=connection_name,
        sql = """
        INSERT INTO olympic_dataset.katerynaa_medals (medal_type, count)
        SELECT 'Gold', COUNT(1)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    #ініціалізація Python оператора для завдання імітації затримки виконання
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=tr.ONE_SUCCESS,        #виконується, якщо хоча б одне попереднє завдання успішне
    )

    #ініціалізація SqlSensor оператора для перевірки, що останній запис було додано щонайбільше 30 секунд назад
    check_last_added_record_task = SqlSensor(
        task_id="check_last_added_record",
        conn_id=connection_name,
        sql="""
        SELECT *
        FROM olympic_dataset.katerynaa_medals
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode="poke",                    # Перевірка умови періодично
        poke_interval=10,               # Інтервал перевірки (10 секунд)
        timeout=30,                     # Тайм-аут перевірки (30 секунд)
    )

    #визначення послідовності виконання завдань
    create_table_task >> pick_medal_task >> select_task_branch
    select_task_branch >> [count_bronze_task, count_silver_task, count_gold_task]
    count_bronze_task >> delay_task
    count_silver_task >> delay_task
    count_gold_task >> delay_task
    delay_task >> check_last_added_record_task