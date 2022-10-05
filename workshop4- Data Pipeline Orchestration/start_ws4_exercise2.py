import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datath',
}


def my_function(something: str):
    print(something)


with DAG(
    "exercise2_fan_out_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["exercise"]
) as dag:
    
    # Exercise2: Fan-out Pipeline
    # ใน exercise นี้จะได้รู้จักกับการแยก pipeline ออกเพื่อให้ทำงานแบบ parallel พร้อมกันได้
    # และทดลองใช้คำสั่ง gsutil จาก BashOperator

    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=my_function,
        op_kwargs={"something": "Hello World!"},
    )

    t2 = BashOperator(
        task_id="print_date",
        bash_command="echo $(date)",
    )

    #TODO: ใส่ task t3 สำหรับ list ไฟล์ใน GCS bucket ที่เป็น DAGs folder
    #hint: ใช้ BashOperator ร่วมกับ command gsutil
    t3 = BashOperator(
        task_id="list_file_gcs",
        bash_command="gsutil ls gs://asia-east2-workshop4-e1bef6db-bucket",
    )

    
    
    # TODO: ใส่ task dependencies ที่ทำให้รัน t3 พร้อมกับ t2 ได้

    t1 >> [t2,t3]
    
