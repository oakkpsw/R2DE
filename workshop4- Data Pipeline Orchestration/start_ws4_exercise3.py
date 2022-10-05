from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


with DAG(
    "exercise3_fan_in_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["exercise"]
) as dag:

    # Exercise3: Fan-in Pipeline
    # ใน exercise นี้จะได้รู้จักการเขียน task ใน pipeline ขั้นตอนเยอะขึ้น
    # ใช้ DummyOperator เป็น task จำลอง
      
    # TODO: สร้าง DummyOperator เพื่อสร้าง dependency ที่ซับซ้อน
    t0 = DummyOperator(task_id="task_0")
    t1 = DummyOperator(task_id="task_1")
    t2 = DummyOperator(task_id="task_2")
    t3 = DummyOperator(task_id="task_3")
    t4 = DummyOperator(task_id="task_4")
    t5 = DummyOperator(task_id="task_5")
    t6 = DummyOperator(task_id="task_6")

    [t0,t1,t2] >> t4
    [t3,t4,t5] >> t6
    # loop

    #t= [DummyOperator(task_id=f"task_{i}") for i in range(7)]
    # [t[0],t[1],t[2]] >> t[4]
    # [t[3],t[4],t[5]] >> t[6]
  
    