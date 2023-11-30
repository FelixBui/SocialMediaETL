import os
import glob
from airflow.models import DagBag

def test_dag_syntax():
    dagbag = DagBag()
    dag_files = glob.glob(os.path.join(os.path.dirname(__file__), '*.py'))
    failed_dags = []
    
    for dag_file in dag_files:
        file_name = os.path.basename(dag_file)
        dag_name = os.path.splitext(file_name)[0]
        if not dagbag.get_dag(dag_name):
            failed_dags.append(dag_name)
    
    if failed_dags:
        print("Syntax check failed for the following DAGs:")
        for dag_name in failed_dags:
            print("- " + dag_name)
    else:
        print("All DAGs passed syntax check!")

test_dag_syntax()
