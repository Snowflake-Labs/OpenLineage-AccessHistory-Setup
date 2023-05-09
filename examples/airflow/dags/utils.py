from pendulum import now
from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun


def _get_execution_date_of(dag_id):
    @provide_session
    def _get_last_execution_date(exec_date, session=None, **kwargs):
        dag_a_last_run = get_last_dagrun(
            dag_id, session, include_externally_triggered=True
        )
        if not dag_a_last_run:
            return now()
        return dag_a_last_run.execution_date

    return _get_last_execution_date
