import pytest

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="Fetch_and_Upload_Data_to_S3")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1
