import json
import logging
from dbt.cli.main import dbtRunner, dbtRunnerResult
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def merge(temp_table, target_table, model_name):
    dbt_vars = {
        "input_table" : temp_table,
        "output_table" : target_table
    }

    dbt = dbtRunner()
    dbt_args = ["run", "--project-dir", "fpl_dbt", "--select", model_name, "--vars", json.dumps(dbt_vars)]

    res: dbtRunnerResult = dbt.invoke(dbt_args)

    if not res.success:
        raise res.exception
