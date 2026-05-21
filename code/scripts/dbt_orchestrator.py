from dbt.cli.main import dbtRunner, dbtRunnerResult
from tenacity import retry, wait_exponential, stop_after_attempt
import logging, google.cloud.logging, uuid
from pathlib import Path

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def dbt_run(dbt, args):
    res: dbtRunnerResult = dbt.invoke(args)

    if not res.success:
        raise res.exception

def execute_transforms(logging_context):
    logging_context["pipeline_phase"] = "dbt_transforms"
    logging.info(f"Beginning dbt transforms.", extra={"json_fields":logging_context})

    dbt = dbtRunner(callbacks=[])

    current_script_path = Path(__file__).resolve()
    dbt_project_dir = str(current_script_path.parent.parent / "fpl_dbt")

    dbt_args = ["--log-level", "error", "run", "--project-dir", dbt_project_dir, "--profiles-dir", dbt_project_dir]
    
    try:
        dbt_run(dbt, dbt_args)

    except Exception:
        logging.critical(
        f"Couldn't complete dbt transforms. Stopping pipeline.",
        exc_info=True,
        extra={"json_fields":logging_context}
    )
        return
    
    logging_context["pipeline_phase"] = "complete"
    logging.info(f"Dbt transforms complete.", extra={"json_fields":logging_context})

def main(logging_context):
    execute_transforms(logging_context)
    logging.shutdown()

if __name__ == "__main__":
    main()