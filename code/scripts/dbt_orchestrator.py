from dbt.cli.main import dbtRunner, dbtRunnerResult
from tenacity import retry, wait_exponential, stop_after_attempt
import logging, google.cloud.logging, uuid
from pathlib import Path

def initialise_logging():
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10), reraise=True)
def dbt_run(dbt, args):
    res: dbtRunnerResult = dbt.invoke(args)

    if not res.success:
        raise res.exception

def execute_transforms():
    RUN_ID = str(uuid.uuid4())

    logging_context = {
        "run_id" : RUN_ID,
        "pipeline_phase" : "dbt_transforms"
    }

    logging.info(f"Beginning dbt transforms.", extra={"json_fields":logging_context})

    dbt = dbtRunner(callbacks=[])
    model_names = ["staging", "curated", "prod"]

    current_script_path = Path(__file__).resolve()
    dbt_project_dir = str(current_script_path.parent.parent / "fpl_dbt")

    for model in model_names:
        dbt_args = ["--log-level", "error", "run", "--project-dir", dbt_project_dir, "--profiles-dir", dbt_project_dir, "--select", model]
        try:
            dbt_run(dbt, dbt_args)

        except Exception:
            logging.critical(
            f"Couldn't complete transform for {model} data. Stopping pipeline.",
            exc_info=True,
            extra={"json_fields":logging_context}
        )
            return
    
    logging_context["pipeline_phase"] = "complete"
    logging.info(f"Dbt transforms complete.", extra={"json_fields":logging_context})

def main():
    initialise_logging()
    execute_transforms()
    logging.shutdown()

if __name__ == "__main__":
    main()