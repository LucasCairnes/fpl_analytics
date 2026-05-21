import logging, google.cloud.logging, uuid
import sys

from scripts.load_fpl_api_data import main as run_fpl_api_extraction
from scripts.load_player_data import main as run_player_extraction
from scripts.dbt_orchestrator import main as run_dbt_transforms

def main():
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

    logging.getLogger("pandas_gbq").setLevel(logging.WARNING)

    RUN_ID = str(uuid.uuid4())

    logging_context = {
        "run_id" : RUN_ID,
        "pipeline_phase" : "initialising"
    }

    api_success = run_fpl_api_extraction(logging_context)
    player_success = run_player_extraction(logging_context)

    if not api_success or not player_success:
        logging.error("One or more pipelines failed. Stopping pipeline.")
        logging.shutdown()
        sys.exit(1)

    dbt_success = run_dbt_transforms(logging_context)

    if not dbt_success:
        logging.shutdown()
        sys.exit(1)

    logging.info("Pipeline finished successfully.")
    logging.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    main()