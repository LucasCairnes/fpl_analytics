from scripts.load_fpl_api_data import main as run_fpl_api_extraction
from scripts.load_player_data import main as run_player_extraction
from scripts.dbt_orchestrator import main as run_dbt_transforms

api_extraction = run_fpl_api_extraction()
player_extraction = run_player_extraction()

if api_extraction or player_extraction:
    run_dbt_transforms()