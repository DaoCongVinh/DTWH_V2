# apify_service.py
from apify_client import ApifyClient
from logging_setup import logger

def run_actor(actor_id, apify_token, run_input):
    """
    Gọi Apify actor và trả về (run_id, dataset_id, results).
    """
    logger.info("Calling Apify actor: %s", actor_id)
    client = ApifyClient(apify_token)
    run = client.actor(actor_id).call(run_input=run_input)
    
    dataset_id = run.get("defaultDatasetId")
    run_id = run.get("id")
    
    logger.info("Fetching results from dataset: %s", dataset_id)
    results = [item for item in client.dataset(dataset_id).iterate_items()]
    
    return run_id, dataset_id, results