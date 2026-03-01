from apify_client import ApifyClient
import os
from dotenv import load_dotenv
load_dotenv()

# Initialize the ApifyClient with your API token
client = ApifyClient(os.getenv("APIFY_TOKEN"))

# Prepare the Actor input
run_input = {
    "keyword": "Vinted",
    "limit": 10,
    "sort": "relevance",
    "time_filter": "day",
}

# Run the Actor and wait for it to finish
run = client.actor("2aTxJQei6EYjQsD9A").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    print(item)