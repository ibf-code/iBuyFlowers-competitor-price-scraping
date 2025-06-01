import os
import datetime
import pickle
from openai import OpenAI
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
bq_client = bigquery.Client()

COMPETITOR_NAME = "old_Flowermarketpace" # Change this to the name of the competitor you want to match
INPUT_FILE = f"./Competitor_catalog/Catalog_{COMPETITOR_NAME}.csv"
OUTPUT_FILE = f"./Mappings/{COMPETITOR_NAME}_Mapped.csv"

today = datetime.datetime.now().strftime("%Y-%m-%d")
CATALOG_FILE = f"./Catalog/ibuyflowers_catalog_{today}.pkl"
MATCHED_EMBEDDING_FILE = f"./Embeddings/ibuyflowers_{COMPETITOR_NAME}_embeddings.pkl"

# if the directories for whatever reason do not exist, this will create them
os.makedirs("./Training_data", exist_ok=True)
os.makedirs("./Mappings", exist_ok=True)
os.makedirs("./Embeddings", exist_ok=True)
os.makedirs("./Catalog", exist_ok=True)
os.makedirs("./Queries", exist_ok=True)
os.makedirs("./Prompts", exist_ok=True)

# Function to load prompts from files
def load_prompt(filename, **kwargs):
    prompt_path = os.path.join("Prompts", filename)
    with open(prompt_path, 'r') as f:
        prompt = f.read()
    return prompt.format(**kwargs)

## To prevent static csv files, we load a query that generates the catalog dynamically
# initiate the directory for queries
def load_query(filename):
    query_path = os.path.join("Queries", filename)
    with open(query_path, 'r') as f:
        return f.read()

# Initialize the BigQuery client and run the query to load the iBuyFlowers catalog
def load_ibuyflowers_catalog():
    query = load_query("iBuyFlowers_catalog.sql")
    return bq_client.query(query).to_dataframe()

print(f"Loading fresh iBuyFlowers catalog...")
# Return the query output to a dataframe
ibuyflowers_df = load_ibuyflowers_catalog()
print(f"Loaded {len(ibuyflowers_df)} iBuyFlowers products")

# Now after creating a dataframe with the iBuyFlowers catalog, we can load the competitor data
print(f"Loading competitor data for {COMPETITOR_NAME}...")
try:
    competitor_df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(competitor_df)} {COMPETITOR_NAME} products")
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit(1)

product_name_to_variety = {
    row["product_name"]: row["variety"]
    for _, row in ibuyflowers_df.iterrows()
}
ibuyflowers_products = ibuyflowers_df["product_name"].tolist()

if os.path.exists(CATALOG_FILE):
    print(f"Load existing embeddings from {CATALOG_FILE}...")
    with open(CATALOG_FILE, 'rb') as f:
        ibuyflowers_embeddings = pickle.load(f)
else:
    print("No recent embeddings found, generating new ones..")
    def embed_products(texts, model="text-embedding-3-small", batch_size=50):
        embeddings = []
        for i in tqdm(range(0, len(texts), batch_size)):
            batch = texts[i:i + batch_size]
            response = client.embeddings.create(
                model=model,
                input=batch
            )
            batch_embeddings = [e.embedding for e in response.data]
            embeddings.extend(batch_embeddings)
        return np.array(embeddings)
    
    ibuyflowers_embeddings = embed_products(list(ibuyflowers_products))

    with open(CATALOG_FILE, 'wb') as f:
        pickle.dump(ibuyflowers_embeddings, f)
    print(f"Fresh catalog embedding save to {CATALOG_FILE}")      
 
cache = {}
def get_matches_from_cache(query,candidates):
    key = (query, tuple([c[0] for c in candidates]))
    if key in cache:
        return cache[key]
    result = gpt_select_best_match(query, candidates)
    cache[key] = result
    return result
    
def get_top_k_matches(query, k=10):
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=[query]
    )
    query_embedding = np.array([response.data[0].embedding])
    scores = cosine_similarity(query_embedding, ibuyflowers_embeddings)[0]
    top_k_idx = scores.argsort()[::-1][:k]
    return [(ibuyflowers_products[i], scores[i]) for i in top_k_idx]

def gpt_select_best_match(competitor_name, candidates):
    system_prompt = load_prompt("variety_matching_prompt.txt",
        competitor_name=COMPETITOR_NAME)
    
    candidate_list = "\n".join([f"- {name}" for name, _ in candidates])
    user_prompt = (
        f"The competitor {COMPETITOR_NAME} sells: \"{competitor_name}\".\n\n"
        f"From the list below, which is the best match?\n{candidate_list}\n\n"
        f"Only reply with the best match or 'Null'."
    )
    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"GPT failed for {competitor_name}: {e}")
        return "Null"

results = []
print("matching products")
for row in tqdm(competitor_df.itertuples(), total=len(competitor_df)):
    top_k = get_top_k_matches(row.product_name, k=10)
    best_match = get_matches_from_cache(row.product_name, top_k)
    results.append({
        "competitor_product_id": row.competitor_product_id,
        "competitor": row.product_name,
        "matched_ibuyflowers": best_match,
        "variety_key": product_name_to_variety.get(best_match, "Null")
    })
with open(MATCHED_EMBEDDING_FILE, "wb") as f:
    pickle.dump(results, f)

pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
print(f"Matching completed. Results saved to {OUTPUT_FILE} and embeddings to {MATCHED_EMBEDDING_FILE}")
print(f"Total matches found for {COMPETITOR_NAME}: {len(results)}")

