

# Competitor Prices

- [Competitor Prices](#competitor-prices)
    - [Flower Matching](#flower-matching)
      - [How it Works](#how-it-works)
      - [iBuyFlowers Catalog](#ibuyflowers-catalog)
      - [Competitor Catalog](#competitor-catalog)
      - [Prompt](#prompt)
      - [Master Table](#master-table)
    - [Scraping \& Extracting](#scraping--extracting)
      - [Generic Helpers](#generic-helpers)
        - [ETA Fetching](#eta-fetching)
        - [Cleaning](#cleaning)
        - [BigQuery Export](#bigquery-export)
        - [Logging](#logging)
      - [Mayesh](#mayesh)
      - [FlowerMarketPlace](#flowermarketplace)
          - [Stem Price Extracting](#stem-price-extracting)
      - [Petaljet](#petaljet)
      - [DVFlora](#dvflora)
    - [Dashboard](#dashboard)
    - [Maintenance \& Future work](#maintenance--future-work)
        - [Farm Prices](#farm-prices)
        - [Inventory monitoring](#inventory-monitoring)
        - [Master Table](#master-table-1)
        - [Slack Notifications](#slack-notifications)

---
### Flower Matching

#### How it Works
To successfully match products across competitors, the process involves:
- Fetching and preparing a clean iBuyFlowers product catalog.
- Loading the competitor catalog.
- Preprocessing and identifying top_k potential matches using cosine similarity.
- Feeding the top_k candidates to the GPT model to select the most likely match.

#### iBuyFlowers Catalog
To improve the accuracy of productmatching it's crucial to present the model clean and well formated productnames. Previously we simply used ```variety_name``` and the respective ```variety_key```. However this was not enough and returned rather *"strange"* productnames like this:

```
- playa blanca (Sometimes have subtle blush hints)
- white
- mayras white (white to yellowish)
```



Therefore I created  `iBuyFlowers_catalog.sql` , a query specifically designed to return clean data. It basically combines variety_name, color and productgroup but also applies some other cleaning features. Lastly Bouquets and mixed boxes are excluded as they are too complex to match confidently.

```
iBuyFlowers_catalog.sql
```

<details>
<summary>Query Output </summary>

| **product_name**                                    | **variety**                               |
|:---------------------------------------------------|:------------------------------------------|
| Green Lucidum Foliage Viburnum                     | 9ad36cde-c1a3-4ea6-a8c8-9edaf3ca59d0     |
| White Cremon Magnum Chrysanthemum Disbuds/ Mums    | 95fb3da7-892d-4065-a25f-931574f6b4ad     |
| Pink Constancedavid Austin Garden Roses            | 2599e1c4-da18-4742-9317-1b08cb834f29     |
| White Elegance Ranunculus                          | f3014234-8c56-43ea-869c-f26b3b559fd5     |
| Pink Doncel Carnation                              | abc65461-0901-44ac-bb67-15c814a06de8     |
| ...                                                | ...     |

</details>
<br>

#### Competitor Catalog
Whenever competitors are adding new products it will be noticed in the Log files. To update the mapping 


<details>
<summary>Configure Competitor for matching</summary>

```python
COMPETITOR_NAME = "Mayesh"  # <-- Change the name here
INPUT_FILE = f"./Competitor_catalog/Catalog_{COMPETITOR_NAME}.csv"
OUTPUT_FILE = f"./Mappings/{COMPETITOR_NAME}_Mapped.csv"

CATALOG_FILE = f"./Catalog/ibuyflowers_catalog_{today}.pkl"
MATCHED_EMBEDDING_FILE = f"./Embeddings/ibuyflowers_{COMPETITOR_NAME}_embeddings.pkl"
```
</details>



#### Prompt

The prompt contains a few specific instructions tailored to matching as much products as possible. 
If it is not confident enough in picking a match it returns "NULL"
<details>


<summary>Current Prompt </summary>

```
You are a product matching assistant.
You receive a competitor product name and a list of 10 candidate matches from a master catalog.
These candidates were selected using cosine similarity over vector embeddings.
Your job is to select the single best match for the same flower product, based on the following logic:
Match Criteria:
- The same variety or cultivar name (e.g. 'Mondial', 'Patience', 'Freedom') is the strongest matching signal.
- Small differences in color naming (e.g. 'White' vs 'Cream') are acceptable when the variety clearly aligns.
- Differences in plural or word order (e.g. 'Rose' vs 'Roses') are okay.
- Words like 'Spray', 'Garden', or 'Standard' are meaningful product subtypes — these must be present or absent in both names, regardless of casing.
- it is possible that you receive the same product name multiple times, this is expected and you should return the same product name that you matched on previous occurences.
- if the competitor name is for example "Rose Freedom Red " and you mapped it on a match from the master catalog "Freedom Red Roses" dont map the next occurence of "Rose Freedom Red" on anything different then "Freedom Red Roses" 

- Reject generic matches if the competitor name is specific.
- Only match if there is a strong semantic overlap on variety/cultivar and product type.
- It is okay to return the same internal product for multiple competitor names if they refer to the same variety.

Do not:
- Match based on color or product type alone.
- Guess if the variety/cultivar is unclear or missing — in that case, respond with 'Null'.

You must return only one product name from the list below, copied exactly.
If no match is clearly correct, return 'Null'. Do not explain.    
```
</details>
<br>
This prompt is pretty long and is therefore loaded with a simple function to keep the code as clean as possible:

```python
# load_prompt function
def load_prompt(filename, **kwargs):
    prompt_path = os.path.join("Prompts", filename)
    with open(prompt_path, 'r') as f:
        prompt = f.read()
    return prompt.format(**kwargs)

    ...

# Using the prompt  
def gpt_select_best_match(competitor_name, candidates):
    system_prompt = load_prompt("variety_matching_prompt.txt",
        competitor_name=COMPETITOR_NAME)
```

#### Master Table
The Mastertable consists of a ```variety_key``` mapped to a ```competitor_product_id```.
The competitor_ids are from all the 4 competitors combined and currently the table contains roughly 3500 rows. 
In previous versions the productmatching was handled during the scraping process which required local files , seriously slowed down the scraping process and in general was not ideal.
Using the master table enables us to handle data way faster and handle the productmatching within BigQuery by a simple SQL JOIN statement.

**BigQuery**
```
SELECT * FROM `ibuyflower-dwh.competitor_product_mapping.competitor_product_mapping`
```
**Local:**

```
python -m utils.competitor_products_table
```
*(Outputs to ./Catalog/Current_master_table.csv)*

<details>
<summary>Preview </summary>

| **competitor_product_id**                                    | **variety_key**                               |
|:---------------------------------------------------|:------------------------------------------|
| 8152609227061                    | aed08d76-4782-479f-8426-9f93e180ebbe     |
| 8464046948661    | 6958ab90-871d-4d3e-be35-11a496e2118d     |
| 4548189847651            | 1b8586e0-db22-4ce5-8b3e-bb7e3b45f6dd    |
| 8379464843573                          | ec99c2c0-ef14-4960-bf13-48ac7a071792     |
| 8282599850293                             | 13dfb1bd-5e5e-479d-855e-590948213b0d    |
| ...                                                | ...     |



</details>

---

### Scraping & Extracting
Although competitor websites are structured differently, the scraping and extraction process is consistent across them.  
Each competitor has a dedicated **scraper** and **extractor** module, designed for modularity and easy maintenance.

#### Generic Helpers

Before diving into specific scrapers, here are essential helpers used throughout:
<BR>

##### ETA Fetching
```python
from utils.eta_fetcher import get_earliest_eta_date
```
The get_earliest_eta_date function logs into the Mayesh API, retrieves available delivery dates, and returns the earliest “Farm Direct” delivery date. This ETA is then used for the other competitors as well.
<br>

##### Cleaning

```python
from Cleaners.cleaner import clean_product_name, extract_stem_length, extract_stems_per_unit
```
These utilities standardize and enrich product data for accurate competitor matching purposes:

- **`clean_product_name`**: Cleans product names using two strategies:
  - **Safe**: Basic trimming and whitespace normalization.
  - **Aggressive**: Expands abbreviations (e.g., "Wht" → "White"), removes extraneous codes/units, and normalizes names (ideal for messy competitor data like DVFlora).
- By choosing `strategy="aggressive"` or `strategy="safe"`, when calling clean_product_name the cleaning level can be adapted to the data source. 
  - *Among the current competitors Aggresive is adviced for at least DVFlora*
<br>
- **`extract_stem_length`**: Extracts stem lengths from product names (e.g., "70/80cm", "7080") to support matching. In cases like these the first number is picked ( in this example 70)
- **`extract_stems_per_unit`**: Identifies the number of stems per unit from patterns like "10 stems/bunch" or "x5".

##### BigQuery Export
bigquery_export.py contains, per competitor specified fields. The reason why we have to list competitor specific fields is that for example Mayesh does show inventory and occasionaly grower names while others don't. 

```Python
BIGQUERY_FIELDS = {
    "mayesh": [
        "created_at", "eta_date", "state", "competitor", "grower_name", "grower_country",
        "competitor_product_id", "stem_length", "color_name", "available_units",
        "stems_per_unit", "stem_price", "margin", "unit_price", "unit", "competitor_product_name"
    ],
```
The relevant function in the helper is:
```Python
def export_products_to_bigquery(products, table_name):
```
This enables us to use the function like so in the ```mayesh_scraper.py``` :

```Python
if __name__ == "__main__":
    eta_date = get_earliest_eta_date()
    products = asyncio.run(fetch_mayesh_products(eta_date))
--> export_products_to_bigquery(products, table_name="mayesh")
```

While the export_to_bigquery function is synchronous its simply wrapped inside ``` asyncio.to_thread()``` in the main.py

where we call it like this:
```python
await asyncio.to_thread(export_products_to_bigquery, products, table_name)
```
and orchestrate all the scrapers like this:

```python
await asyncio.gather(
    run_scraper(fetch_mayesh_products, eta_date, "mayesh"),
    run_scraper(fetch_petaljet_products, eta_date, "petaljet"),
    run_scraper(fetch_flowermarketplace_products, eta_date, "flowermarketplace"),
    run_scraper(fetch_dvflora_products, eta_date, "dvflora")
  )
```
##### Logging
While previous versions didn't have any logging in place this one does. Relevant logs can be found inside the ```logging directory```. 





<details>
<summary>bigquery_export.log</summary>

```
2025-05-31 22:16:14,255 [INFO] utils.bigquery_export: ✅ Uploaded 3227 products to competitor_price.petaljet.
2025-05-31 22:16:16,564 [INFO] utils.bigquery_export: ✅ Uploaded 1617 products to competitor_price.mayesh.
2025-05-31 22:17:01,864 [INFO] utils.bigquery_export: ✅ Uploaded 1349 products to competitor_price.dvflora.
2025-05-31 22:17:20,044 [INFO] utils.bigquery_export: ✅ Uploaded 1446 products to competitor_price.flowermarketplace.
```
</details>

<details>
<summary>main_scraper.log</summary>

```
2025-05-31 22:16:07,694 [INFO] __main__: Starting mayesh scraping...
2025-05-31 22:16:07,713 [INFO] __main__: Starting petaljet scraping...
2025-05-31 22:16:07,724 [INFO] __main__: Starting flowermarketplace scraping...
2025-05-31 22:16:07,733 [INFO] __main__: Starting dvflora scraping...
```
</details>

---
#### Mayesh

The **Mayesh scraper** constructs a `JSON payload` to query the Mayesh API's inventory endpoint, specifying the needed filters:

```python
payload = {
    "filters": {
        "perPage": 2000,  #No pagination needed when setting like this         
        "sortBy": "Name-ASC",          
        "pageNumb": 1,                  
        "date": eta_date,              
        "is_sales_rep": 0,              
        "is_e_sales": 0,               
        "criteria": {"filter_program": ["5"]}
        "criteriaInt": {"filter_program": {"5": "Farm Direct Boxlots"}},  
        "search": ""  #Empty to return all products                   
    }
}
```
The **Mayesh extractor** (`extract_mayesh_product`) transforms raw product data into a structured format:

- Populates additional fields including `unit_price`, `margin`, `grower_name`, `grower_country` and `available_units`.
- Leverages Pydantic `ProductSchema` to enforce a standardized data structure, ensuring standardised output.

- This line calculates the product’s **margin percentage** from the markup value (visible in the JSON response):
```python
margin = (1 - (1 / raw["markup"])) * 100 if raw.get("markup") else None
```
---

#### FlowerMarketPlace
The **Flowermarketplace scraper** constructs a dynamic URL with query parameters to request product listings from the API:

``` python
url = f"https://flowermarketplace.com/wp-admin/admin-ajax.php?action=wpf_product_listings&model=landed&date_text={eta_date}&page_no={page}"
```

The **Flowermarketplace extractor** (`extract_flowermarketplace_product`) transforms raw product data into a standardized format:
- Applies **aggressive cleaning** to product names using `clean_product_name`.
- Extracts **stem length** and **stems per unit** using `extract_stem_length` and `extract_stems_per_unit`.
<br>

###### Stem Price Extracting 
The ```stem_price``` was unlike Mayesh not directly visible in the JSON. While we ofcourse could switch to using CSS Selectors, i'm not a big fan of Scraping data with Selectors as it will break the Scraper whenever the website structure is slightly adjusted. Besides that JSON is way faster and besides stem_price all other data we need **is** directly visible in the JSON. 


To understand the function lets look at the price_data within the JSON:


```JSON
{
  "id": 12345,
  "name": "Exotic Box",
  "prices_data": [
    {
      "selection": "one", <-- Default Option
      "price": "109.09",
      "fob_price": "109.09",
      "landed_price": "150.00",
      "stock": "10"
    },
    {
      "selection": "two",
      "price": "98.36",
      "fob_price": "98.36",
      "landed_price": "139.53",
      "stock": "10"
    },
    {
      "selection": "three", <-- Cheapest option
      "price": "92.31",
      "fob_price": "92.31",
      "landed_price": "127.66",
      "stock": "10"
    }
  ]
}
```
Now to extract the ```stem_price``` the function first tries to locate selection "three". If it finds it it will use it, when it cant find three it tries "one".


```python
    for selection in ["three", "one"]:
        for tier in prices_data:
            if tier.get("selection") == selection:
                try:
                    stem_price = float(tier.get("landed_price"))
                    break
                except (ValueError, TypeError):
                    continue
        if stem_price is not None:
            break

```
So far so good but occasionally there is no selection available. Now what? We simply handle it by dividing the amount of stems per unit by the unit_price. The extract_stems_per_unit function is called :
```python
 stems_per_unit = extract_stems_per_unit(product_name_raw) or 1
```
The fallback here is 1 as sometimes products don't specify amount of stems (e.g happens for big plants for example). In that case ```unit_price == stem_price ```which is the same as ```unit_price / 1```.

---
#### Petaljet
The **PetalJet scraper** constructs URLs to query paginated product JSON data:
```python
url = f"https://petaljet.com/collections/all-products/products.json?page={page_number}"
```
- It iterates through pages (1 to 22) using asynchronous requests with up to 5 concurrent requests (`CONCURRENT_REQUESTS`).
  - concurrency > 5 will cause blocks from the server

The **PetalJet extractor** extracts **stem price** directly from the variant title if formatted as `($x.xx Each)`, or calculates it by dividing `unit_price` by `stems_per_unit` if not present:
```python
if not stem_price and unit_price and stems_per_unit:
    stem_price = round(unit_price / stems_per_unit, 2)
```
Good to know here is that extracted products are **enriched with the ETA date**, as setting ETA doesn't change prices nor availability whatsoever.

---
#### DVFlora
The **DVFlora scraper** is by far the most difficult competitor to scrape. This is due too a messy website structure , without API and not even consistent HTML tables. 
Therefore this scraper works differently and reads product listing URLs from a CSV file and fetches product pages asynchronously:

```python
url = row["url"]
```

- Utilizes `aiohttp` with up to 50 concurrent requests to efficiently retrieve product HTML pages.
- Extracts table row data from each page using `selectolax` HTML parsing. The choice for `selectolax`is mainly to increase the speed.
- Applies filtering logic to exclude duplicate or incomplete product entries based on item numbers. Even with unique productlinks dvflora website contains duplicate products.
- Progress is tracked with a `tqdm` progress bar, and usually takes around 30 seconds to parse. 
- They tend to do server maintenance at 9:00 UTC lasting usually for an hour.

The **DVFlora extractor** (`extract_dvflora_product`) transforms raw product data into a structured format:
- Applies **aggressive cleaning** to product names using `clean_product_name`, and additional regex cleanup for trailing codes and embedded patterns. (90% of the cleaner.py is specifically written for this competitor)

---
### Dashboard

The end result is in the form of a dashboard that can be viewed [here](https://lookerstudio.google.com/reporting/dbd2e4be-8180-4625-b0c5-fc251dc85667)

![Alt Text](https://github.com/ibf-code/iBuyFlowers-competitor-price-scraping/blob/main/Examples/dashboard.png)

---
### Maintenance & Future work
While this project is set up in a way that adding more competitors is very easy, it's not really adding much value to do so. When looking at the current data it can be noticed that prices tend to be somewhat alligned accross the currently monitored competitors. 

##### Farm Prices
Currently we present purely price data vissible on the website aka the price customers are paying. However for some competitors this project is able to also see prices being set from the farm perspective meaning we could use this information as input to negotiate prices with farms ourselves.

##### Inventory monitoring
It is possible to see inventory levels per product per competitor (excluding Petaljet). We can use this in our advantage by increasing our price whenever competitors are out of inventory for a particular product.

##### Master Table 
Occasionally we have to refresh the ```competitor_product_table```. I expect once per month or even once every 2 months is fine. Keep an eye on the log files for sudden drops in products scraped to spot possible issues early on

##### Slack Notifications
Just like the daily sales slack bot , that sends a quick overview of the day before we can easily create something similar for this project. While we still have to decide how often we run the Scraper , I already created a working Slack notification function that is functional. Additionally if we decide to run the project daily, we can add aditional information e.g competitors running out of inventory for product xyz


```
2025-06-01 00:11:55,027 [INFO] utils.slack_notifier: Sending completion notification: Scraping Complete!
Total Products: 7,629
• Mayesh: 1,617
• Petaljet: 3,227
• Flowermarketplace: 1,446
• Dvflora: 1,339

Duration: 73.8s
```

