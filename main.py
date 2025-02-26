import requests as rq, pandas as pd, snowflake.connector as sc, jsonpath_ng as jp, os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

database_id     = os.getenv("DATABASE_ID")
bearer_token    = os.getenv("BEARER_TOKEN")
sf_user         = os.getenv("USER")
sf_password     = os.getenv("PASSWORD")
sf_account      = os.getenv("ACCOUNT")
sf_warehouse    = os.getenv("WAREHOUSE")
sf_database     = os.getenv("DATABASE")
sf_schema       = os.getenv("SCHEMA")
table_name      = "data_eng_events_raw"

def extract_notion_details(notion_data):
    parsed = []

    for page in notion_data:

        details = {
            "id":   page.get("id"),
            "name": "$.properties.Name.title[*].text.content",
            "date": "$.properties.Date.date.start",
            "time": "$.properties.Time.rich_text[*].text.content",
            "url":  "$.properties.Link.url"
        }

        for field, exp in list(details.items())[1:]:

            expr = jp.parse(exp)
            match = expr.find(page)
            details[field] = match[0].value if match else None

        parsed.append(details)
        
    return parsed

url = f"https://api.notion.com/v1/databases/{database_id}/query/"

headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

print("Pulling data from API...\n")

response = rq.post(url, headers=headers).json()

notion_pages = response.get("results", [])

outputs = extract_notion_details(notion_pages)

df = pd.DataFrame(outputs)

print("Dataframe created. See below:")

print(df)

conn = sc.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

print("\nDropping landing table...\n")

drop_query = f'DROP TABLE IF EXISTS "{table_name}"'
conn.cursor().execute(drop_query)

print(f"Dropped. Writing to Snowflake as {sf_database.lower()}.{sf_schema.lower()}.{table_name}...\n")

write_pandas(conn, df, table_name, auto_create_table=True)

count_query = f'SELECT COUNT(*) FROM "{table_name}"'
count = conn.cursor().execute(count_query).fetchone()[0]

print(f"Finished creating {sf_database.lower()}.{sf_schema.lower()}.{table_name} with {count} rows. Setting grants...\n")

grant_query = f"GRANT SELECT ON ALL TABLES IN SCHEMA {sf_schema} TO ROLE CORE"
grant = conn.cursor().execute(grant_query)

print("Done!")

conn.close()
