from dagster import asset, AssetExecutionContext
import requests
from typing import List

@asset(config_schema={"intercom_api_token": str})
def fetch_company_names_from_intercom(context: AssetExecutionContext):
    """
    This fetches data from Intercom API
    """
    api_token = context.solid_config["intercom_api_token"]
    url = "https://api.intercom.io/companies"
    
    headers = {
        "Authorization": f"Bearer dG9rOmQyMjJmMzRkXzRlYTZfNDEyZl9iNzIzXzYyOGY5ZjllZmViYzoxOjA=",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    companies = response.json()["companies"]
    company_names = [company["name"] for company in companies]

    context.log.info(f"Fetched {len(company_names)} company names from Intercom.")
    return company_names


# @asset
@asset(deps=[fetch_company_names_from_intercom])
def my_second_asset(context: AssetExecutionContext):
    """
    This is the second asset
    """
    data = [4,5,6]
    context.log.infor(f"Output data is: {data}")
    return data




















# @asset#(required_resource_keys={"snowflake"})
# def load_company_names_to_snowflake(context: AssetExecutionContext, fetch_company_names_from_intercom: List):
#     """
#     This loads data into Snowflake
#     """
#     conn = context.resources.snowflake

    # Assuming you have a table named 'intercom_companies' with a column 'company_name'
    # create_table_query = """
    # CREATE TABLE IF NOT EXISTS dg_mode_intercom_companies (
    #     company_name STRING
    # );
    # """

    # insert_query = """
    # INSERT INTO dg_mode_intercom_companies (company_name) VALUES (%s);
    # """

    # with conn.cursor() as cursor:
    #     cursor.execute(create_table_query)
    #     cursor.executemany(insert_query, [(name,) for name in company_names])

    # context.log.info(f"Loaded {len(company_names)} company names into Snowflake.")

    
    # context.log.info(f"Loaded  company names into Snowflake.")

    # return "this is second asset"

# if __name__ == "__main__":
#     run_config = {
#         "solids": {
#             "fetch_company_names_from_intercom": {
#                 "config": {
#                     "intercom_api_token": "Bearer dG9rOmQyMjJmMzRkXzRlYTZfNDEyZl9iNzIzXzYyOGY5ZjllZmViYzoxOjA="
#                 }
#             }
#         },
#         "resources": {
#             "snowflake": {
#                 "config": {
#                     "account": "THOUGHTSPOT_ANALYTICS",
#                     "user": "ETL",
#                     "password": "th0ughtSp0t",
#                     "database": "STAGING",
#                     "schema": "PUBLIC",
#                     "warehouse": "GTM_WH_WRITE",
#                 }
#             }
#         }
#     }