# gs_to_sf

Test Google Sheets to Snowflake ETL.

## Setup
1. Clone
1. `pip install -r requirements.txt`
1. Create the target table in Snowlake, see DDL in `init.sql`
1. Get a Google Service Account https://cloud.google.com/iam/docs/creating-managing-service-accounts and save it to google_credentials.json. It should look similar to `google_credentials.example.json`.
1. Get your Snowflake creds into a file, similarly to `snowflake_credentials.example.json`
1. Run `run.sh`
