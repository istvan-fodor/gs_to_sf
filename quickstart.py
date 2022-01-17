from __future__ import print_function
import snowflake.connector
import os.path
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1lmseZ3wd4uXvwP1BBFZHkYRQ88Okrmqeum9kYGX_ENc'
RANGE_NAME = 'Sheet1'


def get_sheet(spreadsheet_id, range):
    """Extract Data from Google Sheets"""
    try:
        service = build('sheets', 'v4')

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range).execute()
        values = result.get('values', [])

        df = None
        if not values:
            print('No data found.')
        else:
            df = pd.DataFrame(data = values[1:], columns=values[0])

        return df
    except HttpError as err:
        print(err)
        raise err

def transform(df):
    """Transform"""

    return df

def load_to_store(df):
    
    conn = snowflake.connector.connect(
                user='ifodor85',
                password='efn.fyz0ycg5nyb_ZUJ',
                account='zs98740',
                warehouse='COMPUTE_WH',
                database='MY_DB',
                schema='PUBLIC'
                )
    
    stmt = """
    MERGE INTO 
      records r1 
    USING 
        (select ? as id, ? as name, ? as address) r2
    ON r1.id = r2.id 
    WHEN MATCHED THEN 
        UPDATE SET 
            r1.name = r2.name
          , r1.address = r2.address
    WHEN NOT MATCHED THEN
        INSERT (id, name, address) 
        VALUES (r2.id, r2.name, r2.address)
    """
    
    rows_to_insert = df.values
    conn.cursor().executemany(stmt, rows_to_insert)



def main():
    try :
        df = get_sheet()
        df = transform(df)
        load(df)
    except Exception as err:
        print("Unable to complete ETL flow")
        print(err)
        exit(1)


if __name__ == '__main__':
    main()