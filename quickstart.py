from __future__ import print_function

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

def load(df):
    

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