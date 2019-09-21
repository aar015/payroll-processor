import io
import pandas
from googleapiclient import discovery
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload


class GoogleSheetIOStream(object):
    def __init__(self, service_account_file='config/creds.json'):
        scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
        self.drive_service = discovery.build('drive', 'v3', credentials=creds)
        self.sheet_service = discovery.build('sheets', 'v4', credentials=creds)

    def create_dir(self, name, parent=None, sharewith=[]):
        if parent is None:
            metadata = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
        else:
            metadata = {'name': name, 'parents': [parent['id']], 'mimeType': 'application/vnd.google-apps.folder'}
        folder = self.drive_service.files().create(body=metadata).execute()
        for person in sharewith:
            self.drive_service.permissions().create(fileId=folder['id'], body={'type': 'user', 'role': 'writer', 'emailAddress': person}).execute()
        return folder

    def get_file(self, name, parent=None):
        if parent is None:
            query = "name='{}'".format(name)
        else:
            query = "'{}' in parents and name='{}'".format(parent['id'], name)
        files = self.drive_service.files().list(q=query).execute().get('files')
        if len(files) == 0:
            return None
        if len(files) == 1:
            return files[0]
        raise Exception('Query does not uniquely identify file')

    def download_sheet(self, name, parent):
        sheet = self.get_file(name, parent)
        request = self.drive_service.files().export_media(fileId=sheet['id'], mimeType='text/csv').execute()
        if request == b'':
            return pandas.DataFrame()
        data = pandas.read_csv(io.BytesIO(request))
        return data

    def upload_sheet(self, data, name, parent=None, format=None, numlines=None):
        sheet = self.get_file(name, parent)
        if sheet is None:
            print('Creating New Google Sheet')
            if parent is None:
                metadata = {'name': name, 'mimeType': 'application/vnd.google-apps.spreadsheet'}
            else:
                metadata = {'name': name, 'parents': [parent['id']], 'mimeType': 'application/vnd.google-apps.spreadsheet'}
            data_stream = io.StringIO(data)
            media = MediaIoBaseUpload(data_stream, 'text/csv')
            self.drive_service.files().create(body=metadata, media_body=media).execute()
            data_stream.close()
        else:
            data_stream = io.StringIO(data)
            media = MediaIoBaseUpload(data_stream, 'text/csv')
            self.drive_service.files().update(fileId=sheet['id'], media_body=media).execute()
            data_stream.close()
        if format is not None:
            sheet = self.get_file(name, parent)
            sheetId = self.sheet_service.spreadsheets().get(spreadsheetId=sheet['id']).execute()['sheets'][0]['properties']['sheetId']
            format['requests'][0]['addBanding']['bandedRange']['range']['sheetId'] = sheetId
            if numlines is not None:
                format['requests'][0]['addBanding']['bandedRange']['range']['endRowIndex'] = numlines + 1
            self.sheet_service.spreadsheets().batchUpdate(spreadsheetId=sheet['id'], body=format).execute()


def run():
    io_stream = GoogleSheetIOStream()
    working_dir = io_stream.get_file('mng-payroll')
    bonus_dir = io_stream.create_dir('mng-bonus', working_dir)
    io_stream.create_dir('bonus-history', bonus_dir)


if __name__ == '__main__':
    run()
