# Guide: https://medium.com/analytics-vidhya/how-to-connect-google-drive-to-python-using-pydrive-9681b2a14f20
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime
import schedule
import time


class BaoLong:
    def __init__(self, folder_name):
        self.folder_name = folder_name
        self.folder_id = None
        self.backup_number = 5
        self.upload_file_name = datetime.now().strftime("BAK_%Y_%m_%d_%H_%M_%S") + '.sql'

        gauth = GoogleAuth()
        if "mycreds.txt" in os.listdir():
            gauth.LoadCredentialsFile("mycreds.txt")
        else:
            gauth.LocalWebserverAuth()

        self.drive = GoogleDrive(gauth)

        if "mycreds.txt" not in os.listdir():
            print('Saved credential.')
            gauth.SaveCredentialsFile("mycreds.txt")

    def drive_trashing(self):
        file_list = sorted(self.drive_file_list(), key=lambda k: k['createdDate'], reverse=True)
        for f in file_list[self.backup_number:]:
            file = self.drive.CreateFile({'id': f['id']})
            file.Trash()

    def drive_upload(self, file_path=None):
        file_path = file_path if file_path is not None else self.upload_file_name
        file = self.drive.CreateFile({'parents': [{'id': self.folder_id}],
                                      'title': self.upload_file_name})
        file.SetContentFile(file_path)
        file.Upload()

        self.drive_check_upload_successfully(self.upload_file_name)

    def drive_file_list(self):
        file_list = self.drive.ListFile({'q': f"'{self.folder_id}' in parents and trashed=false"}).GetList()
        return [{'title': f['title'],
                 'id': f['id'],
                 'createdDate': datetime.fromisoformat(f['createdDate'][:-1]).timestamp()}
                for f in file_list]

    def drive_check_upload_successfully(self, file_name):
        file_list_info = self.drive_file_list()
        if file_name in [i['title'] for i in file_list_info]:
            print('.')
        else:
            print('Error. Pls check it again!')

    def drive_folder_create(self):
        if self.drive_get_folder_id() is None:
            folder = self.drive.CreateFile(
                {'title': self.folder_name, "mimeType": "application/vnd.google-apps.folder"})
            folder.Upload()
            self.drive_get_folder_id()
            print('Create new Folder.')

    def drive_get_folder_id(self):
        folders = self.drive.ListFile({'q': f"title = '{self.folder_name}' and trashed=false"}).GetList()
        if len(folders):
            self.folder_id = folders[0]['id']
            return True
        else:
            return None

    def local_database_bak(self):
        backup_command = 'PostgreSQLdump -u root -p TeamWorks'
        backup_command = 'ls'
        export_folder = os.path.join(os.getcwd(), self.upload_file_name)
        os.system(f'{backup_command} > {export_folder}')

        return self.local_check_backup_successfully()

    def local_check_backup_successfully(self):
        for i in range(5):
            if self.upload_file_name in os.listdir():
                return True
            time.sleep(2)
        else:
            return False

    def local_remove_bak(self):
        os.remove(self.upload_file_name)


def job():
    bl = BaoLong(folder_name='BAOLONG_DATABASE_BK')
    bl.local_database_bak()
    bl.drive_folder_create()
    bl.drive_upload()
    bl.drive_trashing()
    bl.local_remove_bak()


# schedule.every().day.at("23:30").do(job)
schedule.every().minute.at(":30").do(job)

while True:
    schedule.run_pending()
    time.sleep(10)
