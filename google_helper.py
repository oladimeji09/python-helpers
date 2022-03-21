#@auto-fold regex /./
from __future__ import print_function
import pickle, email, os.path,  mimetypes,os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import base64,io
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from httplib2 import Http
from apiclient import errors
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from python_helpers import python_helper as ph

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://mail.google.com/','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/calendar']

def main(service):
    """Use this for the different google api   """
    creds = None
    if os.path.exists(ph.root_fp+'creds/token.pickle'):
        with open(ph.root_fp+'/creds/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                ph.root_fp+'creds/client_secret.json', SCOPES)
            creds = flow.run_local_server(port=4785)
        # Save the credentials for the next run
        with open(ph.root_fp+'creds/token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    if service == 'gmail':
        v = 'v1'
    else:
        v ='v3'
    serviced = build(service, v, credentials=creds)
    return serviced

def CreateMessage(to,subject,message_text,cc='',bbc='',attachments=''):
      """Create Message object"""
      mimeMessage = MIMEMultipart()
      mimeMessage['to'] = to
      mimeMessage['from'] = 'me'
      mimeMessage['cc'] = cc
      mimeMessage['bcc'] = bbc
      mimeMessage['subject'] = subject
      mimeMessage.attach(MIMEText(message_text, 'html'))

      for attachment in attachments: #list of file_paths
          content_type, encoding = mimetypes.guess_type(attachment)
          main_type, sub_type = content_type.split('/', 1)
          file_name = os.path.basename(attachment)
          f = open(attachment, 'rb')

          myFile = MIMEBase(main_type, sub_type)
          myFile.set_payload(f.read())
          myFile.add_header('Content-Disposition', 'attachment', filename=file_name)
          email.encoders.encode_base64(myFile)
          f.close()
          mimeMessage.attach(myFile)

      return {'raw': base64.urlsafe_b64encode(mimeMessage.as_bytes()).decode()}

def SendMessage(message):
    """Send CreateMessage object"""
    message = (main('gmail').users().messages().send(userId='me', body=message).execute())

def FindMessage(query):
    """Return a list of message IDs searched"""
    resp = main('gmail').users().messages().list(userId='me' ,maxResults = 500, q= '{}'.format(query)).execute()
    try:
        ids = [item['id'] for item in resp['messages']]
        while 'nextPageToken' in resp:
            resp = main('gmail').users().messages().list(userId='me' ,maxResults = 500, pageToken = resp['nextPageToken']).execute()
            ids.extend([item['id'] for item in resp['messages']])
        return ids
    except:
        print('No messages found, try again!')

def ReadMessage(id,path=''):
    """Returns message body and downloads attachment if available"""
    resp = main('gmail').users().messages().get(userId='me', id= id, format="full").execute()
    subject = [i['value'] for i in resp.get("payload").get("headers") if i["name"]=="Subject"]
    files = []
    body_str= ''
    for part in resp.get("payload")['parts']:
        if part.get('mimeType') == 'text/plain':
            body_str =(base64.urlsafe_b64decode(part.get('body').get('data').encode("ASCII")).decode("utf-8"))
        if part.get('mimeType') == 'multipart/alternative':
            for part in part.get('parts'):
                if part.get('mimeType') == 'text/plain':
                    body_str =(base64.urlsafe_b64decode(part.get('body').get('data').encode("ASCII")).decode("utf-8"))

        if  'attachmentId' in part['body']:
            att = main('gmail').users().messages().attachments().get(userId='me',
                                    messageId=id,id=part['body']['attachmentId']).execute()
            data = base64.urlsafe_b64decode(att['data'].encode('UTF-8'))
            files.append(part['filename'])
            with open(os.path.join(path+part['filename']), 'wb') as f:
                        f.write(data)
            body_str = subject[0] +'\n\n' +body_str +'\n\n{} was downloaded into the local directory'.format(part['filename'])
    print(body_str)
    return body_str,files

def ModifyLabel(id,label,type):
    """Add or remove labels from emails"""
    respd = main('gmail').users().messages().modify(userId='me', id= id, body = {"addLabelIds" if type == 'add' else "removeLabelIds": [label]}).execute()
    return True

def create_folder(name,sub=None,sub2=None):
    """Create a new folder if it doesn't exists in google drive"""
    main('drive').files().emptyTrash().execute()
    page_token = None
    fd_name = sub2 if sub2 is not None else sub if sub is not None else  name
    response =  main('drive').files().list(q="name = '{}' ".format(fd_name.replace("'",r"\'")),
                                          spaces='drive',
                                          fields='nextPageToken, files(id, name, mimeType,parents)',
                                          pageToken=page_token).execute()
    # TODO: complete this...find the name of the parent folder then search in the parent folder for the sup folder names  'appDataFolder' in parents
    #https://developers.google.com/drive/api/v3/search-files
    if len(response['files']) > 0: # if folder already exists
        print('Folder already exists: %s id: %s' % (response['files'][0]['name'], response['files'][0]['id']))
        return(response['files'][0]['id'])
    else:
        file_metadata = {'name': name,'mimeType': 'application/vnd.google-apps.folder'}
        file = main('drive').files().create(body=file_metadata, fields='id').execute()
        if sub:
            file_metadata = {'name': sub,'mimeType': 'application/vnd.google-apps.folder', 'parents': [file.get('id')]}
            file = main('drive').files().create(body=file_metadata, fields='id').execute()
        if sub2:
            file_metadata = {'name': sub2,'mimeType': 'application/vnd.google-apps.folder', 'parents': [file.get('id')]}
            file = main('drive').files().create(body=file_metadata, fields='id').execute()
        return(file.get('id'))

def check_if_folder_exists(folder_name):
    """Check if a folder exists in the gogole drive"""
    resp = main('drive').files().list(q="mimeType='application/vnd.google-apps.folder' and name= '{}'".format(folder_name),
                             spaces='drive',
                             fields='nextPageToken, files(id, name)').execute()
    return resp.get('files')

def gdrive_up(f_id,file_name,file_path):
    """Upload to google drive API using the folder ID"""
    main('drive').files().emptyTrash().execute()
    resp =  main('drive').files().list(q="name = '{}' and '{}' in parents ".format(file_name.replace("'",r"\'"),f_id),
                                          spaces='drive',
                                          fields='nextPageToken, files(id, name, mimeType,parents)').execute()
    if len(resp['files']) > 0 : # if the file name doesn't exists the same parent
        print('File {} already exists in folder {}'.format(file_name, f_id))
        print(resp['files'])
    else:
        folder_id = f_id
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]}
        media = MediaFileUpload(file_path,
                                #mimetype='image/png',
                                resumable=True)
        file = main('drive').files().create(body=file_metadata,
                                            media_body=media,
                                            fields='id').execute()

def find_files(name):
    """Find files with a specfic name and file type"""
    page_token = None
    files = []
    while True:
        response = main('drive').files().list(q="name contains '{}' and  trashed = false ".format(name),
                                              spaces='drive',
                                              fields='nextPageToken, files(id,name,mimeType)',
                                              pageToken=page_token).execute()
        for file in response.get('files', []):
            # Process change
            files.append([file.get('name'),'.'+file.get('mimeType')[-3:], file.get('id')])
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
    return files

def download_file(file_name,file_id,location=''):
    """Download files using the file ID"""
    request = main('drive').files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fd=fh, request=request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
        fh.seek(0)
        with open(os.path.join(ph.root_fp+'working_files/'+location,file_name),'wb') as f:
            f.write(fh.read())
            f.close()

def gspread_con():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(ph.root_fp+'creds/svc-holmes.json', scope)
    gc = gspread.authorize(credentials)
    return gc

def open_wb(id):
    """ open workbook"""
    wb = gspread_con().open_by_key(id)
    return wb

def num_to_col_letters(num):
    """numbers to columns"""
    letters = ''
    while num:
        mod = (num - 1) % 26
        letters += chr(mod + 65)
        num = (num - 1) // 26
    return ''.join(reversed(letters))

def rep_data_sh(df,wb_id,sh_name):
    import gspread_dataframe as gd, gspread_formatting as f
    """Replace data in sheet with dataframe, if sheet doesn't exists then create a new sheet"""
    top_row_fmt = f.cellFormat(backgroundColor= f.color(1,1,1),#https://tug.org/pracjourn/2007-4/walden/color.pdf
    textFormat=f.textFormat(bold=False, foregroundColor=f.color(0,0,0)))
    if sh_name in str(open_wb(wb_id).worksheets()):
        sheet = open_wb(wb_id).worksheet(sh_name)
        f.format_cell_range(sheet, 'A1:{}{}'.format(num_to_col_letters(df.shape[1]),df.shape[0]), top_row_fmt) #remove formats
    else:
        sheet = open_wb(wb_id).add_worksheet(sh_name, df.shape[0],df.shape[1])
    f.set_frozen(sheet, rows=0) #Remove top row formats and unfreeze top row
    sheet.resize(rows=1) #delete all row
    gd.set_with_dataframe(sheet, df) #insert data
    sheet.set_basic_filter()
    new_fmt =  top_row_fmt+ f.cellFormat(backgroundColor=f.color(1,3.5,454))
    f.format_cell_range(sheet, 'A1:{}1'.format(num_to_col_letters(df.shape[1])), new_fmt)
