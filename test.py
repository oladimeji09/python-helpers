import google_helper as gp

gp.find_files('google_helper','')
root = r'C:\Users\oolao\github\Projects\personal\testing'
msg_body ="<html><body>Hi,<br /><br />Your order is available, please see attached.<br /> <br /> With best regards <br /> <b>MGMT </a> </body></html>"
Msg = CreateMessage('dimejiola@msn.com',r"for educational purposes only.", msg_body)
SendMessage(Msg)
ModifyLabel(FindMessage('Unknown Investor')[0],'UNREAD','add')
print(ReadMessage(FindMessage('Unknown Investor')[0]))
gdrive_up('16N9yJ6x0vSpW-sfdpdsAmSQkcncnLHC8',file_name ='google_helper.py',file_path =r'C:\Users\oolao\github\python-helpers\google_helper.py')

if __name__ == '__main__':
    main()
