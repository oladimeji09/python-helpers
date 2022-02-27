from python_helpers import google_helper as gh
from python_helpers import python_helper as ph


gh.find_files('google_helper')
root = r'C:\Users\oolao\github\Projects\personal\testing'
msg_body ="<html><body>Hi,<br /><br />Your order is available, please see attached.<br /> <br /> With best regards <br /> <b>MGMT </a> </body></html>"
Msg = gh.CreateMessage('dimejiola@msn.com',r"123.", msg_body)
gh.SendMessage(Msg)
gh.ModifyLabel(gh.FindMessage('hi')[0],'UNREAD','add')
print(gh.ReadMessage(gh.FindMessage('Unknown')[0]))
gh.gdrive_up('16N9yJ6x0vSpW-sfdpdsAmSQkcncnLHC8',file_name ='test.py',file_path =ph.root_fp+'python-helpers/test.py')

if __name__ == '__main__':
    main()
