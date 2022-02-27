#@auto-fold regex /./
import json,os,boto3,psycopg2,shutil
import gspread_dataframe as gd, gspread_formatting as f, gspread,requests as r
from oauth2client.service_account import ServiceAccountCredentials
from datetime import  datetime,timedelta

if os.name == "nt":
    root_fp =  r"C:/Users/oolao/github/"
elif os.name == "posix":
    root_fp = r"/home/oolao/github/"

creds = json.load(open(root_fp+'creds/creds.json'))

def now(type):
    """Return the current date/time in different formats"""
    if type == 1:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    elif type == 2:
        now = datetime.now().strftime('%H:%M:%S')
    elif type == 3: ##return the day of week
        now = datetime.isoweekday(datetime.now())
    elif type == 4: ##return the day of week
        now = datetime.today()
    return now

def date_between(ss,ee):
    """Return dates between the numbers"""
    start_date =  (now(4)  - timedelta(days=ss))
    end_date =  (now(4)  - timedelta(days=ee))
    return [start_date,end_date]

def s3up(table_name,s3_folder,file_ext='.csv',bucket='bucket_name'):
    """Upload a file to s3 bucket"""
    cc = creds.get("aws").get("s3")
    s3 = boto3.resource('s3',aws_access_key_id=cc.get("access_key_id"), aws_secret_access_key=cc.get("secret_access_key"),region_name =cc.get("region"))
    s3.meta.client.upload_file(table_name+file_ext, bucket, '%s/%s' %(s3_folder,table_name+file_ext))

def psycopg_con():
    """Create a connection to redshift"""
    cc = creds.get("aws").get("redshift")
    con=psycopg2.connect(dbname=cc.get("db"),host=cc.get("host"),port=cc.get("port"), user=cc.get("user_name"),password=cc.get("password"))
    return con

def rsup(s3_folder,table_name,file_ext='.csv', bucket="bucket_name"):
    """Copy data from S3 buckets into tables"""
    cc = creds.get("aws").get("s3")
    if file_ext == '.csv':
        deli  = ','
    elif file_ext == '.txt' :
        deli = r'\t'
    cur = psycopg_con().cursor() # create connection
    dml = """copy %s.%s from 's3://%s/%s/%s'
        access_key_id '%s'
        secret_access_key '%s'
        region 'us-east-1'
        ignoreheader 1
        null as ''
        removequotes
        delimiter '%s'
        escape
        acceptinvchars ;""" % (s3_folder,table_name,bucket,s3_folder,table_name+file_ext,cc.get("access_key_id"),cc.get("secret_access_key"),deli)
    cur.execute(dml)
    cur.connection.commit()
    cur.connection.close()
    return cur.statusmessage

def rsexe(sql,type):
    """Use this to execute queries in redshift.
    If type = dml a dataframe is return
    if type = ddl status message is return
    if type = dl2 data is delete then a temp table is returned
    """
    cur = psycopg_con().cursor()
    if type == 'dml':
        data=pd.read_sql(sql, cur.connection)
    elif  type == 'ddl':
        cur.execute(sql)
        data = cur.statusmessage
    elif  type == 'dl2':
        data=pd.read_sql(sql.split(';')[0]+';'+sql.split(';')[1],cur.connection) # insert data to be deleted into temp table
        cur.execute(sql.split(';')[2]) # delete data
    cur.connection.commit()
    cur.connection.close()
    return data

def stg_sql(schema, table,date_column, startdate,endate):
    """         0 = select * into #temp from {0}.{1} where {2}::date>= \'{3}\'and {2}::date <= \'{4}\';
                1 = select * from #temp;
                2 = delete from {0}.{1} where {2}::date>= \'{3}\' and {2}::date <= \'{4}\';
    """
    return  """ select * into #temp from {0}.{1} where {2}::date>= \'{3}\'and {2}::date <= \'{4}\';
                select * from #temp;
                delete from {0}.{1} where {2}::date>= \'{3}\' and {2}::date <= \'{4}\';
            """.format(schema, table,date_column, startdate,endate)

def s3_redshift(df, s3_folder, table_name,file_ext='.csv'):
    """ Save file to the working files folder and then move it to S3 bucket then into redshift"""
    df =  df.convert_dtypes()
    df.to_csv(table_name+file_ext, sep=',' ,index=False,  encoding='utf-8', line_terminator =None)
    s3up(table_name,file_ext,s3_folder)
    rsup(s3_folder,table_name,file_ext)
    print('Copied data into {0}/{1}/{2} s3 bucket\n '\
        'Inserted data into {1}.{2}'.format(bucket,s3_folder,table_name))

def mssql(sql,type):
    """ Connect to SQL Server, Type dml is use to fetch data while ddl is used to modify data"""
    import pymssql
    cc = creds.get("sql_sever")
    conn = pymssql.connect(server=cc.get("sql_sever"), user=cc.get("user_name"), password=cc.get("password"), database=cc.get("db"))
    cursor = conn.cursor(as_dict=True)
    cursor.execute(sql)
    if type == 'dml':
        data=cursor.fetchall()
    elif  type == 'ddl':
        data = None
    conn.commit()
    conn.close()
    return data

def gspread_con():
    """Create a gspread connection"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(root_fp+'creds/svc-holmes.json', scope)
    gc = gspread.authorize(credentials)
    return gc

def open_wb(id):
    """Open workbook"""
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

def ppl_hr(query):
    """Return contents from the people HR API, requires query keys"""
    cc = creds.get("ppl_hr").get("api_key")
    link = 'https://api.peoplehr.net/Query'
    para = json.dumps({"APIKey": cc.get("pplapikey"),"Action": 'GetQueryResultByQueryName',"QueryName" : query})
    post = r.post(link,para)
    content = post.json()['Result']
    return  content

def get_s3_file_version(bucket,filename):
    """Downloads list of versions a file"""
    cc = creds.get("aws").get("s3")
    s3 = boto3.resource('s3',aws_access_key_id=cc.get("access_key_id"), aws_secret_access_key=cc.get("secret_access_key"))

    bucket = s3.Bucket(bucket)
    versions = bucket.object_versions.filter(Prefix = filename)

    for version in versions:
        try:
            object = version.get()
            path = version.object_key
            last_modified = object.get('LastModified')
            version_id = object.get('VersionId')
            print(path, last_modified, version_id, sep = '\t')

            object = version.get()
            filename = path.rsplit('/')[-1]
            with open(root_fp+'{0}-{1}-{2}'.format(str(last_modified)[:10],version_id,filename), 'wb') as fout:
                shutil.copyfileobj(object.get('Body'), fout)
        except:
            pass

def delete_all_files(file_path):
    for filename in os.listdir(root_fp+file_path):
        try:
            os.remove(root_fp+file_path+'/'+filename)
            print("deleted " + root_fp+file_path+'/'+filename)
        except:
            print("failed deleting " + root_fp+file_path+'/'+filename)
            pass

import importlib as ip, sys
results = []
# def execution(file, fp = root_fp):
def execution(file, fp ):
    try:
        sys.path.insert(0,root_fp)
        global start_time
        start_time = now(2)
        im = ip.import_module(file)
        end_time = now(2)
        if len(im.results) > 1:
            for items in im.results:
                results.append(str(items) )
                print(str(items))
        else:
            results.append(str(im.results[0])+' Started at '+ start_time +' ended at ' + end_time )
            print(str(im.results[0])+' Started at '+ start_time +' ended at ' + end_time )
    except:
        end_time = now(2)
        results.append(file.capitalize()+' job failed! Started at '+ start_time +' ended at ' + end_time)
        print(file.capitalize()+' job failed! Started at '+ start_time +' ended at ' + end_time)
    # return results
