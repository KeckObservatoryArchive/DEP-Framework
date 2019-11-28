from datetime import datetime
import os
import hashlib
from urllib.request import urlopen
import json
from dep_pipeline.send_email import send_email
import configparser
import glob
import re


def get_root_dirs(rootDir, instr, utDate):
    """
    Gets the various rootDir subdirectories of interest

    @param rootDir: root directory for data processing
    @type rootDir: string
    @param instr: instrument name
    @type instr: string
    @param utDate: UT date (yyyy-mm-dd)
    @type utDate: string
    """
    print("***** RUNNING GET_ROOT_DIRS *****")
    instr = instr.upper()
    ymd = utDate
    ymd = ymd.replace('-', '')
    ymd = ymd.replace('/', '')

    dirs = {}
    dirs['stage']   = ''.join((rootDir, '/stage/', instr, '/', ymd))
    dirs['process'] = ''.join((rootDir, '/', instr))
    dirs['output']  = ''.join((rootDir, '/', instr, '/', ymd))
    dirs['lev0']    = ''.join((rootDir, '/', instr, '/', ymd, '/lev0'))
    dirs['lev1']    = ''.join((rootDir, '/', instr, '/', ymd, '/lev1'))
    dirs['anc']     = ''.join((rootDir, '/', instr, '/', ymd, '/anc'))
    dirs['udf']     = ''.join((dirs['anc'], '/udf'))

    print(dirs)
    return dirs




def make_dir_md5_table(readDir, endswith, outfile, fileList=None, regex=None):

    #ensure path ends in slash since we rely on that later here
    if not readDir.endswith('/'): readDir += '/'

    #get file list either direct or using 'endswith' search
    files = []
    if fileList:
        files = fileList
    else:        
        for dirpath, dirnames, filenames in os.walk(readDir):
            for f in filenames:
                if not dirpath.endswith('/'): dirpath += '/'
                match = False
                if endswith and f.endswith(endswith): match = True
                elif regex and re.search(regex, f): match = True
                if match:
                    files.append(dirpath + f)
        files.sort()
        
    #write out table
    with open(outfile, 'w') as fp:
        for file in files:
            md5 = hashlib.md5(open(file, 'rb').read()).hexdigest()
            bName = file.replace(readDir, '')
            fp.write(md5 + '  ' + bName + '\n')



def removeFilesByWildcard(wildcardPath):
    for file in glob.glob(wildcardPath):
        os.remove(file)



def get_api_data(url, getOne=False, isJson=True):
    '''
    Gets data for common calls to url API requests.

    #todo: add some better validation checks and maybe some options (ie getOne, typeCast)
    '''
    
    try:
        data = urlopen(url)
        data = data.read().decode('utf8')
        if isJson: data = json.loads(data)

        if getOne and len(data) > 0: 
            data = data[0]

        return data

    except Exception as e:
        return None



def do_fatal_error(msg, instr=None, utDate=None, failStage=None, log=None):

    #read config vars
    config = configparser.ConfigParser()
    config.read('config.live.ini')
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    
    #form subject
    subject = 'DEP ERROR: ['
    if (instr)     : subject += instr     + ' '
    if (utDate)    : subject += utDate    + ' '
    if (failStage) : subject += failStage + ' '
    subject += ']'

    #always print
    print (subject + ' ' + msg)

    #if log then log
    if log: log.error(subject + ' ' + msg)

    #if admin email and not dev then email
    if (adminEmail != ''):
        send_email(adminEmail, adminEmail, subject, msg)



def update_koatpx(instr, utDate, column, value, log=''):
    """
    Sends command to update KOA data

    @param instrObj: the instrument object
    @param column: column to update in koa.koatpx
    @type column: string
    @param value: value to update column to
    @type value: string
    """

    import configparser
    config = configparser.ConfigParser()
    config.read('config.live.ini')

    user = os.getlogin()
    import hashlib
    myHash = hashlib.md5(user.encode('utf-8')).hexdigest()

    # Create database access URL

    sendUrl = config['API']['koaapi']
    sendUrl += 'cmd=updateTPX&instr=' + instr.upper()
    sendUrl += '&utdate=' + utDate.replace('/', '-') + '&'
    sendUrl += 'column=' + column + '&value=' + value.replace(' ', '+')
    sendUrl += '&hash=' + myHash
    if log: log.info('update_koatpx {} - {}'.format(user, sendUrl))

    # Call URL and check result 
    data = get_api_data(sendUrl)
    if not data:
        if log: log.error('update_koatpx failed! URL: ' + sendUrl)
        return False
    return True


def get_directory_size(dir):
    """
    Returns the directory size in MB

    @param dir: directory to determine size for
    @type dir: string
    """

    #directory doesn't exist
    if not os.path.isdir(dir):
        return 0

    #walk through and sum up size
    total = 0
    for dirpath, dirnames, filenames in os.walk(dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    return str(total/1000000.0)


def get_prog_inst(semid, default=None, log=None, isToO=False):
    """
    Query the proposalsAPI and get the program institution

    @type semid: string
    @param semid: the program ID - consists of semester and progname (ie 2017B_U428)
    """

    #todo: get this url from config
    #TODO: NOTE: This is the only way to get ToO institution info (> 2017).  Do we need to backfill koa_program?
    if isToO: 
        api = 'https://www.keck.hawaii.edu/software/db_api/proposalsAPI.php'
    else:
        api = 'https://www.keck.hawaii.edu/software/db_api/proposalsAPI.php'

    url = api + '?ktn='+semid+'&cmd=getAllocInst'
    val = get_api_data(url, isJson=False)

    if not val or val.startswith('Usage') or val == 'error':
        if log: log.error('Unable to query API: ' + url)
        return default
    else:
        return val

def get_prog_pi(semid, default=None, log=None):
    """
    Query the proposalsAPI and get the PI last name

    @type semid: string
    @param semid: the program ID - consists of semester and progname (ie 2017B_U428)
    """

    #todo: get this url from config
    api = 'https://www.keck.hawaii.edu/software/db_api/koa.php'
    url = api + '?semid='+semid+'&cmd=getPI'
    val = get_api_data(url, getOne=True)

    if (val == None or 'pi_lastname' not in val): 
        if log: log.error('Unable to query API: ' + url)
        return default
    else:
        #remove whitespace and get last name only
        val = val['pi_lastname']
        val = val.replace(' ','')
        if (',' in val): 
            val = val.split(',')[0]
        return val


def get_prog_title(semid, default=None, log=None):
    """
    Query the DB and get the program title

    @type semid: string
    @param semid: the program ID - consists of semester and progname (ie 2017B_U428)
    """

    #todo: get this url from config
    api = 'https://www.keck.hawaii.edu/software/db_api/koa.php'
    url = api + '?cmd=getTitle&semid=' + semid
    title = get_api_data(url, getOne=True)
    if (title == None or 'progtitl' not in title): 
        if log: log.warning('get_prog_title: Could not find program title for semid "{}"'.format(semid))
        return default
    else : 
        #deal with non-printable characters that can end up in progtitl
        progtitl = title['progtitl'].encode('ascii', errors='ignore').decode('UTF-8')
        return progtitl


def is_progid_valid(progid):

    #TODO: improve this
    if not progid: return False
    if len(progid) <= 2: return False
    if len(progid) >= 6: return False
    if " " in progid: return False
    return True


def get_obtain_data(file):
    '''
    Reads an obtain output file (presumably one it created)
    and parses it into a key-value pair array for each entry
    '''

    #check
    if not os.path.exists(file):
        raise Exception('get_obtain_data: file "{}" does not exist!!'.format(file))
        return

    #read each line and create key-value pair rows from col list names
    #NOTE: splitting by tabs is new format method, but we handle old spaces case too
    data = []
    cols = ['Date', 'OA', 'Account', 'Institution', 'Principal', 'ProjCode', 'Observer', 'StartTime', 'EndTime', 'Instrument', 'TelNr']
    with open(file, 'r') as rfile:
        for line in rfile:
            if "\t" in line: vals = line.strip().split("\t")
            else           : vals = line.strip().split(' ')
            row = {}
            for i in range(0, len(cols)):
                row[cols[i]] = vals[i] if (i < len(vals)) else None
            data.append(row)
            del row

    return data

