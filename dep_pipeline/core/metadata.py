"""
  This script creates the archiving metadata file as part of the DQA process.

  Original scripts written in IDL by Jeff Mader
  Ported to python by Josh Riley

#TODO: add more code documentation
#TODO: add more asserts/logging
#TODO: change this to a class?

"""



#imports
import sys
import os
from astropy.io import fits
from dep_pipeline.core.common import make_dir_md5_table
import datetime
import re
import pandas as pd
import html



def make_metadata(keywordsDefFile, metaOutFile, lev0Dir, extraData=None, log=None, dev=False, instrKeywordSkips=[]):
    """
    Creates the archiving metadata file as part of the DQA process.

    @param keywordsDefFile: keywords format definition input file path
    @type keywordsDefFile: string
    @param metaOutFile: metadata output file path
    @type metaOutFile: string
    @param lev0Dir: directory for finding FITS files and writing output files
    @type lev0Dir: string
    @param extraData: dictionary of any extra key val pairs not in header
    @type extraData: dictionary
    """


    #open keywords format file and read data
    #NOTE: changed this file to tab-delimited
    if log: log.info('metadata.py reading keywords definition file: {}'.format(keywordsDefFile))
    keyDefs = pd.read_csv(keywordsDefFile, sep='\t')


    #add header to output file
    if log: log.info('metadata.py writing to metadata table file: {}'.format(metaOutFile))
    with open(metaOutFile, 'w+') as out:

        #check col width is at least as big is the keyword name
        for index, row in keyDefs.iterrows():
            if (len(row['keyword']) > row['colSize']):
                raise Exception("metadata.py: Alignment issue: Keyword column name {} is bigger than column size of {}".format(row['keyword'], row['colSize']))            

        for index, row in keyDefs.iterrows():
            out.write('|' + row['keyword'].ljust(row['colSize']))
        out.write("|\n")

        for index, row in keyDefs.iterrows():
            out.write('|' + row['dataType'].ljust(row['colSize']))
        out.write("|\n")

        #todo: add units?
        for index, row in keyDefs.iterrows():
            out.write('|' + ''.ljust(row['colSize']))
        out.write("|\n")

        for index, row in keyDefs.iterrows():
            nullStr = '' if (row['allowNull'] == "N") else "null"
            out.write('|' + nullStr.ljust(row['colSize']))
        out.write("|\n")


    #track warning counts
    warns = {'type': 0, 'truncate': 0}


    #walk lev0Dir to find all final fits files
    if log: log.info('metadata.py searching fits files in dir: {}'.format(lev0Dir))
    for root, directories, files in os.walk(lev0Dir):
        for filename in sorted(files):
            if filename.endswith('.fits'):
                fitsFile = os.path.join(root, filename)

                extra = {}
                if filename in extraData: extra = extraData[filename]

                log.info("Creating metadata record for: " + fitsFile)
                add_fits_metadata_line(fitsFile, metaOutFile, keyDefs, extra, warns, log, dev, instrKeywordSkips)


    #create md5 sum
    md5Outfile = metaOutFile.replace('.table', '.md5sum')
    if log: log.info('metadata.py creating {}'.format(md5Outfile))
    make_dir_md5_table(lev0Dir, ".metadata.table", md5Outfile)


    #warn only if counts
    if (warns['type'] > 0):
        if log: log.info('metadata.py: Found {} data type mismatches (search "metadata check" in log).'.format(warns['type']))
    if (warns['truncate'] > 0):
        if log: log.warning('metadata.py: Found {} data truncations (search "metadata check" in log).'.format(warns['truncate']))



def add_fits_metadata_line(fitsFile, metaOutFile, keyDefs, extra, warns, log, dev, instrKeywordSkips):
    """
    Adds a line to metadata file for one FITS file.
    """

    #get header object using astropy
    header = fits.getheader(fitsFile)

    #check keywords
    check_keyword_existance(header, keyDefs, log, dev, instrKeywordSkips)

    #write all keywords vals for image to a line
    with open(metaOutFile, 'a') as out:

        for index, row in keyDefs.iterrows():

            keyword   = row['keyword']
            dataType  = row['dataType']
            colSize   = row['colSize']
            allowNull = row['allowNull']

            #get value from header, set to null if not found
            if   (keyword in header) : val = header[keyword]
            elif (keyword in extra)  : val = extra[keyword]
            else: 
                val = 'null'
                if dev: log_msg(log, dev, 'metadata check: Keyword not found in header: ' + keyword)

            #special check for val = fits.Undefined
            if isinstance(val, fits.Undefined):
                val = 'null'
                if dev: log_msg(log, dev, 'metadata check: Keyword value is fits.Undefined: ' + keyword)

            #check keyword val and format
            val = check_keyword_val(keyword, val, row, warns, log)

            #write out val padded to size
            out.write(' ')
            out.write(str(val).ljust(colSize))

        out.write("\n")
 


def check_keyword_existance(header, keyDefs, log, dev=False, instrKeywordSkips=[]):

    #get simple list of keywords
    keyDefList = []
    for index, row in keyDefs.iterrows():
        keyDefList.append(row['keyword'])        

    #find all keywords in header that are not in metadata file
    skips = ['SIMPLE', 'COMMENT', 'PROGTL1', 'PROGTL2', 'PROGTL3'] + instrKeywordSkips
    for keywordHdr in header:
        if keywordHdr not in keyDefList and not is_keyword_skip(keywordHdr, skips):
            log_msg(log, dev, 'metadata.py: header keyword "{}" not found in metadata definition file.'.format(keywordHdr))

    #find all keywords in metadata def file that are not in header
    skips = ['PROGTITL', 'PROPINT']
    for index, row in keyDefs.iterrows():
        keyword = row['keyword']
        if keyword not in header and keyword not in skips and row['allowNull'] == "N":
            log_msg(log, dev, 'metadata.py: non-null metadata keyword "{}" not found in header.'.format(keyword))


def check_keyword_val(keyword, val, fmt, warns, log=None, dev=False):
    '''
    Checks keyword for correct type and proper value.
    '''

    #specific ERROR, UDF values that we should convert to "null"
    errVals = ['#### Error ###']
    if (val in errVals):
        val = 'null'


    #check null
    if (val == 'null' or val == ''):
        if (fmt['allowNull'] == 'N'):
            raise Exception('metadata check: incorrect "null" value found for non-null keyword {}'.format(keyword))            
        return val


    #check value type
    vtype = type(val).__name__

    if (fmt['dataType'] == 'char'):
        if (vtype == 'bool'):
            if   (val == True):  val = 'T'
            elif (val == False): val = 'F'
        elif (vtype == 'int' and val == 0):
            val = ''
            log_msg(log, dev, 'metadata check: found integer 0, expected {}. KNOWN ISSUE. SETTING TO BLANK!'.format(fmt['dataType']))
        elif (vtype != "str"):
            log_msg(log, dev, 'metadata check: var type {}, expected {} ({}={}).'.format(vtype, fmt['dataType'], keyword, val))
            warns['type'] += 1

    elif (fmt['dataType'] == 'integer'):
        if (vtype != "int"):
            log_msg(log, dev, 'metadata check: var type of {}, expected {} ({}={}).'.format(vtype, fmt['dataType'], keyword, val))
            warns['type'] += 1

    elif (fmt['dataType'] == 'double'):
        if (vtype != "float" and vtype != "int"):
            log_msg(log, dev, 'metadata check: var type of {}, expected {} ({}={}).'.format(vtype, fmt['dataType'], keyword, val))
            warns['type'] += 1

    elif (fmt['dataType'] == 'date'):
        try:
            datetime.datetime.strptime(val, '%Y-%m-%d')
        except ValueError:
            log_msg(log, dev, 'metadata check: expected date format YYYY-mm-dd ({}={}).'.format(keyword, val))
            warns['type'] += 1

    elif (fmt['dataType'] == 'datetime'):
        try:
            datetime.datetime.strptime(val, '%Y-%m-%d %H:%i:%s')
        except ValueError:
            log_msg(log, dev, 'metadata check: expected date format YYYY-mm-dd HH:ii:ss ({}={}).'.format(keyword, val))
            warns['type'] += 1
     

    #check char length
    length = len(str(val))
    if (length > fmt['colSize']):
        if (fmt['dataType'] == 'double'): 
            log_msg(log, dev, 'metadata check: char length of {} greater than column size of {} ({}={}).  TRUNCATING.'.format(length, fmt['colSize'], keyword, val))
            warns['truncate'] += 1
            val = truncate_float(val, fmt['colSize'])
        else: 
            log_msg(log, dev, 'metadata check: char length of {} greater than column size of {} ({}={}).  TRUNCATING.'.format(length, fmt['colSize'], keyword, val))
            warns['truncate'] += 1
            val = str(val)[:fmt['colSize']]


    #todo: check value range, discrete values?

    return val


def is_keyword_skip(keyword, skips):
    for pattern in skips:
        if re.search(pattern, keyword):
            return True
    return False


def log_msg (log, dev, msg):
    if not log: return

    if dev: log.warning(msg)
    else  : log.info(msg)



def truncate_float(f, n):
    s = '{}'.format(f)
    exp = ''
    if 'e' in s or 'E' in s:
        parts = re.split('e', s, flags=re.IGNORECASE)
        s = parts[0]
        exp = 'e' + parts[1]

    n -= len(exp)
    return s[:n] + exp



def compare_meta_files(filepaths, skipColCompareWarn=False):
    '''
    Takes an array of filepaths to metadata output files and compares them all to 
    the first metadata file in a smart manner.
    '''
    results = []

    #columns we always skip value check
    skips = ['DQA_DATE', 'DQA_VERS']

    #store list of columns to compare
    compareCols = []
    compareKoaids = []

    #loop, parse and store dataframes
    dfs = []
    for filepath in filepaths:
        data = load_metadata_file_as_df(filepath)
        if isinstance(data, pd.DataFrame): dfs.append(data)
        else                             : return False

    #compare all to first df in list
    baseDf = dfs[0]
    baseColList = baseDf.columns.tolist()
    for i, df in enumerate(dfs):
        if i == 0: continue

        result = {}
        result['compare'] = '==> comparing (0){} to ({}){}:'.format(baseDf.name, i, df.name)
        result['warnings'] = []

        #basic two-way column name compare
        colList = df.columns.tolist()
        for col in colList:
            if col not in baseColList:
                if col not in skips:
                    if not skipColCompareWarn: 
                        result['warnings'].append('Meta compare: MD{} col "{}" not in MD0 col list.'.format(i, col))
            else:
                if col not in compareCols: compareCols.append(col)
        for col in baseColList:
            if col not in colList:
                if col not in skips:
                    if not skipColCompareWarn: 
                        result['warnings'].append('Meta compare: MD0 col "{}" not in MD{} col list.'.format(col, i))
            else:
                if col not in compareCols: compareCols.append(col)


        #basic two-way row find using KOAID value
        for index, row in df.iterrows():
            koaid = row['KOAID']
            baseRow = baseDf[baseDf['KOAID'] == koaid]
            if baseRow.empty: 
                result['warnings'].append('Meta compare: CANNOT FIND KOAID "{}" in MD0'.format(koaid))
                continue
            else:
                if koaid not in compareKoaids: compareKoaids.append(koaid)

        for index, baseRow in baseDf.iterrows():
            koaid = baseRow['KOAID']
            row = df[df['KOAID'] == koaid]
            if row.empty: 
                result['warnings'].append('Meta compare: CANNOT FIND KOAID "{}" in MD{}'.format(koaid, i))
                continue
            else:
                if koaid not in compareKoaids: compareKoaids.append(koaid)

        #for koaids we found in both, compare those rows
        for koaid in compareKoaids:
            row0 = baseDf[baseDf['KOAID'] == koaid].iloc[0]
            row1 = df[df['KOAID'] == koaid].iloc[0]
            for col in compareCols:
                if col in skips: continue

                val0 = row0[col]
                val1 = row1[col]

                if val_smart_diff(val0, val1, col):
                    result['warnings'].append('Meta compare: {}: col "{}": (0)"{}" != ({})"{}"'.format(koaid, col, val0, i, val1))

        results.append(result)

    return results



def val_smart_diff(val0, val1, col=None):

    #turn pandas null to blank 
    if pd.isnull(val0): val0 = ''
    if pd.isnull(val1): val1 = ''

    #special fix for progtitl
    if col == 'PROGTITL':
        val0 = val0.replace('  ',' ')
        val1 = val0.replace('  ',' ')

    #try to decimal format (if not then no problem)
    try:
        newval0 = "{:.2f}".format(float(val0))
        newval1 = "{:.2f}".format(float(val1))
    except:
        newval0 = val0
        newval1 = val1
    val0 = newval0
    val1 = newval1

    #diff
    isDiff = False
    val0 = str(val0).lower()
    val1 = str(val1).lower()
    if val0 != val1:

        #if different, try html escaping
        if html.escape(val0) != html.escape(val1):
            isDiff = True

    return isDiff



def load_metadata_file_as_df(filepath):

    if not os.path.isfile(filepath): return False

    with open(filepath, 'r', errors='replace') as f:

        # Read first line of header and find all column widths using '|' split
        header = f.readline().strip()
        cols = header.split('|')
        colWidths = []
        for col in cols:
            w = len(col)
            if w <= 1: continue;
            colWidths.append(w+1)

        #read fixed-width formatted metadata file using calculated col widths and remove garbage
        data = pd.read_fwf(filepath, widths=colWidths, skiprows=range(1,4))
        data.columns = data.columns.str.replace('|','')
        data.columns = data.columns.str.strip()

        data.name = os.path.basename(filepath)
        return data

    return None


def header_keyword_report(keywordsDefFile, fitsFile):


    #read keywords format file and fits file
    keyDefs = pd.read_csv(keywordsDefFile, sep='\t')
    header = fits.getheader(fitsFile, ignore_missing_end=True)

    #put header keys into set
    print ("=========HEADER LIST===========")
    headerKeys = []
    for key in header.keys():
        print (key)
        headerKeys.append(key)

    #put keyDefs into set
    print ("=========FORMAT LIST===========")
    formatKeys = []
    for index, row in keyDefs.iterrows():
        print (row['keyword'])
        formatKeys.append(row['keyword'])

    #diff sets 
    diff1 = list(set(headerKeys) - set(formatKeys))
    print ("=========KEYWORDS DIFF (header - format)===========\n", diff1)

    diff2 = list(set(formatKeys) - set(headerKeys))
    print ("=========KEYWORDS DIFF (format - header)===========\n", diff2)
