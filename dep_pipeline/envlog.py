import pandas as pd

#todo: super inefficient use of calling this everytime for each fits file!

def envlog(logFile, logType, telnr, dateObs, utc):
    """ 
    Retrieve nearest env log data from envMet.arT or envFocus.arT 
    file that is closest to and within +-interval seconds of the input
    date and time.
    """
    #
    # Setup defaults and config based on logType
    #
    telnr = str(telnr)
    if logType == 'envMet':
        interval = 30
        values = {  'time'         : 'null',
                    'wx_domtmp'    : 'null',
                    'wx_outtmp'    : 'null',
                    'wx_domhum'    : 'null',
                    'wx_outhum'    : 'null',
                    'wx_pressure'  : 'null',
                    'wx_windspeed' : 'null',
                    'wx_winddir'   : 'null',
                    'wx_dewpoint'  : 'null'}
        output = [  'wx_dewpoint', 
                    'wx_outhum', 
                    'wx_outtmp', 
                    'wx_domtmp', 
                    'wx_domhum', 
                    'wx_pressure', 
                    'wx_windspeed', 
                    'wx_winddir']
    elif logType == 'envFocus':
        interval = 2.5
        values = {'time' : 'null', 'guidfwhm' : 'null'}
        output = ['guidfwhm']
    else:
        return
    #
    # Read envlog file to determine if file and header exist
    # Skip first and third lines (interval and type lines)
    # Second line is header
    #
    try:
        #TODO: NOTE: added 'dtype=object' to skip low memory warning due to
        #column mixed data types. We should define dtypes for columns to speed up.
        data = pd.read_csv(logFile, skiprows=[0,2], dtype=object, skipinitialspace=True)
    except Exception as e:
        print ('envlog: Unable to open: {}!'.format(logFile))
        return False
    #
    # Catch any unexpected errors
    #
    try:
        #
        # Setup if using header or index numbers
        #
        if 'UNIXDate' in data.keys():
            hstKeys = ['HSTdate', 'HSTtime']
            keys = ['k0:met:dewpointRaw', 
                    'k0:met:humidityRaw', 
                    'k0:met:tempRaw', 
                    'k'+telnr+':met:tempRaw', 
                    'k'+telnr+':met:humidityRaw', 
                    'k0:met:pressureRaw', 
                    'k'+telnr+':met:windSpeedRaw', 
                    'k'+telnr+':met:windAzRaw']
            if logType == 'envFocus':
                keys = ['k'+telnr+':dcs:pnt:cam0:fwhm']
        else:
            assert False, "ERROR: This numerically indexed column method is possibly unreliable.  Asserting here to see if it ever happens."
            hstKeys = [2, 3]
            keys = [5, 8, 10, 18, 20, 22, 24, 27]
            if logType == 'envFocus':
                keys = [26]
            data = pd.read_csv(logFile, skiprows=[0,1,2], header=None, dtype=object, skipinitialspace=True)
        #
        # Convert DATE-OBS/UT to HST
        #
        from datetime import datetime, timedelta
        utDatetime = datetime.strptime(dateObs + ' ' + utc, '%Y-%m-%d %H:%M:%S.%f')
        utDatetime += timedelta(hours=-10)
        #
        # Find envMet entry within +-interval seconds of this time
        #
        dt1 = utDatetime + timedelta(seconds=-interval)
        dt1 = dt1.strftime('%Y-%m-%d %H:%M:%S.%f')
        dt2 = utDatetime + timedelta(seconds=interval)
        dt2 = dt2.strftime('%Y-%m-%d %H:%M:%S.%f')
        envDatetime = data[hstKeys[0]][0:] + ' ' + data[hstKeys[1]][0:]
        envEntries = pd.to_datetime(envDatetime, format='%d-%b-%Y %H:%M:%S.%f').between(dt1, dt2)
        envIndex = envEntries.index[envEntries]
        if len(envIndex) == 0:
            return values
        #
        # Timestamp of this entry in UT
        #
        mTime = data[hstKeys[0]][envIndex[0]] + ' ' + data[hstKeys[1]][envIndex[0]]    
        mTime = datetime.strptime(mTime, '%d-%b-%Y %H:%M:%S.%f')
        mTime += timedelta(hours=10)
        #todo: truncating microseconds b/c strftime does not support rounding overflow
        mTime = mTime.strftime('%H:%M:%S.%f')[:-4]
        values['time'] = mTime
        #
        # Set individual values for this entry
        #
        for index, key in enumerate(keys):
            value = 'null'
            try:
                if key in data.keys(): 
                    value = data[key][envIndex[0]]
                # elif key.startswith('k1:') or key.startswith('k2:'):
                #     key = key[0]+'0'+key[2:]
                #     if key in data.keys(): 
                #         value = data[key][envIndex[0]]
                value = float("%0.2f" % float(value))
            except (ValueError, KeyError):
                value = 'null'
            values[output[index]] = value

        return values

    except Exception as e:
        return False