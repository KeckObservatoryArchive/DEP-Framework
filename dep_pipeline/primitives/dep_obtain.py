import os
from datetime import datetime as dt, timedelta
from dep_pipeline.core.common import get_api_data
import subprocess

from keckdrpframework.primitives.base_primitive import BasePrimitive
from keckdrpframework.models.arguments import Arguments

class dep_obtain(BasePrimitive):
    """
    Queries the telescope schedule database and creates the following files in stageDir:

    dep_obtainINSTR.txt
    dep_notschedINSTR.txt (if no entries found in database)

    @param instrObj: the instrument object
    @type instrObj: instrument class
    """

    def __init__(self, action, context):
        BasePrimitive.__init__(self, action, context)
        self.instrObj = self.context.instrObj
        self.logger.info('dep_obtain: started for {} {} UT'.format(self.instrObj.instr, self.instrObj.utDate))

    def _perform(self):
        self.logger.info("Running DEP OBTAIN")

        self.logger.info("Get HST from utDate")

        utDateObj = dt.strptime(self.instrObj.utDate, '%Y-%m-%d')
        prevDateObj = utDateObj - timedelta(days=1)
        prevDate = prevDateObj.strftime('%Y-%m-%d')

        args = Arguments()

        # Check if we should run old dep_obtain

        if dt.strptime(self.instrObj.utDate, "%Y-%m-%d") <= dt.strptime("2018-08-01", "%Y-%m-%d"):
            run_old_dep_obtain(self.instrObj.instr, prevDate, self.instrObj.utDate, self.instrObj.dirs['stage'], log)
            return

        # Output files

        notScheduledFile = ''.join((self.instrObj.dirs['stage'], '/dep_notsched', self.instrObj.instr, '.txt'))
        obtainFile       = ''.join((self.instrObj.dirs['stage'], '/dep_obtain', self.instrObj.instr, '.txt'))

        #try:
        if True:
            # Get OA

            telnr = self.instrObj.get_telnr()
            oaUrl = ''.join((self.instrObj.telUrl, 'cmd=getNightStaff', '&date=', prevDate, '&telnr=', str(telnr), '&type=oa'))
            self.logger.info('dep_obtain: retrieving night staff info: {}'.format(oaUrl))
            oaData = get_api_data(oaUrl)
            oa = 'None'
            if oaData:
                if isinstance(oaData, dict):
                    if ('Alias' in oaData):
                        oa = oaData['Alias']
                else:
                    for entry in oaData:
                        if entry['Type'] == 'oa' or entry['Type'] == 'oar':
                            oa = entry['Alias']

            # Read the telescope schedul URL
            # No entries found: Create stageDir/dep_notschedINSTR.txt and dep_obtainINSTR.txt

            instrBase = 'NIRSP' if (self.instrObj.instr == 'NIRSPEC') else self.instrObj.instr
            schedUrl = ''.join((self.instrObj.telUrl, 'cmd=getSchedule', '&date=', prevDate, '&instr=', instrBase))
            self.logger.info('dep_obtain: retrieving telescope schedule info: {}'.format(schedUrl))
            schedData = get_api_data(schedUrl)
            if schedData and isinstance(schedData, dict): schedData = [schedData]
            if not schedData:
                self.logger.info('dep_obtain: no telescope schedule info found for {}'.format(self.instrObj.instr))

                with open(notScheduledFile, 'w') as fp:
                    fp.write('{} not scheduled'.format(self.instrObj.instr))

                with open(obtainFile, 'w') as fp:
                    fp.write("{}\t{}\tNONE\tNONE\tNONE\tNONE\tNONE\tNONE\tNONE\tNONE\tNONE".format(prevDate, oa))

            # Entries found: Create stageDir/dep_obtainINSTR.txt

            else:
                with open(obtainFile, 'w') as fp:
                    num = 0
                    for entry in schedData:

                        if entry['Account'] == '': entry['Account'] = '-'

                        obsUrl = self.instrObj.telUrl + 'cmd=getObservers' + '&schedid=' + entry['SchedId']
                        self.logger.info('dep_obtain: retrieving observers info: {}'.format(obsUrl))
                        obsData = get_api_data(obsUrl)
                        if obsData and len(obsData) > 0: observers = obsData[0]['Observers']
                        else                           : observers = 'None'

                        if num > 0: fp.write('\n')

                        line = ''
                        line += prevDate
                        line += "\t" + oa
                        line += "\t" + entry['Account']
                        line += "\t" + entry['Institution']
                        line += "\t" + entry['Principal']
                        line += "\t" + entry['ProjCode']
                        line += "\t" + observers
                        line += "\t" + entry['StartTime']
                        line += "\t" + entry['EndTime']
                        line += "\t" + entry['Instrument']
                        line += "\t" + entry['TelNr']

                        fp.write(line)
                        self.logger.info("dep_obtain: " + line)

                        num += 1

        #except:
        #    self.logger.info('dep_obtain: {} error reading telescope schedule'.format(self.instrObj.instr))
        #    args.success = False

        args.success = True
        return args



def run_old_dep_obtain(instr, prevDate, utDate, stageDir, log):
    '''
    For dates before 2018-01-01, we have to run the old PHP version since new database does not contain data before then
    '''

    cmd = ["/kroot/archive/dep/obtain/5-1-0/dep_obtain.php", instr, prevDate.replace('-', '/'), utDate.replace('-', '/'), stageDir]
    log.info("Running old dep_obtain.php: " + ' '.join(cmd))
    proc = subprocess.call(cmd)



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
