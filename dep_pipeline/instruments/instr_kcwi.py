'''
This is the class to handle all the KCWI specific attributes
KCWI specific DR techniques can be added to it in the future

12/14/2017 M. Brown - Created initial file
'''

import instrument
import datetime as dt

class Kcwi(instrument.Instrument):
    def __init__(self, endTime=dt.datetime.now(), rDir=''):
        # Call the parent init to get all the shared variables
        super().__init__(endTime, rDir)

        # KCWI has the original file name
        self.ofName = 'OFNAME'
        self.camera = 'CAMERA'
        self.endHour = 'DATE-END'
        # Set the KCWI specific paths to anc and stage
        seq = (self.rootDir, '/KCWI/', self.utDate, '/anc')
        self.ancDir = ''.join(seq)
        seq = (self.rootDir, '/stage')
        self.stageDir = ''.join(seq)
        # Generate the paths to the KCWI datadisk accounts
        self.paths = self.get_dir_list()


    def get_dir_list(self):
        '''
        Function to generate the paths to all the KCWI accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata1400/kcwi'
        for i in range(1,10):
            joinSeq = (path, str(i))
            path2 = ''.join(joinSeq)
            dirs.append(path2)
        joinSeq = (path, 'dev')
        path2 = ''.join(joinSeq)
        dirs.append(path2)
        return dirs

    def get_prefix(self, keys):
        instr = self.get_instr(keys)
        if instr == 'kcwi':
            try:
                camera = keys['CAMERA'].lower()
            except KeyError:
                prefix = ''
            if camera == 'blue':
                prefix = 'KB'
            elif camera == 'red':
                prefix = 'KR'
            elif camera == 'fpc':
                prefix = 'KF'
            else:
                prefix = ''
        else:
            prefix = ''
        return prefix

