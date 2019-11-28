"""
This is the class to handle all the DEIMOS specific attributes
DEIMOS specific DR techniques can be added to it in the future

12/14/2017 M. Brown - Created initial file
"""

import instrument
import datetime as dt
from common import *
import numpy as np

class Deimos(instrument.Instrument):

    def __init__(self, instr, utDate, rootDir, log=None):

        # Call the parent init to get all the shared variables
        super().__init__(instr, utDate, rootDir, log)


        # Set any unique keyword index values here
        self.keywordMap['OFNAME']       = 'DATAFILE'        
        self.keywordMap['FRAMENO']      = 'FRAMENUM'


        # Other vars that subclass can overwrite
        self.endTime = '19:00:00'   # 24 hour period start/end time (UT)


        # Generate the paths to the NIRES datadisk accounts
        self.sdataList = self.get_dir_list()


        # """
        # Values not included in superclass, specific to DEIMOS
        # """
        # # add the FCSIMGFI config file for deimos
        # self.fcsimgfi = 'FCSIMGFI'


    def get_dir_list(self):
        """
        Function to generate the paths to all the DEIMOS accounts, including engineering
        Returns the list of paths
        """
        dirs = []
        path = '/s/sdata100'
        for i in range(1,4):
            path2 = path + str(i)
            for j in range(1,21):
                path3 = path2 + '/deimos' + str(j)
                dirs.append(path3)
            seq = path2 + 'dmoseng'
            dirs.append(seq)
        return dirs


    def get_prefix(self):

        instr = self.get_instr()
        outdir = self.get_keyword('OUTDIR')
        
        if '/fcs' in outdir:
            prefix = 'DF'
        elif instr == 'deimos':
            prefix = 'DE'
        else:
            prefix = ''
        return prefix

