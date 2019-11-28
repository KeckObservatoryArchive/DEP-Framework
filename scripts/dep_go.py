from keckdrpframework.core.framework import Framework
from keckdrpframework.config.framework_config import ConfigClass
from keckdrpframework.models.arguments import Arguments
import subprocess
import time
import argparse
import sys
import importlib

from dep_pipeline.pipelines.dep_pipeline import dep_pipeline


def _parseArguments(in_args):
    description = "DEP pipeline CLI"
    parser = argparse.ArgumentParser(prog=f"{in_args[0]}", description=description)
    parser.add_argument('-c', dest="config_file", type=str, help="Configuration file")

    parser.add_argument('-instr', type=str, help='Instrument name')
    parser.add_argument('-utDate', type=str, default=None,
                        help='UTC Date (yyyy-mm-dd) to search for FITS files in prior 24 hours. Default is current date.')
    parser.add_argument('-tpx', type=int, nargs='?', default=0, help='Update TPX database?  [0, 1].  Default is 0.')
    parser.add_argument('-procStart', type=str, nargs='?', default=None,
                        help='(OPTIONAL) Name of process to start at. ["obtain", "locate", "add", "dqa", "lev1", "tar", "koaxfr"]. Default is "obtain".')
    parser.add_argument('-procStop', type=str, nargs='?', default=None,
                        help='(OPTIONAL) Name of process to stop at. ["obtain", "locate", "add", "dqa", "lev1", "tar", "koaxfr"]. Default is "koaxfr".')
    parser.add_argument('--searchDir', type=str, nargs='?', const=None,
                        help='(OPTIONAL) Directory to search (recursively) for FITS files.  Default search dirs are defined in instrument class files.')
    parser.add_argument('--reprocess', type=str, nargs='?', const=None,
                        help='(OPTIONAL) Set to "1" to indicate reprocessing old data (skips certain locate/search checks)')
    parser.add_argument('--modtimeOverride', type=str, nargs='?', const=None,
                        help='(OPTIONAL) Set to "1" to ignore modtime on files during FITS locate search.')
    parser.add_argument('--metaCompareDir', type=str, nargs='?', const=None,
                        help='(OPTIONAL) Directory to use for special metadata compare report for reprocessing old data.')
    parser.add_argument('--useHdrProg', type=str, nargs='?', const=None,
                        help='(OPTIONAL) Set to "force" to force header val if different.  Set to "assist" to use only if indeterminate (useful for processing old data).')
    parser.add_argument('--splitTime', type=str, nargs='?', const=None,
                        help='(OPTIONAL) HH:mm of suntimes midpoint for overriding split night timing.')
    parser.add_argument('--emailReport', type=str, nargs='?', default='0',
                        help='(OPTIONAL) Set to "1" to send email report whether or not it is a full run')

    args = parser.parse_args(in_args[1:])
    return args


if __name__ == "__main__":

    # get input parameters

    args = _parseArguments(sys.argv)
    instr = args.instr.upper()
    utDate = args.utDate
    tpx = args.tpx
    pstart = args.procStart
    pstop = args.procStop
    emailReport = args.emailReport

    config = ConfigClass(args.config_file)

    # config_dep = ConfigClass('config.live.ini')

    # configuration parameters from teh command line
    # configArgs = []
    if args.searchDir: config.dep.properties['SEARCH_DIR'] = args.searchDir
    if args.modtimeOverride: config.dep.MODTIME_OVERRIDE = args.modtimeOverride
    if args.reprocess: config.dep.REPROCESS = args.reprocess
    if args.metaCompareDir: config.dep.META_COMPARE_DIR = args.metaCompareDir
    if args.useHdrProg: config.dep.USE_HDR_PROG = args.useHdrProg
    if args.splitTime: config.dep.SPLIT_TIME = args.splitTime
    if args.emailReport: config.dep.EMAIL_REPORT = args.emailReport

    try:
        framework = Framework(dep_pipeline, config)
    except Exception as e:
        print("Failed to initialize framework, exiting ...", e)
        traceback.print_exc()
        sys.exit(1)

    # Create instrument object
    className = instr.capitalize()
    module = importlib.import_module('dep_pipeline.instruments.instr_' + instr.lower())
    framework.logger.info("Using module %s to initialize instrument class" % module.__name__)
    instrClass = getattr(module, className)
    instrObj = instrClass(className, utDate, config)
    instrObj.dep_init(False)
    framework.logger.info("Instrument class %s inizialized correctly" % instrClass.__name__)
    framework.context.instrObj = instrObj



    framework.logger.info("Framework initialized")
    arguments = Arguments()
    # print(arguments)
    framework.append_event('obtain', arguments)

    framework.start()
