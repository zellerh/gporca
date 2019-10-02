#!/usr/local/bin/python
#
# gets the values of debug counters collected with the code in the
# CDebugCounter class, see file ../libgpos/include/gpos/common/CDebugCounter.h
#
# usage: get_debug_event_counters.py
#

import sys
import subprocess
import re
import argparse
import os

def run_command(command):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

def processLogFile(logFileLines, allruns):
    # every start marker indicates a new run, we usually print only the last such run,
    # unless allruns is set to true
    current_run_number = 0
    current_run_name = ""
    current_output = []

    for line in logFileLines:
        startMarkerMatch = re.search(r'CDebugCounterEventLoggingStart', line)
        counterLogMatch = re.search(r'CDebugCounterEvent\(', line)
        eofMatch = re.match(r'EOF', line)

        if startMarkerMatch:
            if (not allruns):
                # we are only interested in the last run, discard
                # all information about previous runs
                current_output = []
            current_run_number += 1
            # the run name is the starting timestamp
            current_run_name = re.sub(r'.*:([0-9]+-[^,]+),.*\n',
                                      r'\1',
                                      line)
            current_output.append("%d, 0, , %s, run_start_time, 0" % (current_run_number,current_run_name))
        elif counterLogMatch:
            csv = re.sub(r'.*CDebugCounterEvent\(.*\)\s*,\s*([^"]+).*\n',
                         r'\1',
                         line)
            # prepend the run number to the comma-separated values
            csv = "%d, %s" % (current_run_number, csv)
            current_output.append(csv)

        if startMarkerMatch and allruns or eofMatch:
            if len(current_output) > 1:
                for l in current_output:
                    print l


def main():
    parser = argparse.ArgumentParser(description='Get logged debug event counter data')
    parser.add_argument('--logFile',
                        help='GPDB log file saved from a run with debug event counters enabled (default is to search GPDB master log directory)')
    parser.add_argument('--allRuns', action='store_true',
                        help='record all runs, instead of just the last one, use this if you had several psql runs')

    args = parser.parse_args()

    logfile = args.logFile
    allruns = args.allRuns

    grep_command = 'grep CDebugCounterEvent '
    gather_command = [ 'sh',  '-c' ]

    if logfile is None:
        master_data_dir = os.environ['MASTER_DATA_DIRECTORY']
        if master_data_dir == None or len(master_data_dir) == 0:
            print "$MASTER_DATA_DIRECTORY environment variable is not defined, exiting"
            exit
        grep_command = grep_command + master_data_dir + '/pg_log/*.csv'
    else:
        grep_command = grep_command + logfile

    grep_command = grep_command + "; echo EOF"

    gather_command.append(grep_command)

    all_lines = run_command(gather_command)

    processLogFile(all_lines, allruns)

if __name__== "__main__":
    main()
