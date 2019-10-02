#!/usr/local/bin/python
#
# gets the values of debug counters collected with the code in the
# CDebugCounter class, see file ../libgpos/include/gpos/common/CDebugCounter.h
#
# usage: get_debug_event_counters.py [--logFile <file>] [--allRuns]
#
# Description:
#
# We start by getting relevant log file lines using grep, looking for the pattern
# "DebugCounterEvent".
#
# The log file is either specified by the user, or we grep the log files
# $MASTER_DATA_DIRECTORY/pg_log/*.csv
#
# The log may contain multiple "runs". A run is a series of log entries made by
# the same process. Note that this tool doesn't support concurrent processes logging.
#
# Finally, for each run, we massage the log file lines into a CSV format by removing
# leading and trailing elements. The result is a CSV file printed to stdout.

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
	header_printed = False
	current_run_number = 0
	current_run_name = ""
	current_output = []

	for line in logFileLines:
		utf8_line = line.decode('utf-8')
		start_marker_match = re.search(r'CDebugCounterEventLoggingStart', utf8_line)
		counter_log_match = re.search(r'CDebugCounterEvent\(', utf8_line)
		eof_match = re.match(r'EOF', utf8_line)

		if start_marker_match or eof_match:
			if len(current_run_name) > 0:
				# we are at the end of a run (and possibly the beginning of a new one)
				# if allruns is True then print this run, otherwise print it only when we are at EOF
				if allruns or eof_match:
					if not header_printed:
						# print this header only once
						print("run, query num, query name, counter name, counter_type, counter_value")
						header_printed = True
					# print a dummy counter line that indicates the starting time of the run
					print("%d, 0, , %s, run_start_time, 0" % (current_run_number, current_run_name))
					# now print the actual counter values
					for l in current_output:
						print(l)

			# prepare for the next run by cleaning out our output list, this happens whether
			# we printed the previous run or whether we ignored it
			current_output = []

			if (not allruns):
				# we are only interested in the last run, discard
				# all information about previous runs
				current_run_number = 0

			# assign a number and a name to a run that may be following
			current_run_number += 1
			# the run name is the starting timestamp
			current_run_name = re.sub(r'.*:([0-9]+-[^,]+),.*\n',
									  r'\1',
									  utf8_line)
		elif counter_log_match:
			# we encountered the log entry for one event, remove prefix and suffix and
			# extract the comma-separated values in-between
			csv = re.sub(r'.*CDebugCounterEvent\(.*\)\s*,\s*([^"]+).*\n',
						 r'\1',
						 utf8_line)
			# prepend the run number to the comma-separated values
			csv = "%d, %s" % (current_run_number, csv)
			current_output.append(csv)



def main():
	parser = argparse.ArgumentParser(description='Get logged debug event counter data')
	parser.add_argument('--logFile',
						help='GPDB log file saved from a run with debug event counters enabled (default is to search '
							 'GPDB master log directory)')
	parser.add_argument('--allRuns', action='store_true',
						help='record all runs, instead of just the last one, use this if you had several psql runs')

	args = parser.parse_args()

	logfile = args.logFile
	allruns = args.allRuns

	grep_command = 'grep CDebugCounterEvent '
	gather_command = ['sh', '-c']

	if logfile is None:
		if 'MASTER_DATA_DIRECTORY' in os.environ:
			master_data_dir = os.environ['MASTER_DATA_DIRECTORY']
		else:
			print("$MASTER_DATA_DIRECTORY environment variable is not defined, exiting")
			exit()
		grep_command = grep_command + master_data_dir + '/pg_log/*.csv'
	else:
		grep_command = grep_command + logfile

	grep_command = grep_command + "; echo EOF"

	gather_command.append(grep_command)

	all_lines = run_command(gather_command)

	processLogFile(all_lines, allruns)


if __name__ == "__main__":
	main()
