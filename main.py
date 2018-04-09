import urllib
import sys
import argparse
import getopt
import logging
import pandas as pd

USAGE_MSG = """
usage: python main.py -f <csv filename>
Options:
    -f --file   Name of csv file (with headers)
"""


def main(argv):

    # Get commandline options and arguments
    try:
        opts, args = getopt.getopt(argv, "f:", ['file='])
    except getopt.GetoptError as e:
        print('[ERROR]: incorrect usage', e)
        print(USAGE_MSG)
        sys.exit(2)

    filename = None

    # Process command line options and arguments
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(USAGE_MSG)
            sys.exit()
        if opt in ("-f", "--file"):
            filename = arg

    # End program if no filename was passed
    if filename is None:
        print(USAGE_MSG)
        sys.exit()

    # Configure logging
    logging.basicConfig(
        level=logging.ERROR,
        filename='main.log',
        format='%(relativeCreated)6d %(threadName)s %(message)s')

    # Load csv
    df = pd.read_csv(filename, header=0)

    # Load settings



if __name__ == '__main__':
    main(sys.argv[1:])