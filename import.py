from os import listdir
from os.path import isfile, join
import argparse
import re
import pandas as pd
from sqlalchemy import create_engine


parser = argparse.ArgumentParser(description='Import he data to the MySQL.')
parser.add_argument('--tableDir', metavar='tableDir', type=str, default="./",required=False,
                    help='The directory to look for files.')
parser.add_argument('--fileType', metavar='fileType', type=str, default="csv",required=False,
                    help='The files to be  considered (gz or CSV).')
parser.add_argument('--host', metavar='host', type=str, default="localhost",required=False,
                    help='The MySQL host.')
parser.add_argument('--port', metavar='port', type=int, default=3306,required=False,
                    help='The MySQL port.')
parser.add_argument('--dbName', metavar='dbName', type=str,required=True,
                    help='The MySQL port.')
parser.add_argument('--regex', metavar='regex', type=str, default=".*",required=False,
                    help='The MySQL port.')
parser.add_argument('--dbUser', metavar='dbUser', type=str, required=True,
                    help='The MySQL user.')
parser.add_argument('--dbPass', metavar='dbPass', type=str, required=True,
                    help='The MySQL password.')
args = parser.parse_args()


engine = create_engine("mysql+mysqldb://%s:%s@%s/%s"%(args.dbUser,args.dbPass,args.host,args.dbName))


dirFiles = [f for f in listdir(args.tableDir) if isfile(join(args.tableDir, f)) and f[-len(args.fileType):]==args.fileType and re.search(args.regex,f)]
for f  in dirFiles:
	print(f[:-(len(args.fileType)+1)])
	df=pd.read_csv(f)
	df.to_sql(name=f[:-(len(args.fileType)+1)], con=engine, if_exists='replace', index=False)