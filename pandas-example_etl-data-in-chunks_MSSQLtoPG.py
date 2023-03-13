# Get data from SQL Server directly into Pandas dataframe, then store that data into Postgres table

import sys
#import json
import pandas as pd
from sqlalchemy import create_engine #Note: Latest SQLalchemy2.x will not work with Pandas 1.5.x
import datetime

if len(sys.argv) == 1:
    print("Usage: ", sys.argv[0], "src-dbconnfile.conn target-dbconnfile sqlfilename.sql target_table [chunksize (default 100000 rows) startchunk (default 1) stopchunk (default row count/chunksize)]")
    print("      - filename.sql should contain any SELECT statement that will return the desired data from the source")
    print("      - Rows are retrieved in chunks specified with chunksize, startchunk, and stopchunk...defaults are used if those values are omitted")
    quit()

# Capture command-line arguments
src_connfile = sys.argv[1]  # Passed-in file containing database connection info
trg_connfile = sys.argv[2]  # Passed-in file containing database connection info
sqlfile = sys.argv[3]  # Passed-in filename containing SQL statements in JSON format
target_table = sys.argv[4]  # Passed-in table to append to

if len(sys.argv) > 5:  # They've passed in chunksize...use this to calculate OFFSET and FETCH
    chunksize = int(sys.argv[5])
else:
    chunksize = 100000

if len(sys.argv) > 6:  # They've passed in the startchunk
    startchunk = int(sys.argv[6])
else:
    startchunk = 1

if len(sys.argv) > 7:  # They've passed in the stopchunk, so we'll stop at that value or adjust it to the last chunk if it's too large
    stopchunk = int(sys.argv[7])
else:
    stopchunk = "To be calculated"  # If no stopchunk passed in, we'll calculate it below after getting a row count

print(f"{datetime.datetime.now()}: {sys.argv[0]} executing against {sys.argv[1]} {sqlfile} chunksize={chunksize}...startchunk={startchunk}...stopchunk={stopchunk}")

# Open a connection to the source database
with open(src_connfile) as f:
    src_conninfo = f.read()

engine_mssql = create_engine(src_conninfo, encoding='utf16', execution_options={"isolation_level": "READ UNCOMMITTED"})

# Open a connection to the target database
with open(trg_connfile) as f:
    trg_conninfo = f.read()

engine_pg = create_engine(trg_conninfo)


# Load the source SQL statement metadata from the passed-in filename
# This json file should contain 3 key:query pairs:
#  row-count-query:  This should return a row count from the source table...this is only run once.
#  id-range-query: This should return the min and max ID's within the chunk to be processed...run once per chunk loop, note the substitution variables
#  extract-data-query: This should return the rows to extract within the chunk...run once per chunk loop, note the substitution variables.
# Note: We have to use pandas for this...the base json package does not support multi-line text, but pandas dataframes does.
df_sql = pd.read_json(sqlfile, typ='ser')
#print(df_sql['row-count-query'])
#print(df_sql['id-range-query'])
#print(df_sql['extract-data-query'])

sql_count = df_sql['row-count-query']
df = pd.read_sql(sql_count, engine_mssql)
row_count = df.iat[0,0]
req_chunks = int(row_count // chunksize) + 1  # Do integer division instead of float division, then add 1 to the answer

if stopchunk == "To be calculated":  # If no stopchunk, was passed in, use the required cycles calc'ed from row count and chunksize
   stopchunk = req_chunks

if stopchunk > req_chunks:
    stopchunk = req_chunks

print(f"{datetime.datetime.now()}: Row count = {row_count}...total required chunks = {req_chunks}...using stopchunk = {stopchunk}")

current_chunk = startchunk

# Read the data from SQL server by querying for each chunk by using OFFSET and FETCH (similar to LIMIT and OFFSET)
while current_chunk <= stopchunk:
    
    # Calcuate this iterations OFFSET and FETCH values
    offset = chunksize * (current_chunk - 1)
    fetch = chunksize

    # replace the substitution variables in the id-range-query with the literal values
    sql_idrange = df_sql['id-range-query']
    sql_idrange = sql_idrange.replace('%numrows_to_skip%', str(offset))
    sql_idrange = sql_idrange.replace('%num_rows_to_return%', str(fetch))

    # Execute the query to get ID ranges to extract
    print(f".{datetime.datetime.now()}: Querying database for Chunk {current_chunk} with offset={offset} fetch={fetch}")
    df_idrange = pd.read_sql(sql_idrange, engine_mssql)

    start_id = df_idrange.iat[0,0]
    stop_id = df_idrange.iat[0,1]
    print(f"..{datetime.datetime.now()}: RowID range for chunk {current_chunk}: start_id={start_id}, stop_id={stop_id}")
    
    # Replace the substitution variables in the extract-data-query with the literal values
    sql_extract = df_sql['extract-data-query']
    sql_extract = sql_extract.replace('%start_id%', str(start_id))
    sql_extract = sql_extract.replace('%stop_id%', str(stop_id))

    # Execute the the query to extract data
    df_extract = pd.read_sql(sql_extract, engine_mssql)
   
    # Write the data to a JSON file. Why JSON? Coments include CRLF's, which confuse CSV interpreters.  Pandas JSON solves that issue.
#    filename = 'data/' + outfile_prefix + str(chunksize) + '-' + str(current_chunk).zfill(4) + '.json.gz'
#    pd.set_option('display.max_columns', None)
#    print("Offending rows:\n", df_extract.iloc[6:9])
#    quit()

#    df_extract.to_json(filename, orient='records', date_format='iso', compression={'method': 'gzip'})
#    df_extract.to_json(filename, orient='records', date_format='iso', compression={'method': 'gzip'}, default_handler=str)
#    df_extract.to_json(filename, orient='records', date_format='iso', compression={'method': 'gzip'}, force_ascii=False)
#    print(f"..{datetime.datetime.now()}: Chunk {current_chunk} w/{len(df_extract)} rows to file: {filename}")

    df_extract.to_sql(target_table,engine_pg,if_exists='append', index=False)
    print(f"..{datetime.datetime.now()}: Chunk {current_chunk} w/{len(df_extract)} rows inserted into table {target_table}")

    current_chunk = current_chunk + 1


print(f"{datetime.datetime.now()}: {sys.argv[0]} execution complete\n")
