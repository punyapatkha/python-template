#import lib

import psycopg2
import pandas as pd
import logging
import sqlalchemy

#fuction declare


#initial log
logging.basicConfig(filename='pg_to_pg.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
#%%
logging.info('open log')

connection = psycopg2.connect(
         host= '',
         dbname= '',
         user= 'postgres',
         password= '',
         port= '5432'
     )
connection.cursor()

database_username = 'postgres'
database_password = ''
database_ip       = '127.0.0.1:5432'
database_name     = ''

database_connection = sqlalchemy.create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}'.
                                                       format(database_username, database_password, 
                                                              database_ip, database_name))

#list of table name
list_master_table = ['ADJUST_FACTOR','CALENDAR']
for i in list_master_table:

#for loop table names

    SQL_Query = pd.read_sql_query(
        '''SELECT * FROM "{i}" limit 10'''.format(i), connection)
    x = pd.DataFrame(SQL_Query)
    x = x.reset_index(drop=True)
    x.to_sql(con=database_connection, name=i, if_exists='append',index=False
        , method="multi", chunksize=1000)


logging.info('close log')
logging.shutdown()



def remove_old_data(target,site):
    conn = psycopg2.connect(host="",database="postgres", user="postgres", password='''        ''')
    cursor = conn.cursor()
    
    # change to master.loc[0,0] by using timestamp or using extract date(change name to update time)    
    querystring = '''DELETE FROM {table} WHERE site_name = '{site_id}';'''
    
    cursor.execute(querystring.format(table=target,site_id=site))
    
    conn.commit()

    return print("sucessful delete old rows")
