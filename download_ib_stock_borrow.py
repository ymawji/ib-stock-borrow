import psycopg2
import tempfile
import logging

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Numeric, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ftplib import FTP
import csv
import datetime
import time

from numpy import nan
from functools import wraps

TABLE_NAME='stock_loan'
MAX_FTP_RETRIES = 10

logging.basicConfig(filename='iborrow_log.txt',format='[%(asctime)s] %(message)s',level=logging.INFO)

Base = declarative_base()

class stock_loan(Base):
    __tablename__ = TABLE_NAME
    
    id = Column(Integer,primary_key=True)
    symbol = Column(String)
    timestamp = Column(DateTime)
    country = Column(String)
    currency = Column(String(3))
    isin = Column(String(12))
    name = Column(String(200))
    feerate = Column(Numeric)
    rebaterate = Column(Numeric)
    available = Column(BigInteger)
    
    def __repr__(self):
        return "<%s(symbol='%s', timestamp='%s', country='%s', currency='%s', isin='%s', name='%s', feeRate='%s', rebateRate='%s', available='%s')>" % (TABLE_NAME,
        self.symbol, self.timestamp, self.country, self.currency, self.isin, self.name, self.feerate, self.rebaterate, self.available)
    

def timer(f):
    @wraps(f)
    def decorated(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        msg = f.__name__ + ' took {} seconds'.format(te - ts)
        print(msg)
        logging.info(msg)
        return result

    return decorated


def retry(f,num_retries=MAX_FTP_RETRIES,sleep_sec=2):
    def decorated(*args, **kw):
        actual_retries=0
        while actual_retries <= num_retries:
            try:
                result = f(*args, **kw)
                return result
            except:
                actual_retries += 1
                time.sleep(sleep_sec)
                
        raise Exception('Too many retries.')
            

    return decorated    


class ftp_with_retries(FTP):
    
    @retry
    def __init__(*args, **kw):
        FTP.__init__(*args, **kw)


class Borrow:

    @timer
    def __init__(self,engine,country_list=['australia', 'austria', 'belgium', 'british', 'canada',
                             'dutch', 'france', 'germany', 'hongkong', 'india', 'italy',
                             'japan', 'mexico', 'spain', 'swedish', 'swiss', 'usa']):

        self.engine = engine
        self.country_list = country_list
        
    def get_records(self, country_list):
        self.records = self._download_files(country_list)
        return self.records
    
        
    @timer    
    def update(self):
        self.records = self._download_files(self.country_list)
        self.bulk_insert()
        msg = 'Update Successful'
        print msg
        logging.info(msg)
    
    @timer
    def _parse_file(self,fh,country):
        
        rows = []
     
        #parse timestamp
        timestamp_row = fh.readline().split('|')
        date_str = timestamp_row[1]
        time_str = timestamp_row[2]
        timestamp = datetime.datetime.strptime(date_str+"."+time_str[0:5],'%Y.%m.%d.%H:%M')
        
        stockreader = csv.DictReader(fh, delimiter='|')
        for row in stockreader:
            rows.append(row)
                
        records = []
        for row in rows[:-1]: #skip last EOF row
            if row['FEERATE']=='NA':
                row['FEERATE']=nan
                
            if row['REBATERATE']=='NA':
                row['REBATERATE']=nan
                
            record = {'symbol': row['#SYM'], 'timestamp': timestamp, 'country':country, 'currency': row['CUR'], 'isin': row['ISIN'],
                      'name':row['NAME'], 'feerate':row['FEERATE'], 'rebaterate':row['REBATERATE'], 'available':int(row['AVAILABLE'].replace('>',''))}    
            records.append(record)
        
                
            
        return records
            
    @timer
    def _download_files(self,country_list):
        conn_ftp = ftp_with_retries('ftp3.interactivebrokers.com', 'shortstock')
        
        allRecords=[]
        for country in country_list:
            with tempfile.TemporaryFile('wb+') as temp_curr:
                path = country + '.txt'
                conn_ftp.retrbinary('RETR %s' % path, temp_curr.write)
                temp_curr.seek(0,0)
                
                #parse file and add to list
                allRecords += self._parse_file(temp_curr,country)
                
        return allRecords
               
    @timer
    def bulk_insert(self):
        self.engine.execute(
            stock_loan.__table__.insert(),self.records)
        

    
    
    
if __name__=="__main__":
    engine = create_engine('postgresql+psycopg2://username:password@hostname/my_db_name')
      
    #Base.metadata.create_all(engine)  #if the table does not already exist

    borrow = Borrow(engine)
    borrow.update()
    
