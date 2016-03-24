# ETL from ftp://ftp.nasdaqtrader.com/symboldirectory

from ftplib import FTP
import csv
import numpy as np
import urllib
import os

# Connect

def ftpConnect(hostname,username,userpassword):
  ftp = FTP(hostname)
  print('Calling Connect')
  ftp.login(username,userpassword)
  return ftp

# retrieve files
def getSymbolfiles(ftpfiles):
    ftpobject = ftpConnect('ftp.nasdaqtrader.com','anonymous','anonymous@')
    ftpobject.cwd('symboldirectory')
    for filename in ftpfiles:
        print ('Copying - ', filename)
        gFile = open(filename, "wb")
        ftpobject.retrbinary('RETR '+filename, gFile.write)
        gFile.close()
    print ('Copying is done')
    ftpobject.quit()

# retrieve csv file name and URL
def getCvsContent(filename,attributes):
    with open(filename, attributes) as csvfile:
        filereader = csv.reader(csvfile, delimiter='|')
        #for row in filereader:
        #    print ', '.join(row)
        return filereader

# retriev csv file symbols
def getSymbol(object):
    pass

# dump symbols into database
def saveSymbolsToDb(object):
    pass

def readFileToList (filename):
    try:
        with open (filename, "r") as myfile:
            filecontentlist=myfile.read().splitlines()
    except ValueError:
        print("Error in data.")
    return filecontentlist

def make_filename(output_path, ticker_symbol):
    return output_path + '\\' + ticker_symbol + '.csv'

def make_url(ticker_symbol, base_url):
    return base_url + ticker_symbol

def pull_historical_data(ticker_symbol, base_url, sharesfolder):
    try:
        urllib.urlretrieve(make_url(ticker_symbol, base_url), make_filename(ticker_symbol, sharesfolder))
    except urllib.ContentTooShortError as e:
        outfile = open(make_filename(ticker_symbol, sharesfolder), "w")
        outfile.write(e.content)
        outfile.close()

#################################
#             Main              #
#################################
currentpath = os.getcwd()
#ftplist = ['bxoptions.txt','bxo_lmm.csv','bxtraded.txt','mfundslist.txt','mpidlist.txt','nasdaqlisted.txt','nasdaqtraded.txt','options.txt','otclist.txt','otherlisted.txt','phlxoptions.csv','psxtraded.txt']
base_url = "http://ichart.finance.yahoo.com/table.csv?s="
sharespfolder = 'yahoodata'
sharespfolderpath = currentpath + '\\'+ sharespfolder
symbollistfolder = 'nasdaqlist'
symbollistfolderpath = currentpath + '\\' + symbollistfolder

symbollistfile = ['nasdaqtraded_symbols.txt']
symbollistfilepath = symbollistfolderpath + '\\' + symbollistfile[0]

print (sharespfolderpath,symbollistfolderpath,symbollistfilepath)

symbollistcount = len(symbollistfile)

for i in range(0, symbollistcount):
    symbollist = readFileToList(symbollistfilepath)
    symbollistcount = len(symbollist)
    for j in range(0, 5):
        pull_historical_data(symbollist[j], base_url, sharespfolderpath)
