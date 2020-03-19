import os
import sys
import time
import subprocess
import csv
import urllib
import json


file=open('file_of_errors_'+sys.argv[1]+'.csv','w')
file.write('source              destination             Error_Message           LFN             size(GB)\n')
#with open('file_of_errors_'+sys.argv[1]+'.csv', 'w') as f:

        #theWriter = csv.writer(f)      
        #theWriter.writerow(['Source ', 'Destination ', 'Error Message', 'LFN', 'Size (GB)'])

def node():
        url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes'
        result = json.load(urllib.urlopen(url))
        node = []
        for p in result['phedex']['node']:
                node.append(p['name'])
        return node

def errorlog(source, destination):
        '''
        1. iterate over all the source and destination
        2. import all the information in pandas dataframe 
    
        '''
        url = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/errorlog?from=' + source + '&to=' + destination
        result = json.load(urllib.urlopen(url))
        error = {}
        for p in result['phedex']['link']:
                for j in p['block']:
                        for e in j['file']:
                                lfn=e['name']
                                size=e['size']/1000000000.0
                                for d in e['transfer_error']:
                                        if str(d['detail_log']['$t']) not in error.keys():
                                                error[str(d['detail_log']['$t'])] = 1
                                                file=open('file_of_errors_'+sys.argv[1]+'.csv','a')
                                                file.write(sys.argv[1]+'                '+ destination+'                '+str(d['detail_log']['$t']) +'         '+lfn+'         '+str(size)+'\n')



                                        else:
                                                error[str(d['detail_log']['$t'])] += 1


        return error

def transfers_log(source):
        sites_name = node()
        for destination in sites_name:
                if (destination != source):
                        print errorlog(source, destination)
