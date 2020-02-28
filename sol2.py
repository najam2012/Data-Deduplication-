from __future__ import print_function
from future.builtins import next

import os
import csv
import re
import collections
import logging
import optparse
import numpy

import dedupe
from unidecode import unidecode

output_file = 'data_matching_output.csv'
settings_file = 'data_matching_learned_settings'
training_file = 'data_matching_training.json'


def preProcess(column):
    """
        Do a little bit of data cleaning with the help of Unidecode and Regex.
        Things like casing, extra spaces, quotes and new lines can be ignored.
        """
    column = unidecode(column)
    column = re.sub('\n', ' ', column)
    column = re.sub('-', '', column)
    column = re.sub('/', ' ', column)
    column = re.sub("'", '', column)
    column = re.sub(",", '', column)
    column = re.sub(":", ' ', column)
    column = re.sub('  +', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column:
        column = None
    return column


def readData(filename):
    """
     Read in our data from a CSV file and create a dictionary of records,
     where the key is a unique record ID.
     """
    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        fieldnames = ['ENTITYAGROUP_TRADE_REF', 'ENTITYAGROUP_ENTITY_NAME', ' CLIENT_ENTITY_NAME', 'TRADE_DATE',
                      'EFFECTIVE_DATE', 'CSA_ID',
                      'MATURITY_DATE',
                      'PRODUCT_NAME',
                      'NOTIONAL_CCY_CODE',
                      'NOTIONAL',
                      'NOTIONAL_CCY_CODE2'
                      'NOTIONAL2',
                      'STRIKE_RATE',
                      'ENTITYAGROUP_POSITION',
                      'PUT_CALL',
                      'INDEPENDENT_AMOUNT',
                      'PV_USD',
                      'PV',
                      'UNDERLYER_DESCRIPTION',
                      'PV_CCY_CODE',
                      'PV_VALUE_DATE']

        for j, row in enumerate(reader):
            clean_row = dict([(k, preProcess(v)) for (k, v) in row.items()])
            for i in list(clean_row):
                if i in fieldnames:
                    if i=='ENTITYAGROUP_TRADE_REF':
                        clean_row['Trade Reference']=clean_row[i][2:]

                        del clean_row[i]
                    elif i=='ENTITYAGROUP_ENTITY_NAME':
                        clean_row['Party A branch Name'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'CLIENT_ENTITY_NAME':
                        clean_row['Party B branch Name'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'TRADE_DATE':
                        clean_row['Trade Date'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'EFFECTIVE_DATE':
                        clean_row['Effective Date'] = clean_row[i]
                        del clean_row[i]
                    if i == 'CSA_ID':
                        clean_row['Agreement ID'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'MATURITY_DATE':
                        clean_row['Maturity Date'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'PRODUCT_NAME':
                        clean_row['Product Type'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'NOTIONAL_CCY_CODE':
                        clean_row['Exchanged Notional 1 Currency'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'NOTIONAL':
                        clean_row['Exchanged Notional 1 Amount'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'NOTIONAL_CCY_CODE2':
                        clean_row['Exchanged Notional 2 Currency'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'NOTIONAL2':
                        clean_row['Exchanged Notional 2 Amount'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'STRIKE_RATE':
                        clean_row['Strike Price'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'ENTITYAGROUP_POSITION':
                        clean_row['Buy/Sell'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'PUT_CALL':
                        clean_row['Put/Call'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'INDEPENDENT_AMOUNT':
                        clean_row['Independent Amount Party A'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'PV_USD':
                        clean_row['Valuation Base Currency Amount'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'PV':
                        clean_row['Valuation Native Currency Amount'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'UNDERLYER_DESCRIPTION':
                        clean_row['Underlying'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'PV_CCY_CODE':
                        clean_row['Valuation Native Currency'] = clean_row[i]
                        del clean_row[i]
                    elif i == 'PV_VALUE_DATE':
                        clean_row['Valuation Date'] = clean_row[i]
                        del clean_row[i]
                else:
                    del clean_row[i]


            # if clean_row['1']:
            #    clean_row['1'] = int(clean_row['1'])
            data_d[filename + str(j)] = dict(clean_row)

    return data_d
def readData2(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID.
    """

    data_d = {}

    with open(filename) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = dict([(k, preProcess(v)) for (k, v) in row.items()])
            #if clean_row['price']:
             #   clean_row['price'] = float(clean_row['price'][1:])
            clean_row['Trade Reference'] = clean_row['Trade Reference'][3:]
            data_d[filename + str(i)] = dict(clean_row)

    return data_d


print('importing data ...')
data_1 = readData('Data-set-A2.csv')
print('N data 1 records: {}'.format(len(data_1)))

data_2 = readData2('Data-set-B.csv')
print('N data 2 records: {}'.format(len(data_2)))


#
#def descriptions():
 #   for dataset in (data_1, data_2):
  #      for record in dataset.values():
   #         yield record['description']


if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as sf:
        linker = dedupe.StaticRecordLink(sf)

else:
    fields = [
        {'field': 'Agreement ID', 'type': 'ShortString'},
        {'field': 'Trade Reference', 'type': 'ShortString'},
        {'field': 'Party A branch Name', 'type': 'ShortString'},
        {'field': 'Product Type', 'type': 'String'},
        {'field': 'Party A branch Name', 'type': 'String'},
        {'field': 'Trade Date',  'type': 'DateTime','fuzzy': True,'dayfirst': True},
        {'field': 'Maturity Date',  'type': 'DateTime','fuzzy': True,'dayfirst': True},
        {'field': 'Effective Date',  'type': 'DateTime','fuzzy': True,'dayfirst': True},
        {'field': 'Exchanged Notional 1 Currency', 'type': 'ShortString','crf':True},
        {'field': 'Exchanged Notional 1 Amount', 'type': 'ShortString'},
        {'field': 'Valuation Date', 'type': 'DateTime', 'fuzzy': True, 'dayfirst': True},
        {'field': 'Valuation Native Currency', 'type': 'ShortString'},
        {'field': 'Valuation Native Currency Amount', 'type': 'ShortString'},
        {'field': 'Valuation Base Currency Amount', 'type': 'ShortString'},
      #  {'field': 'Independent Amount Party', 'type': 'Price','has missing':True},
        {'field': 'Strike Price', 'type': 'ShortString', 'has missing': True},
        {'field': 'Underlying', 'type': 'ShortString','has missing':True},
        {'field': 'Buy/Sell', 'type': 'ShortString', 'has missing':True},
        {'field': 'Put/Call', 'type': 'ShortString', 'has missing': True}




    ]

    linker = dedupe.RecordLink(fields)
    print("ok")
    linker.sample(data_2, data_1, 3000)
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            linker.readTraining(tf)
    print('starting active labeling...')
    dedupe.consoleLabel(linker)
    linker.train()
    print("inside")
    with open(training_file, 'w') as tf:
        linker.writeTraining(tf)
    with open(settings_file, 'wb') as sf:
        linker.writeSettings(sf)
print('clustering...')
linked_records = linker.match(data_2, data_1, 0)
print('# duplicate sets', len(linked_records))

cluster_membership = {}
cluster_id = None
for cluster_id, (cluster, score) in enumerate(linked_records):
    for record_id in cluster:
        cluster_membership[record_id] = (cluster_id, score)

if cluster_id:
    unique_id = cluster_id + 1
else:
    unique_id = 0

with open(output_file, 'w') as f:
    writer = csv.writer(f)

    header_unwritten = True

    for fileno, filename in enumerate(('Data-set-A2.csv', 'Data-set-B.csv')):
        with open(filename) as f_input:
            reader = csv.reader(f_input)

            if header_unwritten:
                heading_row = next(reader)
                heading_row.insert(0, 'source file')
                heading_row.insert(0, 'Link Score')
                heading_row.insert(0, 'Cluster ID')
                writer.writerow(heading_row)
                header_unwritten = False
            else:
                next(reader)

            for row_id, row in enumerate(reader):
                cluster_details = cluster_membership.get(filename + str(row_id))
                if cluster_details is None:
                    cluster_id = unique_id
                    unique_id += 1
                    score = None
                else:
                    cluster_id, score = cluster_details
                row.insert(0, fileno)
                row.insert(0, score)
                row.insert(0, cluster_id)
                writer.writerow(row)
