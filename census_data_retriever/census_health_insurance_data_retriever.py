# -*- coding: utf-8 -*-
"""
Created on Sun Aug  7 22:43:53 2022

@author: nical
"""

import os,requests,sqlite3

os.chdir('C:/Users/nical/Documents/py4e/Capstone/week5')

key="89d97a0ee5a2045e83c3e51da330bb8fd2cd3ca1"
host = 'http://api.census.gov/data'
year = 'timeseries'
dataset = 'healthins/sahie'

base_url = '/'.join([host,year,dataset])
get_vars = ['NIC_PT','NUI_PT','RACECAT','RACE_DESC','SEXCAT','SEX_DESC',
            'GEOID']

predicates = {}
predicates['get'] = ','.join(get_vars)
predicates['for'] = 'county:*'
predicates['in'] = 'state:*'
predicates['time'] = '2020'
predicates['key'] = '89d97a0ee5a2045e83c3e51da330bb8fd2cd3ca1'

conn = sqlite3.connect('y2020.sqlite')
cur = conn.cursor()


cur.execute('''DROP TABLE IF EXISTS y2020''')

cur.execute('''CREATE TABLE IF NOT EXISTS y2020
    (id INTEGER PRIMARY KEY,
     NIC_PT TEXT,
     NUI_PT TEXT,
     RACECAT INTEGER NOT NULL,
     RACE_DESC TEXT NOT NULL,
     SEXCAT INTEGER NOT NULL,
     SEX_DESC TEXT NOT NULL,
     state INTEGER NOT NULL,
     county INTEGER NOT NULL,
     GEOID INTEGER NOT NULL)''')

s = dict()

_id=0
r = requests.get(base_url, params = predicates)
lst=r.json()
if len(lst) > 0:
    lst1=lst[1:]
    nic=[item[0] for item in lst1]
    nui=[item[1] for item in lst1]
    racecat=[int(item[2]) for item in lst1]
    racedesc=[item[3] for item in lst1]
    sexcat=[int(item[4]) for item in lst1]
    sexdesc=[item[5] for item in lst1]
    geo=[int(item[6]) for item in lst1]
    state=[int(item[8]) for item in lst1]
    county=[int(item[9]) for item in lst1]
    
    for i in range(len(state)):
        cur.execute('''INSERT OR IGNORE INTO y2020 (id,
                    NIC_PT,
                    NUI_PT,
                    RACECAT,
                    RACE_DESC,
                    SEXCAT,
                    SEX_DESC,
                    state,
                    county,
                    GEOID) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''',
                    ( _id, nic[i], nui[i], racecat[i], racedesc[i],
                     sexcat[i], sexdesc[i], state[i], county[i], geo[i]))
        _id += 1
    r.close()
conn.commit()
cur.close()



base_url = 'https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697'
get_vars = ['FIPS']
predicates = {}
predicates['get'] = ','.join(get_vars)
r = requests.get(base_url, params = predicates)
lst=r.json()


# print(r.json()[1])

