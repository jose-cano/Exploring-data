import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sodapy import Socrata
import mysql.connector

college_names = ["MORRISVILLE STATE COLLEGE", "SUNY COBLESKILL", "SUNY COLLEGE OF TECH AT ALFRED", "SUNY COLLEGE OF TECH AT CANTON",
                 "SUNY COLLEGE OF TECH AT DELHI", "SUNY COLLEGE OF TECH FARMINGDALE", "COLUMBIA UNIV", "TROCAIRE COLLEGE",
                 "SYRACUSE UNIVERSITY UTICA COLLEGE",
                 "LONG ISLAND UNIVERSITY", "CONCORDIA COLLEGE", "NEW YORK SCHOOL INTERIOR DESIGN",
                 "ROCHESTER INST TECH 4YR UNDERGRAD", "ALFRED UNIVERSITY 4YR UNDERGRAD", "ADELPHI UNIVERSITY 4 YR UNDERGRAD"]


college_id = [2895, 2892, 126, 2891, 2893, 2894, 3853,
              1404, 1269, 1433, 1340, 2133, 4105, 3220, 1392]
appended_data = []
for i in range(0, len(college_id)):
    url = f'https://api.chronicle.com/dl/q/bo/public/format/jsonp/name/cb_tuition_2020//order/year_pub:asc/fields/year_pub%7Ctuition_and_fees_in_state%7Ctuition_and_fees_out_of_state%7Croom_and_board%7Csector/find/hide:0:eq%7Corgid:{college_id[i]}:eq/callback/chegTableLive1CB?callback=chegTableLive1CB'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    tuit = str(soup).replace('chegTableLive1CB', '')
    tuit_dict = tuit[1:-1]
    data = json.loads(tuit_dict)
    tuition_data = pd.DataFrame(data)
    tuition_data['college_name'] = college_names[i]
    appended_data.append(tuition_data)


appended_data = pd.concat(appended_data, ignore_index=True)
appended_data.to_csv('top15_data.csv', index=False)


mydb = mysql.connector.connect(
    host="localhost",
    user="jose",
    password="0422",
    database="final_project"
)

mycursor = mydb.cursor()

sql = "INSERT INTO raw_data (year_pub, tuition_and_fees_in_state, tuition_and_fees_out_of_state, room_and_board, sector, college_name) VALUES (%s, %s, %s, %s, %s, %s)"
val = []

for x in appended_data.values:
    val.append(tuple(x))

mycursor.executemany(sql, val)

mydb.commit()
