import datetime as dt
import requests
import pandas as pd
import json
import parquet
import pyarrow as pa
import pyarrow.parquet as pq

def ETL_script():
    # URL til en API-endpoint
    url = "https://www.varmelast.dk/api/v1/heatdata?contextSite=varmelast_dk"

    response = requests.get(url)  # sending a GET request
    response.raise_for_status() # check status codes
    print(response)
    data = response.json()        # Converts the reply to a Python dict
    new_list = []
    for i in range(len(data["data"])):
        new_list.append(data["data"][i]["key"])
    for i in range(len(data["legend"])):
        if data["legend"][i]["key"] in new_list:
            print(data["legend"][i]["key"],": ", data["legend"][i]["title"])
    #['timestamp', 'accDirection', 'heatDirection', 'vfToCtr', 'vfToVeks', 'objects', 'legend', 'data']
    # Forklaring:
        # timestamp -> dato
        # accDirection -> 1.0 (antageligt værdi fra 0.0 til 1.0)
        # heatDirection -> (antageligt værdi fra 0.0 til 1.0)
        # vfToCtr -> (antageligt værdi fra 0.0 til 1.0)
        # vfToVeks -> (antageligt værdi fra 0.0 til 1.0)
        # objects -> liste med 31 selskaber (se nedenfor) der indeholder dictionaries med følende keys: ['text', 'link', 'posX', 'posY', 'type', 'key', 'title', 'backgroundColor', 'borderColor', 'showOnGraph', 'showOnGraphLegend']
            # 0 Amagerværket
            # 1 ARC, Amager Ressource Center
            # 2 ARGO - Roskilde Kraftvarmeværk
            # 3 Avedøreværket
            # 4 City 2 Fjernkøling varmepumpe
            # 5 Copenhagen Markets Varmepumpe
            # 6 CP Kelko overskudsvarme
            # 7 Energicentralen Fjernkøling Varmepumpe
            # 8 Frederiksberg vandværk (varmepumpe)
            # 9 GLC2 elkedel
            # 10 Global Connect varmepumpe
            # 11 Hedehusene solvarme
            # 12 Høje Tåstrup Damvarmelager
            # 13 Ishøj Biomasse
            # 14 Køge Kraftvarmeværk
            # 15 Kopenhagen Fur
            # 16 Lynetten Renseanlæg (overskudsvarme)
            # 17 Mølleholmen grundvand varmepumpe
            # 18 Nordea varmepumpe
            # 19 Novozymes Overskudsvarme
            # 20 NYC Elkedel
            # 21 H.C. Ørsted Værket
            # 22 Rockwool Varmepumpe
            # 23 Roskilde Renseanlæg varmepumpe
            # 24 Sjællandsbroens varmepumpe
            # 25 Solrød biogas
            # 26 Tårnby Varmepumpe
            # 27 Tiegtensgade varmepumpe
            # 28 Varmeakkumulator ved Amagerværket
            # 29 Varmeakkumulatorer ved Avedøreværket
            # 30 Vestforbrænding
        # legend -> liste med 16 selskaber (se nedenfor) der indeholder dictionaries med følende keys: ['units', 'sortOrder', 'key', 'title', 'backgroundColor', 'borderColor', 'showOnGraph', 'showOnGraphLegend']
            # 0 CTR
            # 1 VEKS
            # 2 Produktion i alt
            # 3 Affaldsenergianlæg
            # 4 Kraftvarmeanlæg
            # 5 Lokal produktion
            # 6 Spidslast biomasse
            # 7 Spidslast gas
            # 8 Spidslast olie
            # 9 Elkedler
            # 10 Varmepumper
            # 11 Industriel overskudsvarme
            # 12 Overskudsvarme datacentre
            # 13 Biogas
            # 14 Solvarme
            # 15 CO2 - Udledning
        # data -> liste med 16 selskaber der indeholder dictionaries med følende keys: ['key': 'value', 'valueError']



    #for keys in list(data.keys()):
    #    print(data[keys])
    #df_varmelast = pd.DataFrame(data["timestamp"]["text"]) # Saves the data as a Pandas data frame
    #print(df_varmelast)
    #pa_elspotprices = pa.Table.from_pandas(df_elspotprices) # Converts Pandas data frame to Parquet

    #todays_date = dt.date.today()

    #pq.write_table(pa_elspotprices, f'elspotprices_{todays_date}') # Saves the daily spot prices
ETL_script()