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
    dfs = []
    non_dfs = {}
    #print(list(data.keys()))
    for key in list(data.keys()):
        try:
            dfs.append(pd.DataFrame(data[key]))
        except:
            #print(f"No data frame to store for {key}")
            non_dfs[key] = data[key]
    
    #for i in range((len(dfs))):
    #    print(dfs[i].keys(), dfs[i])
    #for i in list(non_dfs.keys()):
    #    print(i, ": ", non_dfs[i])
    #print(df)
    # data indeholder følgende liste ['timestamp', 'accDirection', 'heatDirection', 'vfToCtr', 'vfToVeks', 'objects', 'legend', 'data']
    # Forklaring:
        # timestamp     -> tidsstempel
        # accDirection  -> 1.0 (antageligt værdi fra 0.0 til 1.0)
        # heatDirection -> (antageligt værdi fra 0.0 til 1.0)
        # vfToCtr       -> (antageligt værdi fra 0.0 til 1.0)
        # vfToVeks      -> (antageligt værdi fra 0.0 til 1.0)
        # objects       -> liste med 31 selskaber (se nedenfor) der indeholder dictionaries med følende keys: ['text', 'link', 'posX', 'posY', 'type', 'key', 'title', 'backgroundColor', 'borderColor', 'showOnGraph', 'showOnGraphLegend']
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
        # legend        -> liste med 16 selskaber (se nedenfor) der indeholder dictionaries med følende keys: ['units', 'sortOrder', 'key', 'title', 'backgroundColor', 'borderColor', 'showOnGraph', 'showOnGraphLegend']
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
        # data          -> liste med 16 selskaber der indeholder dictionaries med følende keys: ['key': 'value', 'valueError']
            #   'key'               : 'title' (fra legend)
            # BE-EO-CTR-EFF         :  CTR
            # DAP-VEKS-FORBRUG-EFF  :  VEKS
            # TOTAL                 :  Produktion i alt
            # BE-VL-AFFALD-EF       :  Affaldsenergianlæg
            # BE-VL-KRAFTV-EF       :  Kraftvarmeanlæg
            # LOCAL                 :  Lokal produktion
            # BE-VL-BIO-EF          :  Spidslast biomasse
            # BE-VL-SPIDS-GAS-EF    :  Spidslast gas
            # BE-VL-SPIDS-OLIE-EF   :  Spidslast olie
            # BE-VL-EVO-EF          :  Elkedler
            # BE-VL-VP-EF           :  Varmepumper
            # BE-VL-IO-EF           :  Industriel overskudsvarme
            # BE-VL-OD-EF           :  Overskudsvarme datacentre
            # BE-VL-BG-EF           :  Biogas
            # BE-VL-SOL-EF          :  Solvarme
            # BE-VL-TOTAL-FAK       :  CO2 - Udledning


    #for keys in list(data.keys()):
    #    print(data[keys])
    #df_varmelast = pd.DataFrame(data["timestamp"]["text"]) # Saves the data as a Pandas data frame
    #print(df_varmelast)
    #pa_elspotprices = pa.Table.from_pandas(df_elspotprices) # Converts Pandas data frame to Parquet

    #todays_date = dt.date.today()

    #pq.write_table(pa_elspotprices, f'elspotprices_{todays_date}') # Saves the daily spot prices
    return dfs, non_dfs
#dfs, non_dfs = ETL_script()