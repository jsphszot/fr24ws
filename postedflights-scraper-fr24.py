import requests
import json
import numpy as np
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from slacker import Slacker

startTimer = datetime.now()
monthDay = startTimer.strftime('%Y-%b-%d')
hourMins = startTimer.strftime('%H:%M')

# slack api connection - used to track process
# won't work unless you have a token to the slack channel
slack_api_token = "put-your-token-here"
slack_channel = "fr24ws-traqueocompetencia"
username = "HAL 9000"
slack = Slacker(slack_api_token)

msg = "{}\n\n{}: Corriendo proceso diario a las {}.\n...".format("- "*20, monthDay, hourMins)
slack.chat.post_message(channel=slack_channel, text=msg, username=username)

# Try to scrape, else report error
try:

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0"}

    # List of Airlines to track and airports to get Arrival/Departure info from
    CompetenciaList = [
        {'Airline': 'AviancaCargo', 'Code': 'qt-tpa','OrgDes': ['MIA', 'BOG', 'ASU', 'MVD','EZE'],},
        {'Airline': 'AtlasAir', 'Code': '5y-gti','OrgDes': ['MIA', 'VCP', 'EZE'],},
        {'Airline': 'Kalitta', 'Code': 'k4-cks','OrgDes': ['MIA'],},
        {'Airline': 'Centurion', 'Code': 'gg-kye','OrgDes': ['MIA','AGT'],},
        {'Airline': 'Cargolux', 'Code': 'cv-clx','OrgDes': ['LUX','VCP'],},
        {'Airline': 'MartinAir', 'Code': 'mp-mph','OrgDes': ['MIA','GUA','AMS','VCP'],},
        {'Airline': 'Lufthansa', 'Code': 'lh-dlh','OrgDes': ['VCP', 'MVD','EZE','GIG','CWB'],},
        {'Airline': 'Turkish', 'Code': 'tk-thy','OrgDes': ['DSS','MIA'],},
        {'Airline': 'Qatar', 'Code': 'qr-qtr','OrgDes': ['GRU','LUX'],},
        {'Airline': 'Korean', 'Code': 'ke-kal','OrgDes': ['MIA','VCP'],},
    ]

    datatable = []
    for element in CompetenciaList:

        airline_name = element['Airline']
        airline_code = element['Code']
        url1 = 'https://www.flightradar24.com/data/airlines/' + airline_code + '/routes'
        url2 = url1 + '?get-airport-arr-dep={}'

        s = requests.session()
        r = s.get(url1, headers=headers, verify=False)

        for abbr in element['OrgDes']:
            # must provide cookies from url1 to get into url2
            cookie = r.cookies.get_dict()
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0", "Content-Type": "application/json", "x-fetch": "true"}
            response = s.get(url2.format(abbr), cookies=cookie, headers=headers, verify=False).json()

            # response is in json format, loop to extract desired fields
            for arrdep in response:
                for country in response[arrdep]:
                    cntry_dict = response[arrdep]

                    for airport in cntry_dict[country]['airports']:
                        ato_dict = cntry_dict[country]['airports']

                        for flight in ato_dict[airport]['flights']:
                            flt_dict = ato_dict[airport]['flights']

                            for date in flt_dict[flight]['utc']:

                                date_dict = flt_dict[flight]['utc']
                                datarow = {}
                                datarow['Airline'] = airline_name
                                datarow['ArrDep'] = arrdep
                                datarow['Country'] = country
                                datarow['Airport'] = airport
                                datarow['AirportClicked'] = abbr
                                datarow['Flight'] = flight
                                datarow['Aircraft'] = date_dict[date]['aircraft']
                                datarow['Date'] = date
                                datarow['Time'] = date_dict[date]['time']
                                datarow['Timestamp'] = date_dict[date]['timestamp']
                                datarow['Offset'] = date_dict[date]['offset']
                                datatable.append(datarow)

    # Slack msg - finished scraping
    msg = "WebScrapping a fr24 finalizada\nIniciando formateo y escritura."
    slack.chat.post_message(channel=slack_channel, text=msg, username=username)

    df = pd.DataFrame(datatable)
    df['HMS'] = [datetime.fromtimestamp(x).strftime('%H:%M:%S') for x in df['Timestamp']]
    df['Org'] = np.where(df['ArrDep']=='arrivals', df['Airport'], df['AirportClicked'])
    df['Des'] = np.where(df['ArrDep']=='arrivals', df['AirportClicked'], df['Airport'])

    FechaVista = datetime.now().strftime('%Y-%m-%d')
    df['FechaVista'] = FechaVista

    selectCols = ['Airline', 'Org', 'Des', 'Flight', 'Aircraft', 'Date', 'HMS', 'ArrDep', 'FechaVista']
    df = df[selectCols]

    # Write to shared folder
    write_to = 'shared-folder-goes-here'
    df.to_csv(write_to + 'SeguimientoCompetencia{}.csv'.format(datetime.now().strftime('%Y%m%d')), index=False)

    # Slack msg - Written to shared folder
    msg = "CSV escrito a carpeta Registro.\nIniciando carga a GBQ."
    slack.chat.post_message(channel=slack_channel, text=msg, username=username)


    # Upload to GBQ - requieres json authentication file
    client = bigquery.Client('ed-cm-caranalytics-dev')

    schema = [
        bigquery.SchemaField('Airline', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Org', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Des', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Flight', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Aircraft', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Date', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('HMS', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('ArrDep', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('FechaVista', 'STRING', mode='REQUIRED'),
        ]

    dataset_name = 'fr24WSxRgn'
    table_name = 'df_fr24wsTraqueoCompetencia'

    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    ## create table if doesn't exist, delete
    client.create_table(table, exists_ok=True)
    #client.delete_table(table_ref)


    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = 'CREATE_IF_NEEDED'
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema = schema

    client.load_table_from_dataframe(dataframe=df,
                                     destination=table_ref,
                                     job_config=job_config).result()

    msg = "Carga a GBQ finalizada:\nTabla [df_fr24wsTraqueoCompetencia]\n Dataset [fr24WSxRgn]\n{}".format("- "*20)
    slack.chat.post_message(channel=slack_channel, text=msg, username=username)

except Exception as e:
    msg = "Process faiulure:\n`{}`\n{}".format(str(e),"- "*20)
    slack.chat.post_message(channel=slack_channel, text=msg, username=username)
