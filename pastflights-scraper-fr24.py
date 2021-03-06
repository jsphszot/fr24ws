import re
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import urllib3

# Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def processTime(startTime, format='s'):
    """
    startTime: a datetime object (usually datatime.now()).
    format: default 's', returns total seconds, 'f' returns min and secs.
    """
    totalTime = round((datetime.now() - startTime).total_seconds(), 2)
    ftotalTime = "{}m and {}s".format(int(totalTime//60), int(totalTime%60))
    if format == 's':
        return totalTime
    else:
        return ftotalTime

def reTime(x):
    return re.search('\d{2}:\d{2}', x)

# Airline input list
CompetenciaList = [
                    {'Airline': 'ABX', 'Code':'gb-abx',},
                    {'Airline': 'AeroLogic', 'Code':'3s-box',},
                    {'Airline': 'AeroUnion', 'Code':'6r-tno',},
#                     {'Airline': 'Aigle Azur', 'Code':'zi-aaf',},
                    {'Airline': 'AirACT', 'Code':'9t-run',},
                    {'Airline': 'Amerijet', 'Code':'m6-ajt',},
                    {'Airline': 'ASLAirlinesBelgium', 'Code':'3v-tay',},
                    {'Airline': 'AstralAviation', 'Code':'8v-acp',},
                    {'Airline': 'Atlas', 'Code':'5y-gti',},
                    {'Airline': 'Avianca', 'Code':'av-ava',},
                    {'Airline': 'AviancaCargo', 'Code':'qt-tpa',},
                    {'Airline': 'AzulCargo', 'Code':'ad-azu',},
                    {'Airline': 'CAL CargoAirlines', 'Code':'5c-icl',},
                    {'Airline': 'Cargo Air', 'Code':'cgf',},
                    {'Airline': 'Cargojet', 'Code':'w8-cjt',},
                    {'Airline': 'CargoLogicAir', 'Code':'p3-clu',},
                    {'Airline': 'Cargolux', 'Code':'cv-clx',},
                    {'Airline': 'CargoluxItalia', 'Code':'c8-icv',},
                    {'Airline': 'ChinaCargoAirlines', 'Code':'ck-ckk',},
                    {'Airline': 'EgyptAirCargo', 'Code':'msx',},
                    {'Airline': 'EmiratesSkyCargo', 'Code':'ek-uae',},
                    {'Airline': 'EthiopianCargo', 'Code':'et-eth',},
                    {'Airline': 'Etihad', 'Code':'ey-etd',},
                    {'Airline': 'Fedex', 'Code':'fx-fdx',},
                    {'Airline': 'KalittaAir', 'Code':'k4-cks',},
                    {'Airline': 'KLM', 'Code':'kl-klm',},
                    {'Airline': 'Korean', 'Code':'ke-kal',},
                    {'Airline': 'LufthansaCargo', 'Code':'gec',},
                    {'Airline': 'Martinair', 'Code':'mra',},
                    {'Airline': 'NorthernAirCargo', 'Code':'nc-nac',},
                    {'Airline': 'PolarAirCargo', 'Code':'po-pac',},
#                     {'Airline': 'QatarAirCargo', 'Code':'qac',},
                    {'Airline': 'QatarAirways', 'Code':'qr-qtr',},
                    {'Airline': 'SaudiArabianCargo', 'Code':'sv-sva',},
                    {'Airline': 'SingaporeAirlinesCargo', 'Code':'sqc',},
                    {'Airline': 'SkyLeaseCargo', 'Code':'gg-kye',},
                    {'Airline': 'Turkish', 'Code':'tk-thy',},
                    {'Airline': 'UPS', 'Code':'5x-ups',},
                    ]

# Start timer
startTimer = datetime.now()
WeekToday=startTimer.isocalendar()[1]

msg = ("\n{}\n{}\n{}\n".format("- "*20, startTimer.strftime('%Y-%m-%d %H:%M:%S'),"Starting New Process ..."))

# loop through Airline list, get Rgn from flightradar24 and save as df.
msg = "Getting Registration Numbers for required Airlines..."
print(msg)

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0"}

emptyList = []
for thing in CompetenciaList:
    Airline = thing['Airline']
    AirCode = thing['Code']
    url0 = 'https://www.flightradar24.com/data/airlines/' + AirCode + '/fleet'

    req1 = requests.get(url0, headers=headers, verify=False)
    soup = BeautifulSoup(req1.content, 'html5lib')
    dltag = soup.find('dl', attrs = {'id': 'list-aircraft'})

    AircraftTypeList = dltag.find_all('dt', attrs = {'class': None})
    TableBodyList = dltag.find_all('tbody')

    for i in range(len(AircraftTypeList)):
        AircraftType = AircraftTypeList[i].find('div').text
        tbodyList = [x.text.strip() for x in TableBodyList[i].find_all('a', attrs={'class': 'regLinks'})]
        for reggynum in tbodyList:
            emptyList.append([Airline, AircraftType, reggynum])

AircraftRgn_df = pd.DataFrame(emptyList, columns = ['Airline', 'Aircraft', 'RegNum'])

msg = "Finished scrapping Aircraft Rgn in {} seconds".format(processTime(startTimer))
print(msg)


# loop through Airline list, get Rgn from flightradar24 and save as df.
msg = "Looping over Registration Numbers..."
print(msg)

# Max info date (usually just use current date, unless info needed from a certain past point in time)
pageTimeStamp = int(datetime.timestamp(datetime.now()))

datatable = []
pageNum = 1
loopLen = len(AircraftRgn_df)
loopRange = range(loopLen)

for num in loopRange:
    numTimer = datetime.now()

    rgn = AircraftRgn_df['RegNum'][num]
    aln = AircraftRgn_df['Airline'][num]
    act = AircraftRgn_df['Aircraft'][num]

    url1 = 'https://www.flightradar24.com/data/aircraft/' + rgn
    url2 = ('https://api.flightradar24.com/common/v1/flight/list.json?query='
            + rgn
            + '&fetchBy=reg&page='
            + str(pageNum) +
            '&pk=&limit=100&token=uRqpBn1x2OXEYqowDH7rurXG1vEy6vyXwuVPtD3RPxo&timestamp='
            + str(pageTimeStamp))

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0"}
    s = requests.session()
    r = s.get(url1, headers=headers, verify=False)

    cookie = r.cookies.get_dict()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0", "Content-Type": "application/json", "x-fetch": "true"}
    response = None
    while response is None:
        try:
            response = s.get(url2, cookies=cookie, headers=headers, verify=False).json()
        except:
            pass
    
    try:
        data = response['result']['response']['data']
    except KeyError:
        data = None

    if data == None:
        next
    else:
        for row in data:
            # fill named data in list
            callsn = row['identification']['callsign'] # callsign
            fltnum = row['identification']['number']['default'] # flight number
            statusRaw = row['status']['text'] # status

            status=re.search('[A-Z|a-z]+', statusRaw).group(0) if reTime(statusRaw) else "-"
            statusTime=reTime(statusRaw).group(0) if reTime(statusRaw) else "-"

            deptime1=row['time']['real']['departure'] # utc of event
            deptime=None if deptime1 == None else datetime.fromtimestamp(deptime1).strftime('%Y-%m-%d %H:%M:%S')

            arrtime1 = row['time']['real']['arrival'] # utc of event
            arrtime = None if arrtime1 == None else datetime.fromtimestamp(arrtime1).strftime('%Y-%m-%d %H:%M:%S')

            try:
                orgato=row['airport']['origin']['code']['iata']
                orgoffset=row['airport']['origin']['timezone']['offset']
                deplocaltime=datetime.fromtimestamp(deptime1 + orgoffset).strftime('%Y-%m-%d %H:%M:%S')
                WeekUTC=int(pd.to_datetime(deptime).week)
            except TypeError:
                orgato=None
                deplocaltime=None
                WeekUTC=None

            try:
                desato=row['airport']['destination']['code']['iata']
                desoffset=row['airport']['destination']['timezone']['offset']
                arrlocaltime=datetime.fromtimestamp(arrtime1 + desoffset).strftime('%Y-%m-%d %H:%M:%S')
            except TypeError:
                desato=None
                arrlocaltime=None

            datatable.append([
                            aln,
                            act,
                            rgn,
                            callsn,
                            fltnum,
                            WeekUTC,
                            orgato,
                            desato,
                            deptime,
                            arrtime,
                            deplocaltime,
                            arrlocaltime,
                            status,
                            statusTime,
                            ])

    count=num+1
    rgnTime=processTime(numTimer)
    totalTime=processTime(startTimer, 'f')

    print(("{count}/{loopLen} in {rgnTime} seconds, total time: {totalTime} "
            .format(count=count,
                    loopLen=loopLen,
                    rgnTime=rgnTime,
                    totalTime=totalTime
                    )))

msg = "Finished looping in {}".format(processTime(startTimer, 'f'))
print(msg)

# write as DataFrame
AirIteCols=[
        'Airline',
        'Aircraft',
        'RegNum',
        'Callsign',
        'FlightNum',
        'WeekUTC',
        'Org',
        'Des',
        'DepartureUTC',
        'ArrivalUTC',
        'DepartureLT',
        'ArrivalLT',
        'Status',
        'StatusTime',
        ]

AircraftItinerario=pd.DataFrame(datatable, columns=AirIteCols)

WeekToday=startTimer.isocalendar()[1]
InfoWeek=startTimer.isocalendar()[1]-1
InfoWeekRange=list(range(InfoWeek-2, InfoWeek+1))

csv_name = f'CompRgnW{InfoWeek}.csv'
AircraftItinerario.query(f"Week in {InfoWeekRange}")\
    .to_csv(csv_name, index=False)
    
# for each data row, status must be Landed, Org and DepartureUTC must not be NaN, same for Des and ArrivalUTC
cleanfilt=AircraftItinerario.Org.notnull() & AircraftItinerario.Des.notnull() & (AircraftItinerario.Status == 'Landed')
testDF=AircraftItinerario[cleanfilt]

# Select only airplanes that go through MIA for wanted Airlines
cond1="(Org == 'MIA' or Des == 'MIA')"
cond2="Airline in ['Atlas','Avianca', 'AviancaCargo','KalittaAir','Korean','SkyLeaseCargo','Turkish',]"
miaflights=list(set(testDF.query(f"{cond1} and {cond2}").RegNum))

ruteoList=[]
for rgn in miaflights:

    castor=testDF.query(f" RegNum == '{rgn}' ").sort_values(by=['DepartureUTC'])

    aln=castor.iloc[0]['Airline']
    rgninfo=[aln, rgn]

    indexlist=[]
    for j in list(range(len(castor))):

        if castor.iloc[j].Org == 'MIA':
            indexlist.append(j)

    for p in list(range(len(indexlist)-1)):
        castor2=castor.iloc[indexlist[p]:(indexlist[p+1]+1)]
        dep=castor.iloc[indexlist[p]].DepartureLT
        dwk=pd.to_datetime(dep).week
        arv=castor.iloc[(indexlist[p+1])-1].ArrivalLT
        rtg="-".join(castor2.Org)

        rgninfo2=[dwk, rtg, dep, arv]
        ruteoList.append(rgninfo + rgninfo2)

RuteoCols=['Competidor', 'MatriculaAvion', 'WeekDep', 'Ruteo', 'FechaDep', 'FechaArr',]
RuteoDF=pd.DataFrame(ruteoList, columns=RuteoCols).sort_values(['Competidor', 'MatriculaAvion', 'FechaDep',])
RuteoDF.to_csv("RuteoCompMIA.csv", index=False)
