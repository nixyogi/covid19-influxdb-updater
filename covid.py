import csv
import time
import datetime
import os

from influxdb import InfluxDBClient

import wget 

"""
Download the CSV files
"""
url_confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"

url_recovered = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"

url_deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"

print("Downloading Data from Github...")
wget.download(url_confirmed)
wget.download(url_recovered)
wget.download(url_deaths)

print("Finished Downloading!")

"""
Begin Parsing CSV
"""

dataConfirmed = []
dataRecovered = []
dataDeaths = []

print("Reading CSV...")

with open('time_series_19-covid-Confirmed.csv', 'r') as infected,open('time_series_19-covid-Recovered.csv', 'r') as recov, open('time_series_19-covid-Deaths.csv', 'r') as dead:
        """
        Parse CSV into a dict
        """
        dictConfirmed = csv.DictReader(infected)
        dictRecovered = csv.DictReader(recov)
        dictDeaths = csv.DictReader(dead)

        print("Reading CSV Done!")
        
        """
        Loop and append to data
        """
        print("Cleaning confirmed Data...")
        for confirmed in dictConfirmed:
                for k, v in confirmed.items():
                        """
                        Keep only Date column
                        """
                        if "/20" not in k: 
                                continue
                        unix_time = time.mktime(datetime.datetime.strptime(k, "%m/%d/%y").timetuple())
                        unix_time = (unix_time*1000000000)
                        """
                        Empty State ? Replace with NA
                        """
                        if (not confirmed["Province/State"]):
                                confirmed["Province/State"] = 'NA'
                        
                        """
                        Append Data to an array
                        """

                        dataConfirmed.append("{measurement},state={state} latitude={lat},longitude={longi},confirmed={value} {timestamp}"
                                .format(measurement=confirmed["Country/Region"].replace(' ', '-').replace(',', ''),
                                        state=confirmed["Province/State"].replace(' ', '-').replace(',', ''),
                                        lat=confirmed["Lat"],
                                        longi=confirmed["Long"],
                                        value=v,
                                        timestamp=int(unix_time)))
        """
        Loop and append to data
        """
        print("Cleaning recovered Data...")
        for recovered in dictRecovered:
                for k, v in recovered.items():
                        """
                        Keep only Date column
                        """
                        if "/20" not in k: 
                                continue
                        unix_time = time.mktime(datetime.datetime.strptime(k, "%m/%d/%y").timetuple())
                        unix_time = (unix_time*1000000000)
                        """
                        Empty State ? Replace with NA
                        """
                        if (not recovered["Province/State"]):
                                recovered["Province/State"] = 'NA'
                        
                        """
                        Append Data to an array
                        """

                        dataRecovered.append("{measurement},state={state} latitude={lat},longitude={longi},recovered={value} {timestamp}"
                                .format(measurement=recovered["Country/Region"].replace(' ', '-').replace(',', ''),
                                        state=recovered["Province/State"].replace(' ', '-').replace(',', ''),
                                        lat=recovered["Lat"],
                                        longi=recovered["Long"],
                                        value=v,
                                        timestamp=int(unix_time)))
        """
        Loop and append to data
        """
        print("Cleaning deaths Data...")
        for deaths in dictDeaths:
                for k, v in deaths.items():
                        """
                        Keep only Date column
                        """
                        if "/20" not in k: 
                                continue
                        unix_time = time.mktime(datetime.datetime.strptime(k, "%m/%d/%y").timetuple())
                        unix_time = (unix_time*1000000000)
                        """
                        Empty State ? Replace with NA
                        """
                        if (not deaths["Province/State"]):
                                deaths["Province/State"] = 'NA'
                        
                        """
                        Append Data to an array
                        """

                        dataDeaths.append("{measurement},state={state} latitude={lat},longitude={longi},deaths={value} {timestamp}"
                                .format(measurement=deaths["Country/Region"].replace(' ', '-').replace(',', ''),
                                        state=deaths["Province/State"].replace(' ', '-').replace(',', ''),
                                        lat=deaths["Lat"],
                                        longi=deaths["Long"],
                                        value=v,
                                        timestamp=int(unix_time)))


print("Done Cleaning!")

"""
Print Payload for debugging

print(dataConfirmed)
print(dataRecovered)
print(dataDeaths)
"""
print("Sending to Influxdb....")

client = InfluxDBClient(host='148.251.91.243', port=8086)
client.write_points(dataConfirmed, database='covid19_test', batch_size=10000, protocol='line')
client.write_points(dataRecovered, database='covid19_test', batch_size=10000, protocol='line')
client.write_points(dataDeaths, database='covid19_test', batch_size=10000, protocol='line')

print("Sent Data!")
print("Clean Up CSV Files.")

if os.path.exists("time_series_19-covid-Confirmed.csv"):
	os.remove("time_series_19-covid-Confirmed.csv")
else:
	print("The file does not exist") 

if os.path.exists("time_series_19-covid-Deaths.csv"):
	os.remove("time_series_19-covid-Deaths.csv")
else:
	print("The file does not exist") 

if os.path.exists("time_series_19-covid-Recovered.csv"):
	os.remove("time_series_19-covid-Recovered.csv")
else:
	print("The file does not exist") 

print("Done Cleaning!") 
