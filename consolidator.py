import concurrent.futures
import os
import re
import pandas as pd
import time


def checkIfTexas_Station(filename, capturePattern, stationDF):
	usaf, wban, year = getFileNameInformation(filename, capturePattern)

	if(stationDF['USAF'].isin([usaf]).any() and stationDF['WBAN'].isin([wban]).any()):
		return True
	else:
		return False

def getFileNameInformation(filename, capturePattern):
	#capture usaf and wban from the filename
	captureMatch = capturePattern.match(filename)
	usaf = captureMatch.group(1)
	wban = captureMatch.group(2)
	year = captureMatch.group(3)
	return (usaf, wban, year)

def getData(fileTuple):
	filename = fileTuple[0]
	parentDirectory = fileTuple[1]
	capturePattern = fileTuple[2]
	whitespacePattern = fileTuple[3]
	csvString = ""

	usaf, wban, year = getFileNameInformation(filename, capturePattern)
	filepath = parentDirectory + "\\" + filename
	data_file = open(filepath, 'r')
	for line in data_file:
		#replace all whitespace characters in the line with commas
		line = whitespacePattern.sub(",", line)

		#remove comma at end of string that replaced the newline
		line = line[:-1]
		line = line + "\n"

		csvString += usaf + "," + wban + "," + line
	data_file.close()

	return (csvString, year)

def writeToCSV(csvList, key, headers):
	csvFileName = key + "_data.csv"

	csv_file = open(csvFileName, 'w')
	csv_file.write(headers)
	for string in csvList:
		csv_file.write(string)
	csv_file.close()


def main():
	start = time.time()
	with concurrent.futures.ProcessPoolExecutor() as executor:
		#set regex pattern
		whitespace_pattern = re.compile('\s+')
		capt_group_pattern = re.compile('(\w*)\-(\w*)\-(\w*)')

		#set parent directory
		parent_directory = "D:\\isd-lite\\years_unzipped"

		#headers for csv files
		headers = "USAF,WBAN,YEAR,MONTH,DAY,HOUR,AIR_TEMP,DEW_POINT_TEMP,SEA_LVL_PRESSURE,WIND_DIR,WIND_SPEED_RATE,SKY_CVRG_CODE,LIQ_DEPTH_ONE_HOUR,LIQ_DEPTH_SIX_HOUR\n"

		#variable to hold list of all files
		allFiles = []

		#dictionary to hold file information for each year
		year_dict = {
			'1940': [],
			'1950': [],
			'1951': [],
			'1960': [],
			'1970': []
		}

		#set dataframe to hold station information for all countries
		stationDF = pd.read_csv("isd-history.csv")
		#filter dataframe to only hold station information for Texas
		stationDF = stationDF.loc[(stationDF['CTRY'] == 'US')]
		stationDF = stationDF.loc[(stationDF['STATE'] == 'TX')]

		#get list of all year directories in the years_unzipped directory
		directories = os.listdir(parent_directory)
		for directory in directories:
			files = os.listdir(parent_directory + "\\" + directory)
			for file in files:
				allFiles.append((file, parent_directory + "\\" + directory, capt_group_pattern, whitespace_pattern))

		#filter files to only contain files for US stations
		allFiles = list(filter(lambda x: checkIfTexas_Station(x[0], capt_group_pattern, stationDF), allFiles))

		#divide processing of the files among all available cpu cores
		generator = executor.map(getData, allFiles)

		for csvString, year in generator:
			year_dict[year].append(csvString)

		for key in year_dict:
			writeToCSV(year_dict[key], key, headers)
		
		end = time.time()
		print("Duration in seconds: %d", (end % 60) - (start % 60))

if __name__ == '__main__':
	main()
			

