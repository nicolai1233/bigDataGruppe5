import pandas as pd
import pydoop.hdfs as hdfs
import phonenumbers
import json
import pytz
from phonenumbers.phonenumberutil import (
    region_code_for_country_code,
    region_code_for_number,
)

class CountryConverter:

    def countryCodeToAcronym(self,data,EuCovidData):
        print("---countryCodeToAcronym---")     

        value = {'country_acronym':[],'count':[]}
        for line in range(len(data['country_acronym'])):
            #remove unwanted values
            try:
                if(data['country_acronym'][line]== None or int(data['country_acronym'][line].isdigit() != True)):
                    continue
            except ValueError:
                continue

            #code to acronym convertion
            d = region_code_for_country_code(int(data['country_acronym'][line]))

            # remove unwanted values
            if(d == 'ZZ' or d == '001'):
                continue
            
            #save
            value['country_acronym'].append(d) 
            value['count'].append(data['count'][line])

        #save wrong inputs
        #inputs where a country acronym was inserted into a contry codes location
        for line in range(len(data['country_acronym'])):
            for i in range(len(value['country_acronym'])):
                #skips
                if(data['country_acronym'][line] is float):
                    continue

                if(data['country_acronym'][line]==value['country_acronym'][i]):
                    value['count'][i] = int(float(value['count'][i]))+int(float(data['count'][line]))
           
        
        #convert contry acronym to contry name
        for i in range(len(value['country_acronym'])):
            #vals
            country_acronym = value['country_acronym'][i]
            #user specified
            if(country_acronym=='AC'):
                value['country_acronym'][i] = 'Saint Helena'
                continue
            if(country_acronym=='XK'):
                value['country_acronym'][i] = 'Kosovo'
                continue
            value['country_acronym'][i] = pytz.country_names[country_acronym]

        print("---save data local---")
        #export
        with open("covidCasesTest.json", "w") as outfile:
            l = []
            for index in range(len(value['country_acronym'])):
                for i in range(len(EuCovidData['country'])):
                    if(value['country_acronym'][index] == EuCovidData['country'][i]):
                        l.append({"country":value['country_acronym'][index],"tweetCount":value['count'][index],"death":int(float(EuCovidData['death'][i])),"OfficialCovidCases":int(float(EuCovidData['cases'][i]))})
                    if(EuCovidData['country'][i] == "United_States_of_America" and value['country_acronym'][index] == "United States"):
                        l.append({"country":value['country_acronym'][index],"tweetCount":value['count'][index],"death":int(float(EuCovidData['death'][i])),"OfficialCovidCases":int(float(EuCovidData['cases'][i]))})
                        
            json.dump(l, outfile)

           
           
        print("--- add to hdfs ---")
        df = pd.read_json ('covidCasesTest.json')
        export_csv = df.to_csv ('finalResultCombine.csv', index = None, header=True)
        hdfs.rm('covidCases/finalResultCombine.csv')
        hdfs.put('finalResultCombine.csv','covidCases')
   