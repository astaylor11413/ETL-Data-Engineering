import glob 
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file='log_executions.txt'
target_file='data_transformed.csv'

#extract data from csv file
def extract_from_csv(file):
    df = pd.read_csv(file)
    return df

#extract data from json file
def extract_from_json(file):
    df = pd.read_json(file,lines=True)
    return df

#extract data from xml file
def extract_from_xml(file):
    df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    root = ET.parse(file).getroot()
    for car in root:
        car_model = car.find('car_model').text
        year_of_manufacture = car.find('year_of_manufacture').text
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        df = pd.concat([df,pd.DataFrame({'car_model':[car_model],'year_of_manufacture':[year_of_manufacture],'price':[price],'fuel':[fuel]})],ignore_index=True)
    return df

#extract from csv, json, or xml files depending on what is found in directory
def extract():
    df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    #extract csv data
    for file in glob.glob('*.csv'):
        if(file!=target_file):
            df = pd.concat([df,pd.DataFrame(extract_from_csv(file))],ignore_index=True)

    #extract json data
    for file in glob.glob('*.json'):
        df = pd.concat([df,pd.DataFrame(extract_from_json(file))],ignore_index=True)

    #extract xml data
    for file in glob.glob('*.xml'):
        df = pd.concat([df,pd.DataFrame(extract_from_xml(file))],ignore_index=True)

    return df

#transform price data-rounding to 2 decimal places for each
def transform(df):
    df['price']= round(df.price,2)
    return df

#load transformed data to target file
def load(transformed_df, target_file):
    transformed_df.to_csv(target_file)

#log process as it is executed
def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file,"a") as f:
        f.write(timestamp+"-"+message+'\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
  
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
  
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
  
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
  
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
  
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load(transformed_data,target_file) 
  
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
  
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 