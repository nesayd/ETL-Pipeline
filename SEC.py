#!/usr/bin/env python3
'''
This program creates a data pipeline to load XBRL datasets from the SEC website onto Redivis platform. This dataset provides a lot of good information from SEC filings in a tabular format. During this ETL process, I build an automated pipeline with format checks and data type checks by scraping the SEC website, downloading monthly or quarterly zipfiles into the Yens directory, unzipping them in a new folder, looking up for only .tsv files in the folder, converting the .tsv files into .csv files, and finally uploading them to Redivis platform.

Author: Neset Aydin
Date: 5/9/2022
'''

import time
import redivis
import sys 
from bs4 import BeautifulSoup
import requests 
import json
import wget 
import zipfile
from os import walk
import os
import pandas as pd
from collections import defaultdict
import logging
import warnings
warnings.filterwarnings("ignore")

def get_links(url:str) -> list:
    '''
    Extract the links of zipfiles on sec.gov
    Parameters: 
        url (string): web address
    Returns: 
        links (list): a list of zipfile links on the website
    Ex: 
        >>> url = "https://www.sec.gov/dera/data/financial-statement-and-notes-data-set.html"
        >>> get_links(url)[:2]
        ['https://www.sec.gov/files/dera/data/financial-statement-and-notes-data-sets/2022_04_notes.zip',
        'https://www.sec.gov/files/dera/data/financial-statement-and-notes-data-sets/2022_03_notes.zip']
    '''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.find('title')
    links = []
    for table in soup.findAll('td', attrs = {'class':'views-field views-field-field-display-title'}):
        links.append("https://www.sec.gov" + table.find('a')['href'])
    return links

def download_zipfiles(download_path:str, links:str) -> None: 
    '''
    Download the zipfiles extracted from the SEC website into the Yens directory.
    Parameters: 
        download_path (string): path for the zipped files
        links (list): list of urls of the zip files
    Returns: 
        Does not return anything.
    '''
    for link in links:
        file_name = link.split('/')[-1]
        count = 0
        if not os.path.exists(os.path.join(download_path, file_name)):
            logging.info(f'Link {file_name} is ready to download!' )
            wget.download(link, out = download_path)
            logging.info(f'Link {file_name} successfully downloaded to {download_path}!')
            count += 1
    if not count == 0:
        logging.info(f'Successfully downloaded {count} zip files.')

def unzip(unzip_path:str, zip_files:str) -> None:
    '''
    Unzips the downloaded files in the directory into a new folder.
    Parameters: 
        unzip_path (string): path for the unzipped files
        zip_files (string) : downloaded zip files in the directory
    Returns: 
        Does not return anything.
    '''

    for (dirpath, dirnames, filenames) in walk(zip_files):
        for file in filenames: 
            if file.endswith(".zip"):
                fullpath = os.path.join(dirpath, file)
                foldername = file.split('.')[0]
                unzip_folder = unzip_path+'/'+foldername
                if not os.path.exists(unzip_folder):
                    os.makedirs(unzip_folder)
                    with zipfile.ZipFile(fullpath, 'r') as zip_ref:
                        zip_ref.extractall(unzip_folder) 
                    logging.info(f"Successfully unzipped {foldername}!!")


def get_uploaded_tables(text_file_path:str) -> list:
    '''
    Creates a .txt file to keep the records of uploaded paths onto Redivis.
    Passed in the get_path_dicts function.
    Parameters: 
        text_file_path (string): a path for a .txt file
    Returns: 
        list : a list of uploaded paths 
    '''
    if not os.path.exists(text_file_path):
        open(text_file_path, 'w').close()

    with open(text_file_path, "rb") as file:
        content = file.readlines()
    
    return [line.strip().decode('ascii') for line in content]

def write_uploaded_tables(text_file_path:str, table_name_full_path:str) -> None:
    '''
    Writes the uploaded paths to the created .txt file.
    Parameters: 
        text_file_path (string): path for the .txt file
        table_name_full_path (string): paths for the uploaded files
    Returns: 
        Does not return anything.    
    '''
    with open(text_file_path, "a") as f:
        logging.info("Started to write to the .txt file with file path: " + str(table_name_full_path))
        f.write(table_name_full_path)
        f.write('\n')


        

def upload_to_redivis(table_name:str,dataset_path:str,dataset)->None:
    '''
    Creates a new table on Redivis that will append each time the file with the same name gets uploaded.
    Uploads (by appending) the file onto Redivis.
    Parameters: 
        table_name (string): the name of the table created for a specific file to be uploaded on Redivis
        dataset_path (string): the path of the file to be uploaded
        dataset (string): the name of the file to be appended
    Returns: 
        Does not return anything.
    '''
    table = (dataset.table(table_name))

    if not table.exists():
        table.create(description=None, upload_merge_strategy="append")

    upload = table.upload(table_name) 
    
    full_path = dataset_path+'/'+table_name+'.csv'
    
    #Quality check control here 
    #DF before uploading
    df_before = pd.read_csv(full_path)
    logging.info(f"Before uploading the CSV file: Rows: {df_before.shape[0]}, Columns: {df_before.shape[1]}")
    
    #Get shape from Redivis before uploading 
    table.get()
    if 'numRows' in table.properties: 
     
        prev_row_count = table.properties['numRows']
        prev_col_count = table.properties['variableCount']
    else: 
        prev_row_count = 0
        prev_col_count = 0
    
    #Uploading
    # try: 
    with open(full_path, "rb") as file:
        upload.create(
            file, 
            type="delimited",
            delimiter = ",",
            skip_bad_records=True,
            allow_quoted_newlines=True, 
            quote_character='"', 
            remove_on_fail=False, 
            wait_for_finish=True,
            raise_on_fail=True,
        )
#     except Exception as e: 
#         if hasattr(e, 'message'): 
#             if 'An error occured while transferring' in e.message: 
#                 logging.error("Network Connection Error occured. Check Redivis Platform. Manage imports. Delete the errorred upload(s). Rerun the script!")
#                 raise Exception
                
                
    
        
    #Get shape from Redivis after uploading
    table.get()
    after_row_count = table.properties['numRows']
    after_col_count = table.properties['variableCount']
    logging.info(f"After uploading the CSV file: Rows: {after_row_count - prev_row_count} , Columns: {after_col_count} ")


def get_paths_dict(text_file_path:str, unzip_path:str, dataset_name:str)->dict:
    '''
    Creates a dataset on Redivis using the organization name (Stanford Library) that will be composed of multiple tables for different file names.
    Creates a dictionary of the filenames as keys and full paths of those files as values.
    Walks the unzipped files and finds the files ending with .tsv.
    Parameters:
        text_file_path (string): files uploaded are collected in a .txt file in this path
        unzip_path (string): path for the unzipped files
        dataset_name (sting): the name of the dataset passed to redivis api function 
    Returns:
        d (dictionary): a dictionary of filenames as keys and a list of full paths of those files as values
        dataset (string): the name of the dataset created on Redivis  
    '''
    
    dataset = redivis.organization("StanfordGSBLibrary").dataset(dataset_name)
    
    #if you are creating the dataset yourself on Redivis platform manually, just use your username. 
    #(don't forget pass an argument to the function for username). below line is the script:
    # dataset = redivis.user(username).dataset(dataset_name)
    
    #if dataset exists, donot create the same one. 
   

    if not dataset.exists():
        dataset.create(public_access_level="data")
 
    
    d = defaultdict(list) 
    
    uploaded_files = get_uploaded_tables(text_file_path)

    for (dirpath, dirnames, filenames) in walk(unzip_path):
        for file in filenames:
            if file.endswith('.tsv') and not 'checkpoint' in file: 
                file_path = dirpath+'/'+file #the full path of a file that belongs to a specific month's folder
                if not file_path in uploaded_files:
                    d[file].append(file_path) #file name as our key e.g. tag.tsv

    d = {k: sorted(v, reverse=False) for k,v in d.items()}
    
    return d, dataset

def upload_append_csv_files(text_file_path: str, unzip_path:str , dataset_path:str, 
                            dataset_name:str, username:str) -> None:
    """
    Gets the dictionary and the created dataset on Redivis.
    Reads the .tsv files using the values of the dictionary.
    Converts the dataframe of each .tsv file to a .csv file and upload it to Redivis platform.
    Slack-alerts the slack channel for the removed rows.
    Parameters: 
        text_file_path (string): files uploaded are collected in a .txt file in this path
        unzip_path (string): path for the unzipped files
        dataset_path (string): paths of the .csv files to be uploaded to Redivis
        dataset_name (sting): the name of the dataset passed to redivis api function
        username (string) : username for Redivis
    Returns:
        Does not return anything. 
    """
    
    d, dataset = get_paths_dict(text_file_path,unzip_path,dataset_name)
    if not d: 
        logging.info("Redivis database is up to date. Nothing to upload at the moment! Wait until next month.")

    for key, value in d.items(): 
        for path in value: 
            table_name = key.split('.')[0]     
            logging.info("TABLE NAME: " + str(table_name))
            
            with open(path, encoding = "UTF-8") as f: 
                file = f.readlines()
            
            if len(file) < 2: 
                logging.info("Empty file. No rows to be appended!")
                write_uploaded_tables(text_file_path, path)
                logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
                continue
                

            
            logging.info("Number of rows: " + str(len(file)-1))
            line_list = []
            invalid_rows_counter = 0
            idx_list = []
            for i, line in enumerate(file):
                splitted = line.split('\t')
                cleaned = [line.replace("\\"," ").replace("\"", '"').replace("\'", "'").replace("\n","") for line in splitted]
                line_list.append(cleaned)
                
        
             
                if len(cleaned) != len(line_list[0]):
                    line_list.remove(cleaned)
                    invalid_rows_counter += 1
                    row_index = f'{i}' + "."
                    idx_list.append(row_index)
                    logging.info(f"File Path: {path}")
                    logging.info(f"Row Index: {i}")
                    
                    logging.info(f"Removed Row: {cleaned}")
            logging.info(f"Total # of rows skipped: {invalid_rows_counter}")
            logging.info(f"% of rows skipped: {round((invalid_rows_counter/(len(file)-1))*100,3): .3f}")

            df = pd.DataFrame(line_list[1:], columns=line_list[0])
            
            
            csv_path = dataset_path+'/'+table_name+'.csv'
            df.to_csv(csv_path, index=False)
            upload_to_redivis(table_name,dataset_path,dataset)
            
            logging.info(f"Successfully uploaded {csv_path.split('/')[-1]} into {dataset_name} dataset on Redivis!")
            write_uploaded_tables(text_file_path, path)
            logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
            
            if len(idx_list) > 0:
                s = f"Uploading for {path} is completed\nBelow you can find the path of the file with skipped rows!\nCheck log file to see the content of each skipped row!\n" + f"PATH: {path}" +"\nRemoved Row Indices: "+str(idx_list)           
                result = send_slack_message(s)
                if not result: 
                    logging.error("Failed to alert in Slack Channel. Please check slack URL!") 

def send_slack_message(message): 
    '''
    Writes a slack message in text format on the Slack channel of the defined url.
    Parameters: 
        message (string): text message that points out the information of the errored row.
    Returns:
        A message if there is any.
    Ex:
        "Uploading for ./unzip_files/2011q3_notes/txt.tsv is completed
        Below you can find the path of the file with skipped rows!
        Check log file to see the content of each skipped row!
        PATH: ./unzip_files/2011q3_notes/txt.tsv
        Removed Row Indices: ['188992.', '217128.', '217709.']"
    '''
    url = "https://hooks.slack.com/workflows/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"
    payload = {"text":message}
    
    try:
        r = requests.post(url, data=json.dumps(payload))
        return "0" #positive
    except: 
        return "1" #negative
    
def set_logger(path, logging_level):
    '''
    Creates a "Logs" folder if there isn't any. 
    Logs the summary in the hardcoded "ETL.log" file. 
    Parameters: 
        path (str): The path of the Logs
        logging_level (str): The level of the logging type (e.g:logging.debug, logging.info, logging.critical etc.)
    Returns: 
        Does not return anything. 
    '''
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename=f'{path}/ETL.log', filemode='a')    
    

    
url = "https://www.sec.gov/dera/data/financial-statement-and-notes-data-set.html"
download_path = './zip_files'
text_file_path = './uploaded_file_list.txt'
unzip_path = './unzip_files'
dataset_path = './datasets'
log_path = './Logs'

print(f"Script started. Check log file details: {log_path}")

start = time.time()
set_logger(log_path, logging.INFO)
links = get_links(url)
download_zipfiles(download_path, links)
unzip(unzip_path, download_path)
upload_append_csv_files(text_file_path=text_file_path, unzip_path=unzip_path,
                        dataset_path=dataset_path, dataset_name='SEC_XBRL', username='neset06')

print(f"The whole script runs in: {time.time() - start} seconds")