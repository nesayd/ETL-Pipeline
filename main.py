from SEC.py import *
import time 
import warnings
warnings.filterwarnings("ignore")

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
                        dataset_path=dataset_path, dataset_name='SEC_XBRL', username='XXX')

print(f"The whole script runs in: {time.time() - start} seconds")
