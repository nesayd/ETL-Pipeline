# ETL-Pipeline

In this repo you can find an end-to-end automated data pipeline that loads XBRL (eXtensible Business Reporting Language) datasets from the from filings in a tabular format on the SEC EDGAR website onto Redivis platform (a data platform for academic research) by scraping the SEC.gov (U.S. SECURITIES AND EXCHANGE COMMISSION) for the XBRL files in zip format, extracting them into the Yen (Stanford Remote Computers) directory, unzipping those files in order by month/quarter and year into a new path, handling the format checks and data type checks, and finally uploading those files onto the Redivis by creating a new table for each of the eight different datasets and appending each month's or quarter's data in order.     

These XBRL files include the text and detailed numeric information from all financial statements and their notes submitted by filers to the Commission from 2009 on. The zip files are loaded on website quarterly between 2009 and 2020 and monthly after 10/2020. 

The data extracted from the XBRL submissions is organized into eight data sets in .tsv format containing information about submissions, numbers, taxonomy tags, presentation, and more.  Each data set consists of rows and fields, and is provided as a tab-delimited TXT format file.  The data sets are as follows:

* SUB – Submission data set; this includes one record for each XBRL submission. The set includes fields of information pertinent to the submission and the filing entity. Information is extracted from the Commission's EDGAR system and the filings submitted to the Commission by registrants.
* TAG – Tag data set; includes defining information about each tag.  Information includes tag descriptions (documentation labels), taxonomy version information and other tag attributes.
* DIM – Dimension data set; used to provide more detail in numeric and non-numeric facts.
* NUM – Number data set; this includes one row for each distinct amount from each submission included in the SUB data set. The Number data set includes, for every submission, all line item values as rendered by the Commission Viewer/Previewer.
* TXT – Text data set; this is the plain text of all the non-numeric tagged items in the submission.
* REN – Rendering data set; this contains data from the rendering of the filing on the Commission website.
* PRE – Presentation data set; this provides information about how the tags and numbers were presented in the primary financial statements.
* CAL – Calculation data set; provides information to arithmetically relate tags in a filing.







