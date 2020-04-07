import os
import csv
import json
from DataDownloader import DataDownloader
from azure.storage.queue import QueueClient

class DataProcessor():

    ACTIVE_COL_NAME = 'FAC_ACTIVE_FLAG'
    IS_WATER_FAC_COL_NAME = 'SDWIS_FLAG'

    COLUMNS_OF_INTEREST = ('REGISTRY_ID', 'FAC_NAME', 'FAC_STREET', 'FAC_CITY', 'FAC_STATE', 'FAC_ZIP', 'FAC_COUNTY', 
        'FAC_FIPS_CODE', 'FAC_EPA_REGION', 'FAC_LAT', 'FAC_LONG', 'FAC_DERIVED_STCTY_FIPS', 'FAC_DERIVED_ZIP', 
        'FAC_PERCENT_MINORITY', 'FAC_POP_DEN', 'FAC_MAJOR_FLAG', 'FAC_COMPLIANCE_STATUS', 'FAC_INSPECTION_COUNT', 
        'FAC_DATE_LAST_INSPECTION', 'FAC_DAYS_LAST_INSPECTION', 'FAC_FORMAL_ACTION_COUNT', 'FAC_DATE_LAST_FORMAL_ACTION', 
        'FAC_TOTAL_PENALTIES', 'FAC_PENALTY_COUNT', 'FAC_DATE_LAST_PENALTY', 'SDWA_IDS', 'SDWA_SYSTEM_TYPES', 
        'SDWA_INFORMAL_COUNT', 'SDWA_FORMAL_ACTION_COUNT',  'SDWA_COMPLIANCE_STATUS', 'SDWA_SNC_FLAG',
        'CWA_PERMIT_TYPES', 'CWA_COMPLIANCE_TRACKING', 'CWA_NAICS', 'CWA_SICS', 'CWA_INSPECTION_COUNT', 
        'CWA_DAYS_LAST_INSPECTION', 'CWA_INFORMAL_COUNT', 'CWA_FORMAL_ACTION_COUNT', 'CWA_DATE_LAST_FORMAL_ACTION',
        'CWA_PENALTIES', 'CWA_LAST_PENALTY_DATE', 'CWA_LAST_PENALTY_AMT', 'CWA_QTRS_WITH_NC', 'CWA_COMPLIANCE_STATUS', 
        'CWA_SNC_FLAG', 'CWA_13QTRS_COMPL_HISTORY', 'CWA_13QTRS_EFFLNT_EXCEEDANCES', 'CWA_3_YR_QNCR_CODES')

    def __init__(self, csv_path, output_facility_csv):
        self._csv_file = csv_path
        self._csv_columns = []
        self._facility_output = facility_csv

        _queue = 'waterbeacon'
        _connection_string = '' #TODO load from config

        self._queue_client = QueueClient.from_connection_string(
            conn_str=_connection_string, queue_name=_queue)


    def _process_facility_csv(self):
        first_row = True
        with open(self._facility_output, 'w') as target:
            with open(self._csv_file, newline='\n') as f:
                facilityreader = csv.reader(f, delimiter=',', quotechar='"')
                for row in facilityreader:
                    if first_row:
                        self._csv_columns = row
                        first_row = False
                        target.write('%s\n' %','.join(row))
                        continue
                    self._process_facility(row)

    def _process_facility(self, facility_row):
        active = facility_row[self._csv_columns.index(self.ACTIVE_COL_NAME)] == 'Y'
        is_water_facility = facility_row[self._csv_columns.index(
            self.IS_WATER_FAC_COL_NAME)] == 'Y'
        if not (active and is_water_facility):
            return None
        # format the message and put it on the queue
        facility_json = json.dumps(facility_row) # TODO wtf
        # self._queue_client.send_message(facility_json)

        # return 1

    def process_data(self):
        self._process_facility_csv()


if __name__ == "__main__":
    target_dir = '/Users/david/Downloads/'
    target_file_name = 'echo.zip'
    # d = DataDownloader(target_dir, target_file_name)
    # csv = d.process_download()
    csv_file = '/Users/david/Downloads/ECHO_EXPORTER.csv'
    facility_csv = '/Users/david/Downloads/facilities.csv'
    p = DataProcessor(csv_file, facility_csv)
    p.process_data()
