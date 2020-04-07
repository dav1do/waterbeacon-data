import requests
import timeit
import zipfile
import os

class DataDownloader():

    def __init__(self, target_dir, download_zipfile_name):
        self._download_url = 'https://echo.epa.gov/files/echodownloads/echo_exporter.zip'
        self._target_dir = target_dir
        self._target_file = os.path.join(target_dir, download_zipfile_name)

    def _download_file(self):
        start_time = timeit.default_timer()
        with requests.get(self._download_url, stream=True) as r:
            r.raise_for_status()
            with open(self._target_file, 'wb') as f:
                # tested 65592 and it was worse 
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        elapsed = timeit.default_timer() - start_time
        print('took %s seconds to download the file' % elapsed)

    def _extract_file(self):
        EXPECTED_FILE = 'ECHO_EXPORTER.csv'
        # this is strange but there are recommendations against extracting all from internet sources.
        # while it should be safe, a file name change probably means there is more changing
        with zipfile.ZipFile(self._target_file, 'r') as zf:
            for info in zf.infolist():
                if info.filename == EXPECTED_FILE:
                    zf.extract(info, self._target_dir)

        return os.path.join(self._target_dir, EXPECTED_FILE)

    def process_download(self):
        self._download_file()
        return self._extract_file()
