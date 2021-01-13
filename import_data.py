from datetime import datetime, timedelta
import requests
import json
from time import sleep

API_URL = "https://www.meteo-trebeurden.fr/Gauges/data/realtimegaugesWC.txt"


class ApiFetcher():

    def __init__(self):

        self.metrics = {}
        self.last_fetch = None

    def _fetch(self):

        ## Check if last fetch is old
        fetch = False
        if self.last_fetch is None:
            fetch = True
        elif datetime.now() > (self.last_fetch + timedelta(seconds=60)):
            fetch = True

        if fetch:
            print("Fetching from API")
            response = requests.get(API_URL)
            if response.status_code == 200:
                try:
                    self.metrics = json.loads(response.content)
                    self.last_fetch = datetime.now()
                except Exception as e:
                    print(repr(e))

    def get(self, key):
        self._fetch()
        return self.metrics[key]


if __name__ == '__main__':
    a = ApiFetcher()
    while True:
        print("Time: {} SolarRad: {} Temperature: {} ".format(a.get('timeUTC'), a.get('SolarRad'), a.get('temp')))
        sleep(10)

