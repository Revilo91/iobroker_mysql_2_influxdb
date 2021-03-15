import json
import os
import sys
from datetime import datetime
from typing import MutableMapping
from influxdb import InfluxDBClient


class Merge():
    def __init__(self):
        # Load DB Settings
        database_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "database.json")
        if not os.path.exists(database_file):
            print("Please rename database.json.example to database.json")
            sys.exit(1)

        with open(database_file, 'r') as f:
            filedb = f.read()

        try:
            self.db = json.loads(filedb)
        except json.decoder.JSONDecodeError as ex:
            print(database_file + "Json is not valid!")
            print(ex)
            sys.exit(1)
        except Exception as ex:
            print("Unhandeld Exception")
            print(ex)
            sys.exit(1)

        self.INFLUXDB_CONNECTION = InfluxDBClient(host=self.db['InfluxDB']['host'],
                                                  ssl=self.db['InfluxDB']['ssl'],
                                                  verify_ssl=True,
                                                  port=self.db['InfluxDB']['port'],
                                                  username=self.db['InfluxDB']['user'],
                                                  password=self.db['InfluxDB']['password'],
                                                  database=self.db['InfluxDB']['database'])

    def getData(self, measurements):
        rs = self.INFLUXDB_CONNECTION.query(f"SELECT * FROM \"{measurements[0]}\"")
        dataList = list(rs.get_points(measurement=measurements[0]))
        influx_points = []
        ignore = ["time"]
        for data in dataList:
            fields = {}
            for k, d in data.items():
                if k not in ignore:
                    fields[k] = data[k]

            influx_points.append({
                "measurement": measurements[1],
                # "tags": tags,
                "time": data["time"],
                "fields": fields
            })
        return influx_points

    def writeData(self, influx_points):
        print("START")
        try:
            self.INFLUXDB_CONNECTION.write_points(influx_points, retention_policy=self.db['InfluxDB']['retention_policy'], batch_size=100000)
        except Exception as ex:
            print("InfluxDB error")
            print(ex)
        print("FINISH")


if __name__ == "main":
    Measurements = ("hm-rpc.0.MEQ1599097.2.TEMPERATURE", "hm-rpc.0.REQ0936068.2.TEMPERATURE")

    merge = Merge()
    merge.getData(Measurements)

