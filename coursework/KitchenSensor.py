import base64
import json
from dataclasses import dataclass
from dateutil import parser
from datetime import datetime, timedelta
from pb import SensorPayload_pb2


@dataclass
class KitchenData:
    time: datetime
    received_time: datetime
    rssi: int
    snr: float
    data_rate_raw: str
    data_rate: int
    temperature: float
    humidity: int
    ldr: int
    sec_since_pir: int
    PIR_triggered_time: datetime
    sec_since_fridge: int
    fridge_opened_time: datetime


class KitchenSensorParser:

    @staticmethod
    def parse_csv_row(row):
        received_time = parser.parse(row[1])
        time = parser.parse(row[0])
        rssi = int(row[2])
        snr = float(row[3])
        data_rate_raw = row[4]
        data_rate = int(''.join(filter(str.isdigit, data_rate_raw)))
        temperature = float(row[5])
        humidity = int(row[6])
        ldr = int(row[7])
        sec_since_pir = int(row[8])
        PIR_triggered_time = parser.parse(row[9])
        sec_since_fridge = int(row[10])
        fridge_opened_time = parser.parse(row[11])
        return KitchenData(time, received_time, rssi, snr, data_rate_raw, data_rate, temperature, humidity, ldr, sec_since_pir, PIR_triggered_time, sec_since_fridge, fridge_opened_time)

    @staticmethod
    def parse_message(message, received_time):
        payload_dict = json.loads(message.payload)
        received_time = received_time
        # Get metadata
        time = parser.parse(payload_dict['metadata']['time'])
        rssi = payload_dict['metadata']['gateways'][0]['rssi']
        snr = payload_dict['metadata']['gateways'][0]['snr']
        data_rate_raw = payload_dict['metadata']['data_rate']
        data_rate = int(''.join(filter(str.isdigit, data_rate_raw)))

        if payload_dict['port'] == 3:
            # Decode protocol buffer payload
            payload_hex = base64.b64decode(payload_dict['payload_raw'])
            sensor_payload = SensorPayload_pb2.SensorPayload()
            sensor_payload.ParseFromString(payload_hex)

            # Prepare sensor data
            temperature = sensor_payload.temperature / 100
            humidity = sensor_payload.humidity
            ldr = sensor_payload.ldr


            payload_fields = set([field.name for field in sensor_payload._fields])
            if 'sec_since_pir' in payload_fields:
                print(str(sensor_payload.sec_since_pir) + " seconds since last PIR activity")
                sec_since_pir = sensor_payload.sec_since_pir
                PIR_triggered_time = time - timedelta(seconds=sensor_payload.sec_since_pir)
                print("So, the PIR was triggered at:", PIR_triggered_time)
            else:
                sec_since_pir = None
                PIR_triggered_time = None

            if 'sec_since_fridge' in payload_fields:
                print(str(sensor_payload.sec_since_fridge) + " seconds since the fridge was opened")
                sec_since_fridge = sensor_payload.sec_since_fridge
                fridge_opened_time = time - timedelta(seconds=sensor_payload.sec_since_fridge)
                print("So, the fridge was opened at:", fridge_opened_time)
            else:
                sec_since_fridge = None
                fridge_opened_time = None
        else:
            raise ValueError("KitchenSensorParser initialised with incorrect payload type (expected port: 3)")

        return KitchenData(time, received_time, rssi, snr, data_rate_raw, data_rate, temperature, humidity, ldr, sec_since_pir, PIR_triggered_time, sec_since_fridge, fridge_opened_time)
