import base64
import json
import time
import uuid
import yaml
from datetime import datetime, timedelta
import os
import csv
from dateutil import parser
from paho.mqtt.client import Client
from pb import SensorPayload_pb2


class Coursework:

    def __init__(self):
        with open(os.path.join('coursework', r'CW_Mqtt_Secrets.yaml')) as configuration:
            config = yaml.load(configuration, Loader=yaml.FullLoader)
            print(config)

        self.mqtt_clients = []

        self.dashboard_broker = Client(client_id=config['DASHBOARD_MQTT_CLIENT_ID'], clean_session=True)
        self.dashboard_broker.username_pw_set(username=config['DASHBOARD_MQTT_USER'],
                                              password=config['DASHBOARD_MQTT_PASS'])
        self.dashboard_broker.connect(host=config['DASHBOARD_MQTT_BROKER'], port=config['DASHBOARD_MQTT_PORT'])
        self.dashboard_broker.on_publish = self.on_publish

        self.ttn_broker = Client(client_id=str(uuid.getnode()), clean_session=False)
        self.ttn_broker.username_pw_set(username=config['TTN_MQTT_USER'],
                                        password=config['TTN_MQTT_PASS'])
        self.ttn_broker.connect(host=config['TTN_MQTT_BROKER'])
        self.ttn_broker.subscribe(topic="+/devices/+/up")
        self.ttn_broker.on_subscribe = self.on_subscribe
        self.ttn_broker.on_connect = self.on_connect
        self.ttn_broker.on_message = self.on_message

        # Start new threads for each broker
        self.ttn_broker.loop_start()
        self.dashboard_broker.loop_start()

        # Block main thread indefinitely
        while True:
            time.sleep(1)

    def on_subscribe(self, mosq, obj, mid, granted_qos):
        print('----------------')
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_connect(self, client, userdata, flags, rc):
        print('connected:', client._client_id)
        client.subscribe(topic='[topic]', qos=2)

    def on_publish(self, client, userdata, mid):
        print("Published")

    def on_message(self, client, userdata, message):
        print('----------------')
        received_time = datetime.now().astimezone()
        print('Message received at:', received_time)
        print('topic:', message.topic)
        print('message:', message.payload)
        payload = KitchenSensorPayload(message, received_time)

        print('Metadata:')
        print('     Payload sent:', payload.time)
        print('     RSSI:', payload.rssi)
        print('     SNR:', payload.snr)
        print('     Data rate:', payload.data_rate)
        print('Sensor data:')
        print('     Temperature:', payload.temperature)
        print('     Humidity:', payload.humidity)
        print('     LDR:', payload.ldr)
        print('     PIR last triggered:', payload.PIR_triggered_time)
        print('     Fridge last opened:', payload.fridge_opened_time)

        dashboard_json = [{
            "id": "KitchenLoRaIoT",
            "datetime": time.strftime("%Y-%m-%d %H:%M:%S"),
            "sensors": [
                {
                    "id": "Temperature",
                    "unit": 2,
                    "prefix": 0,
                    "type": 4,
                    "data": payload.temperature
                },
                {
                    "id": "LDR",
                    "unit": 24,
                    "prefix": 0,
                    "type": 11,
                    "data": payload.ldr
                },
                {
                    "id": "Humidity",
                    "unit": 20,
                    "prefix": 0,
                    "type": 5,
                    "data": payload.humidity
                },
                {
                    "id": "SignalStrength",
                    "unit": 22,
                    "prefix": 0,
                    "type": 17,
                    "data": {
                        "rssi": payload.rssi,
                        "snr": payload.snr
                    }
                },
                {
                    "id": "DataRate",
                    "data": payload.data_rate
                }
            ]

        }]

        pir_json = [{
            "id": "KitchenLoRaIoT",
            "datetime": payload.PIR_triggered_time.strftime("%Y-%m-%d %H:%M:%S"),
            "sensors": [
                {
                    "id": "PIRTriggered",
                    "data": 100
                }
            ]
        }]

        fridge_json = [{
            "id": "KitchenLoRaIoT",
            "datetime": payload.fridge_opened_time.strftime("%Y-%m-%d %H:%M:%S"),
            "sensors": [
                {
                    "id": "FridgeTriggered",
                    "data": 100
                }
            ]
        }]

        print("Publishing payload to dashboard:", json.dumps(dashboard_json))
        self.dashboard_broker.publish(topic='KitchenIoT', payload=json.dumps(dashboard_json))
        print("Publishing pir trigger to dashboard:", json.dumps(pir_json))
        self.dashboard_broker.publish(topic='KitchenIoT', payload=json.dumps(pir_json))
        print("Publishing fridge trigger to dashboard:", json.dumps(fridge_json))
        self.dashboard_broker.publish(topic='KitchenIoT', payload=json.dumps(fridge_json))
        print("Logging to CSV")
        log_to_csv(payload)


class KitchenSensorPayload:

    def __init__(self, message, received_time):
        payload_dict = json.loads(message.payload)
        self.received_time = received_time
        # Get metadata
        self.time = parser.parse(payload_dict['metadata']['time'])
        self.rssi = payload_dict['metadata']['gateways'][0]['rssi']
        self.snr = payload_dict['metadata']['gateways'][0]['snr']
        self.data_rate_raw = payload_dict['metadata']['data_rate']
        self.data_rate = ''.join(filter(str.isdigit, self.data_rate_raw))

        if payload_dict['port'] == 3:
            # Decode protocol buffer payload
            payload_hex = base64.b64decode(payload_dict['payload_raw'])
            sensor_payload = SensorPayload_pb2.SensorPayload()
            sensor_payload.ParseFromString(payload_hex)

            # Prepare sensor data
            self.temperature = sensor_payload.temperature / 100
            self.humidity = sensor_payload.humidity
            self.ldr = sensor_payload.ldr

            payload_fields = set([field.name for field in sensor_payload._fields])
            if 'sec_since_pir' in payload_fields:
                print(str(sensor_payload.sec_since_pir) + " seconds since last PIR activity")
                self.sec_since_pir = sensor_payload.sec_since_pir
                self.PIR_triggered_time = self.time - timedelta(seconds=sensor_payload.sec_since_pir)
                print("So, the PIR was triggered at:", self.PIR_triggered_time)
            else:
                self.PIR_triggered_time = None

            if 'sec_since_fridge' in payload_fields:
                print(str(sensor_payload.sec_since_fridge) + " seconds since the fridge was opened")
                self.sec_since_fridge = sensor_payload.sec_since_fridge
                self.fridge_opened_time = self.time - timedelta(seconds=sensor_payload.sec_since_fridge)
                print("So, the fridge was opened at:", self.fridge_opened_time)
            else:
                self.fridge_opened_time = None
        else:
            raise ValueError("KitchenSensorPayload initialised with incorrect payload type (expected port: 3)")


output_file = os.path.join(os.getcwd(), 'sensor_data.csv')


def log_to_csv(payload: KitchenSensorPayload):
    if os.path.isfile(output_file):
        # CSV exists, append to end of file
        with open(output_file, 'a', encoding="utf-8", newline='') as sensor_file:
            writer = csv.writer(sensor_file)
            writer.writerow([payload.time,
                             payload.received_time,
                             payload.rssi,
                             payload.snr,
                             payload.data_rate_raw,
                             payload.temperature,
                             payload.humidity,
                             payload.ldr,
                             payload.sec_since_pir,
                             payload.PIR_triggered_time,
                             payload.sec_since_fridge,
                             payload.fridge_opened_time])

    else:
        # CSV does not exist. Write the headings
        with open(output_file, 'w', encoding="utf-8", newline='') as sensor_file:
            writer = csv.writer(sensor_file)
            writer.writerow(['sent_time', 'received_time', 'rssi', 'snr', 'data_rate', 'temperature', 'humidity', 'ldr',
                             'sec_since_pir', 'PIR_triggered_time', 'sec_since_fridge', 'fridge_opened_time'])
            writer.writerow([payload.time,
                             payload.received_time,
                             payload.rssi,
                             payload.snr,
                             payload.data_rate_raw,
                             payload.temperature,
                             payload.humidity,
                             payload.ldr,
                             payload.sec_since_pir,
                             payload.PIR_triggered_time,
                             payload.sec_since_fridge,
                             payload.fridge_opened_time])


def replay_csv_to_mqtt():
    # todo
    pass
