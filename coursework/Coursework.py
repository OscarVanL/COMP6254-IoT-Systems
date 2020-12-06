import base64
import json
import time
import uuid
import yaml
import datetime
import os
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
        self.dashboard_broker.username_pw_set(username=config['DASHBOARD_MQTT_USER'], password=config['DASHBOARD_MQTT_PASS'])
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
        print('Message received at:', datetime.datetime.now())
        print('topic:', message.topic)
        print('message:', message.payload)
        payload_dict = json.loads(message.payload)
        rssi = payload_dict['metadata']['gateways'][0]['rssi']
        snr = payload_dict['metadata']['gateways'][0]['snr']
        data_rate = payload_dict['metadata']['data_rate']
        print('Signal RSSI:', rssi)
        print('Signal SNR:', snr)
        print(payload_dict)

        if payload_dict['port'] == 3:
            # Port 3 is for our sensor data payload
            print("Payload raw:", payload_dict['payload_raw'])
            payload_hex = base64.b64decode(payload_dict['payload_raw'])
            print("Payload hex:", base64.b64decode(payload_dict['payload_raw']))
            print("Decoded payload:")
            sensor_payload = SensorPayload_pb2.SensorPayload()
            sensor_payload.ParseFromString(payload_hex)

            temperature = sensor_payload.temperature / 100
            print("Temperature: " + str(temperature) + "C")
            print("LDR Value: " + str(sensor_payload.ldr))
            print("Humidity: " + str(sensor_payload.humidity) + "%")
            payload_fields = set([field.name for field in sensor_payload._fields])

            # Convert the ISO 8601 datetime from TTN into python datetime
            time = parser.parse(payload_dict['metadata']['time'])

            if 'sec_since_pir' in payload_fields:
                print(str(sensor_payload.sec_since_pir) + " seconds since last PIR activity")
                PIR_triggered_time = time - datetime.timedelta(seconds=sensor_payload.sec_since_pir)
                print("So, the PIR was triggered at:", PIR_triggered_time)
            else:
                PIR_triggered_time = None

            if 'sec_since_fridge' in payload_fields:
                print(str(sensor_payload.sec_since_fridge) + " seconds since the fridge was opened")
                fridge_opened_time = time - datetime.timedelta(seconds=sensor_payload.sec_since_fridge)
                print("So, the fridge was opened at:", fridge_opened_time)
            else:
                fridge_opened_time = None

            dashboard_json = {
                "name": "KitchenLoRaIoT",
                "datetime": time.strftime("%Y-%m-%d %H:%M:%S"),
                "sensors": [
                    {
                        "uid": "Temperature",
                        "unit": 2,
                        "prefix": 0,
                        "type": 4,
                        "data": temperature
                    },
                    {
                        "uid": "LDR",
                        "unit": 24,
                        "prefix": 0,
                        "type": 11,
                        "data": sensor_payload.ldr
                    },
                    {
                        "uid": "Humidity",
                        "unit": 20,
                        "prefix": 0,
                        "type": 5,
                        "data": sensor_payload.humidity
                    },
                    {
                        "uid": "PIRTriggered",
                        "unit": 0,
                        "prefix": 0,
                        "type": 1,
                        "data": PIR_triggered_time.strftime("%Y-%m-%d %H:%M:%S")
                    },
                    {
                        "uid": "FridgeTriggered",
                        "unit": 0,
                        "prefix": 0,
                        "type": 1,
                        "data": fridge_opened_time.strftime("%Y-%m-%d %H:%M:%S")
                    },
                    {
                        "uid": "SignalStrength",
                        "unit": 22,
                        "prefix": 0,
                        "type": 17,
                        "data": {
                            "rssi": rssi,
                            "snr": snr
                        }
                    },
                    {
                        "uid": "DataRate",
                        "unit": 0,
                        "prefix": 0,
                        "type": 1,
                        "data": data_rate
                    }
                ]

            }

            print("Publishing payload to dashboard:", json.dumps(dashboard_json))
            self.dashboard_broker.publish(topic='KitchenIoT', payload=json.dumps(dashboard_json))
