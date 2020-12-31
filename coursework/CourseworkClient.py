import uuid
from dataclasses import dataclass
import yaml
from datetime import datetime, timedelta
import os
import csv
import time
import requests
from paho.mqtt.client import Client
from coursework.KitchenSensor import KitchenSensorParser, KitchenData

csv_file = os.path.join(os.getcwd(), 'sensor_data.csv')


class CourseworkClient:

    def __init__(self):
        with open(os.path.join('coursework', r'CW_Mqtt_Secrets.yaml')) as configuration:
            self.config = yaml.load(configuration, Loader=yaml.FullLoader)
            print(self.config)

        self.mqtt_clients = []

        self.ttn_broker = Client(client_id=str(uuid.getnode()), clean_session=False)
        self.ttn_broker.username_pw_set(username=self.config['TTN_MQTT_USER'],
                                        password=self.config['TTN_MQTT_PASS'])
        self.ttn_broker.connect(host=self.config['TTN_MQTT_BROKER'])
        self.ttn_broker.subscribe(topic="+/devices/+/up")
        self.ttn_broker.on_subscribe = self.on_subscribe
        self.ttn_broker.on_connect = self.on_connect
        self.ttn_broker.on_message = self.on_message
        self.ttn_broker.on_disconnect = self.on_disconnect
        self.last_PIR_triggered = None
        self.last_fridge_opened = None

        # Uncomment to replay saved CSV messages to Graphite
        # replay_csv(self)

        # Start new threads for each broker
        self.ttn_broker.loop_forever()

    def on_subscribe(self, mosq, obj, mid, granted_qos):
        print('----------------')
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_connect(self, client, userdata, flags, rc):
        print('connected:', client._client_id)
        client.subscribe(topic='[topic]', qos=2)

    def on_publish(self, client, userdata, mid):
        print("Published")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("TTN MQTT connection lost. Will re-conect automatically")

    def on_message(self, client, userdata, message):
        print('----------------')
        received_time = datetime.now().astimezone()
        print('Message received at:', received_time)
        print('topic:', message.topic)
        print('message:', message.payload)
        payload = KitchenSensorParser.parse_message(message, received_time)

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

        # Send to grafana graphite
        self.relay_to_grafana(payload)
        print("Logging to CSV")
        log_to_csv(payload)

    def relay_to_grafana(self, payload: KitchenData):

        interval = 120
        graphite_data = [
            {
                "name": "kitcheniot.meta.rssi",
                "value": payload.rssi,
                "interval": interval,
                "unit": "rssi",
                "time": int(payload.time.timestamp()),
                "mtype": "gauge",
            },
            {
                "name": "kitcheniot.meta.snr",
                "value": payload.snr,
                "interval": interval,
                "unit": "dB",
                "time": int(payload.time.timestamp()),
                "mtype": "gauge",
            },
            {
                "name": "kitcheniot.meta.data_rate",
                "value": int(payload.data_rate),
                "interval": interval,
                "unit": "",
                "time": int(payload.time.timestamp()),
                "mtype": "gauge",
            },
            {
                "name": "kitcheniot.sensor.temperature",
                "value": payload.temperature,
                "interval": interval,
                "unit": "°C",
                "time": int(payload.time.timestamp()),
                "mtype": "gauge",
            },
            {
                "name": "kitcheniot.sensor.humidity",
                "value": payload.humidity,
                "interval": interval,
                "unit": "%",
                "time": int(payload.time.timestamp()),
                "mtype": "gauge",
            },
            {
                "name": "kitcheniot.sensor.ldr",
                "value": payload.ldr,
                "interval": interval,
                "unit": "",
                "time": int(payload.time.timestamp()),
                "mtype": "gauge",
            }
        ]

        # Round fridge opened and PIR triggered time to nearest minute
        fridge_opened_rounded = round_minute(payload.fridge_opened_time)
        pir_triggered_rounded = round_minute(payload.PIR_triggered_time)
        # Graphite histogram bins:
        hist_bins = ['0000', '0030', '0100', '0130', '0200', '0230', '0300', '0330', '0400', '0430', '0500', '0530',
                     '0600', '0630', '0700', '0730', '0800', '0830', '0900', '0930', '1000', '1030', '1100', '1130',
                     '1200', '1230', '1300', '1330', '1400', '1430', '1500', '1530', '1600', '1630', '1700', '1730',
                     '1800', '1830', '1900', '1930', '2000', '2030', '2100', '2130', '2200', '2230', '2300', '2330']

        # Don't send duplicate timestamps
        if fridge_opened_rounded != self.last_fridge_opened:
            self.last_fridge_opened = fridge_opened_rounded
            h = fridge_opened_rounded.hour
            m = fridge_opened_rounded.minute
            index = int((h * 60 + m) / 30)

            graphite_data.append({
                "name": "kitcheniot.activity.fridge",
                "value": 1,
                "interval": interval,
                "unit": "",
                "time": int(fridge_opened_rounded.timestamp()),
                "mtype": "gauge",
            })
            graphite_data.append({
                "name": "kitcheniot.activity.fridge.{}".format(hist_bins[index]),
                "value": 1,
                "interval": interval,
                "unit": "",
                "time": int(fridge_opened_rounded.timestamp()),
                "mtype": "gauge",
            })

        if pir_triggered_rounded != self.last_PIR_triggered:
            self.last_PIR_triggered = pir_triggered_rounded
            h = pir_triggered_rounded.hour
            m = pir_triggered_rounded.minute
            index = int((h * 60 + m) / 30)
            graphite_data.append({
                "name": "kitcheniot.activity.pir",
                "value": 1,
                "interval": interval,
                "unit": "",
                "time": int(pir_triggered_rounded.timestamp()),
                "mtype": "gauge",
            })
            graphite_data.append({
                "name": "kitcheniot.activity.pir.{}".format(hist_bins[index]),
                "value": 1,
                "interval": interval,
                "unit": "",
                "time": int(pir_triggered_rounded.timestamp()),
                "mtype": "gauge",
            })

        print("graphite data: ", graphite_data)

        result = requests.post(self.config['GRAPHITE_URL'],
                               auth=(self.config['GRAPHITE_USER'], self.config['GRAPHITE_API_KEY']), json=graphite_data)
        if result.status_code != 200:
            raise Exception(result.text)
        print('%s: %s' % (result.status_code, result.text))


def log_to_csv(payload: KitchenData):
    if os.path.isfile(csv_file):
        # CSV exists, append to end of file
        with open(csv_file, 'a', encoding="utf-8", newline='') as sensor_file:
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
        with open(csv_file, 'w', encoding="utf-8", newline='') as sensor_file:
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


def replay_csv(client: CourseworkClient):
    with open(csv_file, 'r', encoding='utf-8') as past_data:
        reader = csv.reader(past_data, delimiter=',')
        next(reader)  # Skip first row containing headings
        for row in reader:
            parsed = KitchenSensorParser.parse_csv_row(row)
            client.relay_to_grafana(parsed)
            time.sleep(0.1)  # Leave a small delay so we don't spam Graphite


def round_minute(date_time):
    # Rounds to nearest minute by adding a timedelta if seconds>=30
    return (date_time.replace(second=0, microsecond=0, minute=date_time.minute)
            + timedelta(minutes=date_time.second // 30))
