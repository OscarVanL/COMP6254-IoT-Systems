from paho.mqtt.client import Client
import uuid
import json
import yaml
import os
import base64


class Lab2:
    def __init__(self):
        with open(os.path.join('labs', r'Lab2_Mqtt_Secrets.yaml')) as configuration:
            config = yaml.load(configuration, Loader=yaml.FullLoader)
            print(config)

        client = Client(client_id=str(uuid.getnode()), clean_session=False)
        client.username_pw_set(username=config['TTN_MQTT_USER'], password=config['TTN_MQTT_PASS'])
        client.connect(host=config['TTN_MQTT_BROKER'])
        client.subscribe(topic="+/devices/+/up")
        client.on_subscribe = self.on_subscribe
        client.on_connect = self.on_connect
        client.on_message = self.on_message

        while True:
            client.loop()

    def on_subscribe(self, mosq, obj, mid, granted_qos):
        print('----------------')
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_connect(self, client, userdata, flags, rc):
        print('connected:', client._client_id)
        client.subscribe(topic='[topic]', qos=2)

    def on_message(self, client, userdata, message):
        print('----------------')
        print('topic:', message.topic)
        print('message:', message.payload)
        payload_dict = json.loads(message.payload)
        print(payload_dict)
        print("Decoded payload:", base64.b64decode(payload_dict['payload_raw']))
