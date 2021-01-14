import json
from random import randint

import paho.mqtt.client as mqtt_client
import ssl
from time import sleep

from paho.mqtt.client import MQTT_ERR_SUCCESS

BROKER = 'iot.fr-par.scw.cloud'
BROKER_PORT = 8883
device_id = '2bd04a78-1ff8-4f2e-b607-b48d457fe228'

ca_cert = 'certificates/iot-hub-ca.pem'
device_cert = 'certificates/temperature-sensor-1-crt.pem'
device_key = 'certificates/temperature-sensor-1-key.pem'

topic = "temp/sensors"

retain_messages = False


def mqtt_connect() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker")
        else:
            print("Failed to connect to MQTT Broker, return code {}".rc)

    def on_disconnect(client, userdata, rc):
        print("Disconnect form MQTT Broker")

    client = mqtt_client.Client("")
    client.username_pw_set(device_id)
    client.tls_set(
        ca_certs=ca_cert,
        certfile=device_cert,
        keyfile=device_key,
        tls_version=ssl.PROTOCOL_TLSv1_2
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(BROKER, BROKER_PORT)
    return client


def publish(client: mqtt_client):
    while True:
        temperature = str(randint(0, 100))
        payload = {
            'maker': 'maker2',
            'model': 'model2',
            'serial_number': 'temperature-1',
            'temp_in_deg_c': temperature
        }
        result = client.publish(topic, json.dumps(payload), retain=retain_messages)
        if result.rc == MQTT_ERR_SUCCESS:
            print("Pushed {} to topic: {}".format(temperature, topic))
        sleep(3)


def run():
    client = mqtt_connect()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()
