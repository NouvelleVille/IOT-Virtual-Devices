import paho.mqtt.client as mqtt_client
import ssl
from time import sleep

from paho.mqtt.client import MQTT_ERR_SUCCESS

BROKER = 'iot.fr-par.scw.cloud'
BROKER_PORT = 8883
device_id = '641e6fa3-8432-4322-9817-f6261d1042f5'

ca_cert = 'certificates/iot-hub-ca.pem'
device_cert = 'certificates/receiver-crt.pem'
device_key = 'certificates/receiver-key.pem'

topic = "lights/sensors"
retain_messages = True

def mqtt_connect() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker")
        else:
            print("Failed to connect to MQTT Broker, return code {}".rc)

    client = mqtt_client.Client(device_id)
    client.tls_set(
        ca_certs=ca_cert,
        certfile=device_cert,
        keyfile=device_key,
        tls_version=ssl.PROTOCOL_TLSv1_2
    )
    client.on_connect = on_connect
    client.connect(BROKER, BROKER_PORT)
    return client


def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):
        print("Received in topic {}, value: {}".format(msg.topic, msg.payload.decode()))

    client.subscribe(topic)
    client.on_message = on_message

def run():
    client = mqtt_connect()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()