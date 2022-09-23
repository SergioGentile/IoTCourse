from email import utils
import sys
import hat
import mqtt_handler
from graceful_killer import GracefulKiller
from utils import getserial
import json
import time

# Raspberry serial number
SERIAL_NUMBER = getserial()
DATA_FREQUENCE = 1

def main():
    #PUT HERE YOUR CODE
    sense_hat = hat.hat()

    # Compose the message for MQTT
    timestamp = int(time.time())
    env_data = sense_hat.get_env()
    motion_data = sense_hat.get_movement()
    env_data['timestamp'] = timestamp
    motion_data['timestamp'] = timestamp

    # Connect to MQTT
    # client_id: should be unique
    # broker and port: comes from the connecton
    # QoS:
    # 0: you send the message but you are not sure that the other receive the message
    # 1: you are sure that the other part will receive the message, but you don't know how many time
    # 2: we are sure about the communication

    # We will use a public broker
    BROKER_URL = "broker.hivemq.com"
    BROKER_PORT = 1883
    CLIENT_ID = SERIAL_NUMBER
    
    mqtt_client = mqtt_handler.mqtt_handler(
        BROKER_URL,
        BROKER_PORT,
        CLIENT_ID
    )

    # Graceful kill of the app
    grace_killer = GracefulKiller()

    # manage connection
    mqtt_client.connect()
    # Connect to the broker and ensure that it works
    while True:
        if mqtt_client.check_connection():
            break
    print(f"Your serial number: {SERIAL_NUMBER}")

    topic_env = f'IoTCourseData/{SERIAL_NUMBER}/Environment'
    topic_motion = f'IoTCourseData/{SERIAL_NUMBER}/Environment'

    while not grace_killer.is_killed():
        mqtt_client.publish(topic_env, json.dumps(env_data))
        mqtt_client.publish(topic_motion, json.dumps(motion_data))
        time.sleep(DATA_FREQUENCE) # Data Frequency. Remember to specify it otherwise the database will explode


    mqtt_client.disconnect()
    

    pass


if __name__ == "__main__":
    main()
