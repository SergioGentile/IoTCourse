from paho.mqtt import client as mqtt_client
import time

"""
    To Handle mqtt connection
"""

class mqtt_handler:

    def __init__(self,
                 broker,
                 port,
                 client_id,
                 username = "",
                 password = "",
                 qos = 0,
                 verbose = False
                 ):
        self.__broker__ = broker
        self.__port__ = port
        self.__client_id__ = client_id
        self.__username__ = username
        self.__password__ = password
        self.__qos__ = qos
        self.__client__ = None
        self.__verbose__ = verbose

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    def connect(self):
        print(f"Connecting to {self.__broker__} broker on port {self.__port__}")
        # Set connecting Client ID
        client = mqtt_client.Client(self.__client_id__)
        client.username_pw_set(self.__username__, self.__password__)
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.connect(self.__broker__, self.__port__)
        self.__client__ = client
        self.__client__.loop_start()

    def on_disconnect(self, client, userdata, rc):
        print("Disconnecting reason  " + str(rc))
        self.__client__.connected_flag = False
        self.__client__.disconnect_flag = True

    def disconnect(self):
        self.__client__.loop_stop()
        self.__client__.disconnect()

    def check_connection(self):
        return self.__client__.is_connected()

    def on_message(self, client, userdata, msg):
        if self.__verbose__:
            print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    def subscribe(self, topic):
        self.__client__.subscribe(topic)
        self.__client__.on_message = self.on_message

    def publish(self, topic, msg):
        time.sleep(1)
        result = self.__client__.publish(topic, msg)
        status = result[0]
        if status == 0:
            if self.__verbose__:
                print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
