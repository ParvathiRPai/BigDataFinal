import time
import requests
from kafka import KafkaProducer
from json import dumps
import  json

meetup_dot_com="http://stream.meetup.com/2/rsvps"
kafka_topic_name="meetuprsvptopic"
kafka_bootstrap_server='localhost:9092'

if __name__=='__main__':
    kafka_producer_obj=KafkaProducer(bootstrap_servers=kafka_bootstrap_server, value_serializer=lambda x:dumps(x).encode('utf-8'))
    while True:
        try:
            stream_api_response=requests.get(meetup_dot_com,stream=True)
            if stream_api_response.status_code==200:
                for api_response_message in stream_api_response.iter_lines():
                    print("message received: ")
                    print(api_response_message)
                    print(type(api_response_message))

                    api_response_message=json.loads(api_response_message)
                    print("message to be sent")
                    print(api_response_message)
                    print(type(api_response_message))
                    kafka_producer_obj.send(kafka_topic_name,api_response_message)
                    time.sleep(1)
        except Exception as ex:
            print('connection to meet up API cannot be established')
        print("printing after while loop complete ")



