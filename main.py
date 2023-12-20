import json
import os

import coalition_service.coalition_service_pb2 as coalition_service_pb2
import coalition_service.coalition_service_pb2_grpc as coalition_service_pb2_grpc
import grpc
import pika
import school_service.school_service_pb2 as school_pb2
import school_service.school_service_pb2_grpc as school_pb2_grpc
from dotenv import load_dotenv

load_dotenv()
school_service_channel = grpc.insecure_channel(f'{os.getenv("SCHOOL_SERVICE_HOST")}:{os.getenv("SCHOOL_SERVICE_PORT")}')
school_service_stub = school_pb2_grpc.SchoolServiceStub(school_service_channel)

coalition_channel = grpc.insecure_channel(f'{os.getenv("COALITION_SERVICE_HOST")}:{os.getenv("COALITION_SERVICE_PORT")}')
coalition_stub = coalition_service_pb2_grpc.CoalitionServiceStub(coalition_channel)


CURRENT_COUNT = 853
LIMIT = 10

credentials = pika.PlainCredentials(username=os.getenv("RABBITMQ_USERNAME"), password=os.getenv("RABBITMQ_PASSWORD"))
connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv("RABBITMQ_HOST"), port=os.getenv("RABBITMQ_PORT"), credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='actualizator_queue')


def get_school_info():
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    response = school_service_stub.get_school_info(school_pb2.GetSchoolRequest(username=username, password=password))
    print(response)
    return response.access_token


def get_all_members_from_platform():
    access_token = get_school_info()
    for offset in range(0, CURRENT_COUNT, LIMIT):
        print("request for offset: ", offset)
        response = school_service_stub.get_all_members_from_platform(
            school_pb2.GetAllMembersFromPlatformRequest(access_token=access_token, offset=offset, limit=LIMIT))
        print(response.status)
        if response.status == 1:
            return []
        yield response.members


def send_to_queue():
    members = get_all_members_from_platform()
    print(members)
    counter = 0
    for member_obj in members:
        for member in member_obj:
            data = {"login": member.login, "school_user_id": member.school_user_id}
            message_body = json.dumps(data)
            channel.basic_publish(exchange='', routing_key='actualizator_queue', body=message_body)
            counter += 1
            print(f" [x] Sent '{message_body}' to actualizator_queue")
    print(counter)


def main():
    request = coalition_service_pb2.Empty()
    coalition_stub.reset_all_members(request)
    send_to_queue()
    connection.close()


if __name__ == "__main__":
    main()
