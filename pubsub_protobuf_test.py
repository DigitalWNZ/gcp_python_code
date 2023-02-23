from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from google.protobuf.json_format import MessageToJson
from google.pubsub_v1.types import Encoding
import os
import proto_buf_pb2  # type: ignore

# TODO(developer): Replace these variables before running the sample.
project_id = "agolis-allen-first"
topic_id = "proto_buf_test_bin"
path_to_credential = '/Users/wangez/Downloads/GCP_Credentials/agolis-allen-first-2a651eae4ca4.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_credential

publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)

try:
    # Get the topic encoding type.
    topic = publisher_client.get_topic(request={"topic": topic_path})
    encoding = topic.schema_settings.encoding

    # Instantiate a protoc-generated class defined in `us-states.proto`.
    someMessage=proto_buf_pb2.SomeMessage()
    someMessage.Asset.ancestors=["projects/247839977271","organizations/882592888799"]
    someMessage.Asset.assetType="compute.googleapis.com/Instance"

    someMessage.Priorasset.ancestors=["projects/247839977271","organizations/882592888799"]
    someMessage.Priorasset.assetType="compute.googleapis.com/Instanc"

    someMessage.window.startTime="2023-01-09T05:55:11.291529Z"
    someMessage.priorAssetState="PRESENT"


    # Encode the data according to the message serialization type.
    if encoding == Encoding.BINARY:
        data = someMessage.SerializeToString()
        print(f"Preparing a binary-encoded message:\n{data}")
    elif encoding == Encoding.JSON:
        json_object = MessageToJson(someMessage)
        data = str(json_object).encode("utf-8")
        print(f"Preparing a JSON-encoded message:\n{data}")
    else:
        print(f"No encoding specified in {topic_path}. Abort.")
        exit(0)

    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

except NotFound:
    print(f"{topic_id} not found.")