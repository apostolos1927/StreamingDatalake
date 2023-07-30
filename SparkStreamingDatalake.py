import asyncio
import random
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json
import time
import datetime
from datetime import date

EVENT_HUB_CONNECTION_STR = "...."  ##fill in with the connection string from EventHub
EVENT_HUB_NAME = "...."  ##fill in with the EventHub instance name


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    counter = 5
    async with producer:
        # Create a batch.
        while counter > 0:
            event_data_batch = await producer.create_batch()
            DeviceID = random.randint(1, 100)
            d = {
                "deviceID": DeviceID,
                "rpm": random.randint(1, 1000),
                "angle": random.randint(1, 1000),
                "humidity": random.randint(1, 1000),
                "windspeed": random.randint(1, 1000),
                "temperature": random.randint(1, 1000),
                "deviceTimestamp": datetime.datetime.now().strftime(
                    "%d/%m/%Y %H:%M:%S"
                ),
                "deviceDate": date.today().strftime("%d/%m/%Y"),
            }

            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps(d, default=str)))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            time.sleep(3)
            print(d)
            counter = counter - 1


asyncio.run(run())
