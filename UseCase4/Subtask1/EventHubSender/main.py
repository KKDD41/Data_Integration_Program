import asyncio
import csv
import json
import pathlib

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient


EVENT_HUB_NAME = "evh-youflix"
FILES_DIR = 'dummy_dir'
BATCH_SIZE = 1000
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://evhns-di-mentoring-ed.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SLuiGOf3a3+tMUueI1688Sg1fldNvw9JZ+AEhAOVcuI="

async def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    path = pathlib.Path(FILES_DIR)
    files = sorted(
        '/'.join(item.parts)
        for item in path.iterdir()
        if item.is_file()
    )

    print(f'files will be uploaded in following order: {files}')

    async with producer:
        for file in files:
            with open(file, 'r') as f:
                reader = csv.DictReader(f)

                event_data_batch = await producer.create_batch()
                for row in reader:
                    event_data_batch.add(EventData(json.dumps(row)))
                    if len(event_data_batch) >= BATCH_SIZE:
                        print(event_data_batch)
                        await producer.send_batch(event_data_batch)
                        event_data_batch = await producer.create_batch()

                if len(event_data_batch):
                    print(event_data_batch)
                    await producer.send_batch(event_data_batch)


if __name__ == '__main__':
    asyncio.run(main())
