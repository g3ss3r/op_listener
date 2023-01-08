import boto3
import os
import websockets
import asyncio
import json
import time
import datetime
import logging


class IncClass:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(IncClass, cls).__new__(cls)
            cls._id = 0
            cls._found = 0
            cls._received = 0
            cls._reconnections = 0
            cls._start_time = time.time()

        return cls.instance

    def get_found(self):
        return self._found

    def get_id(self):
        return self._id

    def get_received(self):
        return self._received

    def received(self):
        self._received += 1

    def found(self):
        self._found += 1

    def log_reconnect(self):
        self._reconnections += 1

    def get_reconnect_count(self):
        return self._reconnections

    def get_id(self):
        self._id += 1
        return self._id

    def get_start_time(self):
        return self._start_time


async def main():
    try:
        # now = datetime.datetime.today()
        # path = os.path.dirname(os.path.realpath(__file__))
        # log_filename = path + now.strftime("/logs/%d.%m.%Y.log")
        # logging.basicConfig(filename=log_filename, filemode='a', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


        logging.info('init')
        env_rpc_key = os.environ['RPC_KEY']
        env_queue_url = os.environ['QUEUE_URL']
        env_aws_access_key = os.getenv("AWS_ACCESS_KEY")
        env_aws_secret_key = os.getenv("AWS_SECRET_KEY")
        inc = IncClass()

        AWS_S3_CREDS = {
            "aws_access_key_id": env_aws_access_key,
            "aws_secret_access_key": env_aws_secret_key
        }

        client = boto3.client(
            service_name='sqs',
            endpoint_url=env_queue_url,
            region_name='ru-central1',
            **AWS_S3_CREDS
        )
        logging.info('boto3 client created')

        listener = await websockets.connect(env_rpc_key)
        logging.info("listener created")

        # Init subscription
        request = dict(jsonrpc='2.0', id=inc.get_id(), method='eth_subscribe', params=["logs"])
        await listener.send(json.dumps(request))
        await asyncio.wait_for(listener.recv(), timeout=10)
        logging.info("subscription successful")

        last_block = None
        while True:
            #try:
            # Reconnection for listener
            if not listener.open:
                listener = await websockets.connect(env_rpc_key)
                request = dict(jsonrpc='2.0', id=inc.get_id(), method='eth_subscribe', params=["logs"])
                await listener.send(json.dumps(request))
                await asyncio.wait_for(listener.recv(), timeout=5)
                inc.log_reconnect()
                logging.info("reconnecting ...")

            response_str = await asyncio.wait_for(listener.recv(), timeout=5)
            response_json = json.loads(response_str)

            block_number = int(response_json['params']['result']['blockNumber'], 16)
            if last_block is None:
                last_block = block_number

            elif block_number > last_block:
                # Send message to queue
                client.send_message(
                    QueueUrl=env_queue_url,
                    MessageBody=str(last_block),
                    MessageGroupId="op_blocks"

                )
                logging.info("Block found and sended to queue {}".format(last_block))
                inc.found()
                last_block = block_number

    except Exception as e:
        logging.error("Exception: {} {}".format(e.__class__.__name__, e))


if __name__ == '__main__':
    asyncio.run(main())
