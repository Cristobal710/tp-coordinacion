import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}
        self.fruits_tops_count = {}

    def _send_top_fruits(self, client_id):
        logging.info("Received partial tops from all aggregators")
        fruit_chunk = list(self.fruit_top[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda item: (item.fruit, item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize(
                {"client_id": client_id, "type": "result", "fruit_top": fruit_top}
            )
        )
        del self.fruit_top[client_id]
        del self.fruits_tops_count[client_id]

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields["client_id"]

        if client_id not in self.fruit_top:
            self.fruit_top[client_id] = []
            self.fruits_tops_count[client_id] = 0

        for fruit, amount in fields["fruit_top"]:
            bisect.insort(
                self.fruit_top[client_id], fruit_item.FruitItem(fruit, amount)
            )
        self.fruits_tops_count[client_id] += 1

        if self.fruits_tops_count[client_id] == AGGREGATION_AMOUNT:
            self._send_top_fruits(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()