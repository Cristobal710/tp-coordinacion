import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}
        self.eof_count = {}
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        if client_id not in self.fruit_top:
            self.fruit_top[client_id] = []

        for i in range(len(self.fruit_top[client_id])):
            if self.fruit_top[client_id][i].fruit == fruit:
                updated_item = self.fruit_top[client_id].pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(self.fruit_top[client_id], updated_item)
                return
        bisect.insort(self.fruit_top[client_id], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        if client_id not in self.eof_count:
            self.eof_count[client_id] = 0
        self.eof_count[client_id] += 1
        if self.eof_count[client_id] < SUM_AMOUNT:
            return
        
        fruit_chunk = list(self.fruit_top[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize(
            {
                "client_id": client_id,
                "fruit_top": fruit_top
            }
        ))
        del self.fruit_top[client_id]
        del self.eof_count[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if fields["type"] == "data":
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        else:
            self._process_eof(fields["client_id"])
        ack()

    def handle_sigterm(self, signum, frame):
        self.input_queue.stop_consuming()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()