import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

def matching_aggregator_fruit(fruit, aggregator_id):
    total = 0
    for c in fruit:
        total += ord(c)
    return (total % AGGREGATION_AMOUNT) == aggregator_id

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.amount_by_fruit = {}
        self.lock = threading.Lock()
        
    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            if client_id not in self.amount_by_fruit:
                self.amount_by_fruit[client_id] = {}
            self.amount_by_fruit[client_id][fruit] = self.amount_by_fruit[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _notify_sums(self, message):
        for i in range(SUM_AMOUNT):
            control_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_{i}"
            )
            control_queue.send(message)
            control_queue.close()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields["type"] == "data":
            self._process_data(fields["client_id"], fields["fruit"], fields["amount"])
        else:
            self._notify_sums(message)
        ack()

    def _flush_client(self, client_id):
        with self.lock:
            totals = {}
            if client_id in self.amount_by_fruit:
                totals = self.amount_by_fruit[client_id]
                del self.amount_by_fruit[client_id]

        for i in range(AGGREGATION_AMOUNT):
            output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, f"{AGGREGATION_PREFIX}_{i}")
            for final_fruit_item in totals.values():
                if not matching_aggregator_fruit(final_fruit_item.fruit, i):
                    continue
                output_queue.send(
                    message_protocol.internal.serialize(
                        {
                            "client_id": client_id,
                            "type": "data",
                            "fruit": final_fruit_item.fruit,
                            "amount": final_fruit_item.amount,
                        }
                    )
                )
            output_queue.send(
                message_protocol.internal.serialize(
                    {"client_id": client_id, "type": "eof"}
                )
            )
            output_queue.close()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._flush_client(fields["client_id"])
        ack()

    def _control_loop(self):
        try:
            control_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_{ID}"
            )
            control_queue.start_consuming(self.process_control_message)
        except Exception:
            logging.exception("Control thread failed")
            os._exit(1)

    def start(self):
        threading.Thread(target=self._control_loop, daemon=True).start()
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()