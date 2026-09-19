import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            {
                "client_id": self.client_id,
                "type": "data",
                "fruit": fruit,
                "amount": amount,
            }
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            {"client_id": self.client_id, "type": "eof"}
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields.get("client_id") != self.client_id:
            return None
        return fields["fruit_top"]