import pika
from pika import ConnectionParameters
from pika.channel import Channel
import threading
from queue import Queue, Empty
from pretty_logging import pretty_logger
import traceback
from uuid import uuid4
import json
import time
from .helpers import error


class Client(object):

    def __init__(self, queue_name, parameters: ConnectionParameters):
        """
        queue_name: rpc queue
        parameters: rabbit connection parameters
        """
        self._parameters = parameters
        if queue_name is None:
            raise TypeError("queue_name can't be None")
        # invoke channel
        self._conn = None
        self._channel = None
        self._rpc_queue = queue_name
        # callback channel
        self._callback_conn = None
        self._callback_channel = None
        self._callback_queue = None
        self._callback_lock = threading.Lock()
        # feature
        self._feature_map = {}
        self._lock = threading.Lock()
        # start
        self._start()

    def invoke(self, func: str, payload: dict, timeout=3):
        """
        func: function name
        payload: invoke payload
        timeout: timeout second
        """
        # invoke
        corr_id = str(uuid4())
        params = {
            "func": func,
            "payload": payload
        }
        # generate feature
        feature = Queue(1)
        self._lock.acquire()
        self._feature_map[corr_id] = feature
        self._lock.release()
        # publish invoke
        self._callback_lock.acquire()
        if self._callback_queue is None:
            self._callback_lock.release()
            return error(msg="invoke error 0")
        self._callback_lock.release()
        try:
            self._publish(corr_id, params)
        except Exception as e:
            return error(msg="invoke error 1")
        # result
        try:
            result = feature.get(timeout=timeout)
        except Empty as e:
            self._pop_corr(corr_id)
            return error(msg="invoke timeout")
        self._pop_corr(corr_id)
        return json.loads(result)

    def _pop_corr(self, corr_id):
        self._lock.acquire()
        self._feature_map.pop(corr_id)
        self._lock.release()

    def _callback(self, ch: Channel, method, props, body):
        try:
            corr_id = props.correlation_id
            feature: Queue = None
            self._lock.acquire()
            feature = self._feature_map.get(corr_id, None)
            self._lock.release()
            if feature:
                feature.put(body)
            ch.basic_ack(method.delivery_tag)
        except Exception as e:
            pretty_logger.error("rpc client callback error {}".format(traceback.format_exc()))
            ch.basic_reject(method.delivery_tag, requeue=False)

    def _publish(self, corr_id, payload):
        retry = 0
        while True:
            retry += 1
            try:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self._rpc_queue,
                    properties=pika.BasicProperties(
                        reply_to=self._callback_queue,
                        correlation_id=corr_id,
                    ),
                    body=json.dumps(payload))
                break
            except Exception as e:
                self._channel = None
                self._conn = None
                if retry < 3:
                    pretty_logger.error(traceback.format_exc())
                else:
                    raise e

    @property
    def channel(self):
        if self._conn and self._conn.is_open:
            if self._channel and self._channel.is_open:
                return self._channel
            else:
                self._channel = self._conn.channel()
                return self._channel
        else:
            self._conn = pika.BlockingConnection(self._parameters)
            self._channel = self._conn.channel()
            return self._channel

    @property
    def callback_channel(self):
        if self._callback_conn and self._callback_conn.is_open:
            if self._callback_channel and self._callback_channel.is_open:
                return self._callback_channel
            else:
                self._callback_channel = self._callback_conn.channel()
                return self._callback_channel
        else:
            self._callback_conn = pika.BlockingConnection(self._parameters)
            self._callback_channel = self._callback_conn.channel()
            return self._callback_channel

    def _start(self):
        _callback_queue = self.callback_channel.queue_declare(queue='', exclusive=True).method.queue
        self._callback_lock.acquire()
        self._callback_queue = _callback_queue
        self._callback_lock.release()

        def _do_start():
            while True:
                try:
                    if self._callback_queue is None:
                        _callback_queue = self.callback_channel.queue_declare(queue='', exclusive=True).method.queue
                        self._callback_lock.acquire()
                        self._callback_queue = _callback_queue
                        self._callback_lock.release()
                    self.callback_channel.basic_qos(prefetch_count=1)
                    self.callback_channel.basic_consume(queue=self._callback_queue, on_message_callback=self._callback, auto_ack=False)
                    self.callback_channel.start_consuming()
                except Exception as e:
                    self._callback_lock.acquire()
                    self._callback_queue = None
                    self._callback_lock.release()
                    pretty_logger.error("rpc client consumer error")
                    pretty_logger.error(traceback.format_exc())
                    time.sleep(0.5)
        trd = threading.Thread(target=_do_start)
        trd.setDaemon(True)
        trd.start()
