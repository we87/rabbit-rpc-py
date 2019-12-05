# -*- coding: utf-8 -*-
import pika
from pika import ConnectionParameters
from pika.channel import Channel
from pretty_logging import pretty_logger
import json
import traceback
from .helpers import success, error
import time


class Server():

    def __init__(self, queue_name, parameters: ConnectionParameters):
        """
        queue_name: rpc queue
        parameters: rabbit connection parameters
        """
        self._parameters = parameters
        if queue_name is None:
            raise TypeError("queue_name can't be None")
        self._rpc_queue = queue_name
        self._handle_func_map = {}

    def _bind_handle_func(self, handle_name, func):
        if not isinstance(handle_name, str):
            raise TypeError(
                "handle name must be string"
            )
        if handle_name in self._handle_func_map.keys():
            raise AssertionError(
                "bind function mapping is overwriting an existing endpoint function"
            )
        self._handle_func_map[handle_name] = func

    def bind(self, handle_name):
        """
        bind a callable object
        """
        def decorator(func):
            self._bind_handle_func(handle_name, func)
            return func
        return decorator

    def _rpc_response(self, ch: Channel, method, props, body):
        resp = error(msg="invoke failed")
        # invoke
        try:
            params: dict = json.loads(body)
            func: str = params.get("func", None)
            payload: dict = params.get("payload", None)
            if not func:
                resp = error(msg="request params must include func field")
            elif func not in self._handle_func_map.keys():
                resp = error(msg="no func match")
            else:
                pretty_logger.info("ready to invoke: {}".format(func))
                result = self._handle_func_map[func](payload)
                resp = success(result)
        except Exception as e:
            pretty_logger.error("{}".format(traceback.format_exc()))
            resp = error(msg=traceback.format_exc())
        # response
        try:
            ch.basic_publish(
                exchange='',
                routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),  # 验证字段
                body=json.dumps(resp)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            pretty_logger.error("republish failed. err: {}".format(traceback.format_exc()))

    def start(self):
        """
        start the rpc server, this method is block
        """
        while True:
            try:
                conn = pika.BlockingConnection(self._parameters)
                channel = conn.channel()
                channel.queue_declare(queue=self._rpc_queue)
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=self._rpc_queue, on_message_callback=self._rpc_response)
                channel.start_consuming()
            except Exception as e:
                pretty_logger.error("rpc server consumer error")
                pretty_logger.error(traceback.format_exc())
                time.sleep(0.5)
