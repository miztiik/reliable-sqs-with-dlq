# -*- coding: utf-8 -*-

import datetime
import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


"""
.. module: stream_data_producer
    :Actions: Put Records in Kinesis Data Stream
    :copyright: (c) 2021 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""


__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "sqs_data_consumer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    RELIABLE_QUEUE_NAME = os.getenv("RELIABLE_QUEUE_NAME")
    MAX_MSGS_PER_PULL = int(os.getenv("MAX_MSGS_PER_PULL", 2))
    SQS_LONG_POLL_INTERVAL = int(os.getenv("SQS_LONG_POLL_INTERVAL", 20))


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


LOG = set_logging()
sqs_client = boto3.client("sqs")


def get_q_url(sqs_client):
    q = sqs_client.get_queue_url(
        QueueName=GlobalArgs.RELIABLE_QUEUE_NAME).get("QueueUrl")
    LOG.debug(f'{{"q_url":"{q}"}}')
    return q


def get_msgs(q_url, max_msgs, wait_time):
    try:
        msg_batch = sqs_client.receive_message(
            QueueUrl=q_url,
            MaxNumberOfMessages=max_msgs,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=['All']
        )
        LOG.debug(f'{{"msg_batch":"{json.dumps(msg_batch)}"}}')
    except ClientError as e:
        LOG.exception(f"ERROR:{str(e)}")
        raise e
    else:
        return msg_batch


def process_msgs(q_url, msg_batch):
    try:
        m_lst = []
        m_to_del = []
        for m in msg_batch.get("Messages"):
            m_lst.append(m.get("Body"))
            m_to_del.append({
                'Id': m.get("MessageId"),
                'ReceiptHandle': m.get("ReceiptHandle")
            }
            )
        # Delete Processed Messages from Queue
        if m_to_del and len(m_to_del) > 0:
            del_msgs(q_url, m_to_del)
        LOG.info(f'{{"m_lst":"{json.dumps(m_lst)}"}}')
    except ClientError as e:
        LOG.exception(f"ERROR:{str(e)}")
        raise e
    else:
        return m_lst


def del_msgs(q_url, m_to_del):
    sqs_client.delete_message_batch(QueueUrl=q_url, Entries=m_to_del)
    LOG.info(f'{{"m_del_status":True}}')


def lambda_handler(event, context):
    resp = {"status": False}
    LOG.debug(f"Event: {json.dumps(event)}")
    msgs_to_delete = []

    try:
        q_url = get_q_url(sqs_client)
        msg_cnt = 0
        while context.get_remaining_time_in_millis() > 100:
            msg_batch = get_msgs(
                q_url, max_msgs=GlobalArgs.MAX_MSGS_PER_PULL,  wait_time=GlobalArgs.SQS_LONG_POLL_INTERVAL)
            if msg_batch:
                m_lst = process_msgs(q_url, msg_batch)
                msg_cnt += len(m_lst)
            LOG.debug(
                f'{{"remaining_time":{context.get_remaining_time_in_millis()}}}')
        resp["msg_cnt"] = msg_cnt
        resp["status"] = True
        LOG.info(f'{{"resp":{json.dumps(resp)}}}')

    except Exception as e:
        LOG.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
