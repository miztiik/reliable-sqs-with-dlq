# -*- coding: utf-8 -*-

import datetime
import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


"""
.. module: sqs_data_consumer
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
    # https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    # All messages in a failed batch return to the queue, so your function code must be able to process the same message multiple times without side effects.
    try:
        m_process_stat = {}
        m_lst = 0
        m_to_del = []
        err = f'{{"missing_store_id":{True}}}'
        for m in msg_batch:
            if "messageAttributes" in m and "store_id" not in m.get("messageAttributes"):
                LOG.exception(err)
                raise Exception(err)
            else:
                m_lst += 1
        m_process_stat = {
            "s_msgs": m_lst,
            "f_msgs": len(m_to_del)
        }
        LOG.debug(f'{{"m_process_stat":"{json.dumps(m_process_stat)}"}}')
    except ClientError as e:
        LOG.exception(f"ERROR:{str(e)}")
        raise e
    else:
        return m_process_stat


def del_msgs(q_url, m_to_del):
    sqs_client.delete_message_batch(QueueUrl=q_url, Entries=m_to_del)


def lambda_handler(event, context):
    resp = {"status": False}
    LOG.debug(f"Event: {json.dumps(event)}")
    if event["Records"]:
        resp["tot_msgs"] = len(event["Records"])
        LOG.info(f'{{"tot_msgs":{resp["tot_msgs"]}}}')
        q_url = get_q_url(sqs_client)
        m_process_stat = process_msgs(q_url, event["Records"])
        resp["f_msgs"] = m_process_stat.get("f_msgs")
        resp["s_msgs"] = m_process_stat.get("s_msgs")
        resp["status"] = True
        LOG.info(f'{{"resp":{json.dumps(resp)}}}')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
