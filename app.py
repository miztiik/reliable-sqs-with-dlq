#!/usr/bin/env python3

from aws_cdk import core

from reliable_sqs_with_dlq.reliable_sqs_with_dlq_stack import ReliableSqsWithDlqStack


app = core.App()
ReliableSqsWithDlqStack(app, "reliable-sqs-with-dlq")

app.synth()
