import boto3

# --------------------------------------------------------------
# Delete all CloudWatch log streams.
# --------------------------------------------------------------


def delete_log_streams():
    next_token = None
    logs = boto3.client('logs')
    log_groups = logs.describe_log_groups()
    log_groups = logs.describe_log_groups(logGroupNamePrefix='/aws/lambda/')

    for log_group in log_groups['logGroups']:
        log_group_name = log_group['logGroupName']
        print("Delete log group:", log_group_name)

        while True:
            if next_token:
                log_streams = logs.describe_log_streams(logGroupName=log_group_name,
                                                        nextToken=next_token)
            else:
                log_streams = logs.describe_log_streams(
                    logGroupName=log_group_name)

            next_token = log_streams.get('nextToken', None)

            for stream in log_streams['logStreams']:
                log_stream_name = stream['logStreamName']
                print("Delete log stream:", log_stream_name)
                # delete_log_stream(log_group_name, log_stream_name, logs)

            if not next_token or len(log_streams['logStreams']) == 0:
                break
