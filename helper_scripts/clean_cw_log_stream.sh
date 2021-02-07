set -ex


LOG_GROUP_NAME_01="/aws/lambda/data_producer_sqs-message-producer-ingestor-stack"
LOG_GROUP_NAME_02="/aws/lambda/reliable_queue_processor"

aws logs describe-log-groups --query 'logGroups[?starts_with(logGroupName,`$LOG_GROUP_NAME_01`)].logGroupName' --output table | awk '{print $2}' | grep -v ^$ | while read x; do aws logs delete-log-group --log-group-name $x; done
aws logs describe-log-groups --query 'logGroups[?starts_with(logGroupName,`$LOG_GROUP_NAME_02`)].logGroupName' --output table | awk '{print $2}' | grep -v ^$ | while read x; do aws logs delete-log-group --log-group-name $x; done