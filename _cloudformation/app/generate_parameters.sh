#!/bin/bash

set -e -o pipefail

# Get ConfigurationEndpoint in ElastiCache redis cluster mode by AWS CLI and pass it as cloudformation parameters
# because cloudformation does'nt support to retrive ConfigurationEndpoint by GetAtt function.

replication_group_id=$(aws cloudformation describe-stacks --stack-name diamondb-storage | jq -r -M '.Stacks[0].Outputs | map(select(.["OutputKey"]=="RedisClusterReplicationGroupID"))[0].OutputValue')

redis_configuration_endpoint=$(aws elasticache describe-replication-groups | jq -r -M ".ReplicationGroups | map(select(.ReplicationGroupId == \"${replication_group_id}\"))[0].ConfigurationEndpoint")

export DIAMONDB_REDIS_ADDRESS=$(echo "${redis_configuration_endpoint}" | jq -r -M  .Address)
export DIAMONDB_REDIS_PORT=$(echo "${redis_configuration_endpoint}" | jq -r -M  .Port)

printf "cat <<++EOS\n`cat ./parameters.json`\n++EOS\n" | bash | tr -d "\n"
