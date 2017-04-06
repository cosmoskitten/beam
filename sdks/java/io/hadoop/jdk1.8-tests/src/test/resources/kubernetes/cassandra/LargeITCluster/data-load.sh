# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to load data using YCSB on Cassandra multi node cluster.
 
#!/bin/bash

set -e
recordcount=1000

running_seed="$(kubectl get pods -l app=cassandra -o jsonpath="{.items[0].metadata.name}")"
num_of_replicas=$(kubectl get statefulset cassandra --output=jsonpath={.spec.replicas})

echo "Script to load data on $num_of_replicas replicas"
echo "Waiting for Cassandra pods to be in ready state"

# Wait until all the pods configured as per number of replicas, come in running state
i=0
while [ $i -lt $num_of_replicas ]
do
   container_state="$(kubectl get pods -l app=cassandra -o jsonpath="{.items[$i].status.containerStatuses[0].ready}")"
   while ! $container_state; do
      sleep 10s
      container_state="$(kubectl get pods -l app=cassandra -o jsonpath=\
      "{.items[$i].status.containerStatuses[0].ready}")"
      echo "."
   done
   ready_pod="$(kubectl get pods -l app=cassandra -o jsonpath="{.items[$i].metadata.name}")"
   echo "$ready_pod is ready"
   i=$((i+1))
done

# After starting the service, it takes couple of minutes to generate the external IP for the
# service. Hence, wait for sometime and identify external IP of the pod
external_ip="$(kubectl get svc cassandra-external -o jsonpath=\
'{.status.loadBalancer.ingress[0].ip}')"
echo "Waiting for the Cassandra service to come up ........"
while [ -z "$external_ip" ]
do
   sleep 10s
   external_ip="$(kubectl get svc cassandra-external -o jsonpath=\
   '{.status.loadBalancer.ingress[0].ip}')"
   echo "."
done
echo "External IP - $external_ip"

echo "Loading data"
# Create keyspace
keyspace_creation_command="drop keyspace if exists ycsb;create keyspace ycsb WITH REPLICATION = {\
'class' : 'SimpleStrategy', 'replication_factor': 3 };"
kubectl exec -ti $running_seed -- cqlsh -e "$keyspace_creation_command"
echo "Keyspace creation............"
echo "-----------------------------"
echo "$keyspace_creation_command"
echo

# Create table
table_creation_command="use ycsb;drop table if exists usertable;create table usertable (\
y_id varchar primary key,field0 varchar,field1 varchar,field2 varchar,field3 varchar,\
field4 varchar,field5 varchar,field6 varchar,field7 varchar,field8 varchar,field9 varchar);"
kubectl exec -ti $running_seed -- cqlsh -e "$table_creation_command"
echo "Table creation .............."
echo "-----------------------------"
echo "$table_creation_command"

cd ycsb-0.12.0

echo "Starting to load data on ${external_ip}"
echo "-----------------------------"
# Record count set to 1000, change this value to load as per requirement.
# dataintegrity flag is set to true to load deterministic data
./bin/ycsb load cassandra-cql -p hosts=${external_ip} -p dataintegrity=true -p recordcount=\
${recordcount} -p insertorder=ordered -p fieldlength=20 -P workloads/workloadd \
-s > workloada_load_res.txt
