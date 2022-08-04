#### Metrics for solutions.a2.kafka.cassandra.CassandraSinkConnector

**MBean:solutions.a2.kafka:type=Cassandra-Sink-Connector-metrics,name=<Connector-Name>,keyspace=<Keyspace-Name>,table=<Table-Name>**

|Attribute Name                        |Type     |Description                                                                                       |
|:-------------------------------------|:--------|:-------------------------------------------------------------------------------------------------|
|StartTime                             |String   |Connector start date and time (ISO format)                                                        |
|ElapsedTimeMillis                     |long     |Elapsed time, milliseconds                                                                        |
|ElapsedTime                           |String   |Elapsed time, Days/Hours/Minutes/Seconds                                                          |
|TotalRecordsCount                     |long     |Total number of records processed by connector                                                    |
|UpsertRecordsCount                    |long     |Total number of UPSERT operations                                                                 |
|UpsertBindMillis                      |long     |Total time spent for performing bind() for UPSERT operations, milliseconds                        |
|UpsertBindTime                        |String   |Total time spent for performing bind() for UPSERT operations, Days/Hours/Minutes/Seconds          |
|UpsertExecMillis                      |long     |Total time spent for performing execute() for UPSERT operations, milliseconds                     |
|UpsertExecTime                        |String   |Total time spent for performing execute() for UPSERT operations, Days/Hours/Minutes/Seconds       |
|UpsertTotalMillis                     |long     |Total time spent for performing UPSERT operations, milliseconds                                   |
|UpsertTotalTime                       |String   |Total time spent for performing UPSERT operations, Days/Hours/Minutes/Seconds                     |
|UpsertPerSecond                       |int      |Average number of UPSERT operations per second                                                    |
|DeleteRecordsCount                    |long     |Total number of DELETE operations                                                                 |
|DeleteBindMillis                      |long     |Total time spent for performing bind() for DELETE operations, milliseconds                        |
|DeleteBindTime                        |String   |Total time spent for performing bind() for DELETE operations, Days/Hours/Minutes/Seconds          |
|DeleteExecMillis                      |long     |Total time spent for performing execute() for DELETE operations, milliseconds                     |
|DeleteExecTime                        |String   |Total time spent for performing execute() for DELETE operations, Days/Hours/Minutes/Seconds       |
|DeleteTotalMillis                     |long     |Total time spent for performing DELETE operations, milliseconds                                   |
|DeleteTotalTime                       |String   |Total time spent for performing DELETE operations, Days/Hours/Minutes/Seconds                     |
|DeletePerSecond                       |int      |Average number of DELETE operations per second                                                    |
