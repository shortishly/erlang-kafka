# Erlang Kafka Client

EKC is an Erlang client of the
[Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
currently providing the consumer API of
[Apache Kafka](http://kafka.apache.org) a high-throughput distributed
messaging system.

## Building

EKC uses [erlang.mk](https://github.com/ninenines/erlang.mk). To build run `make`.

[![Build Status](https://travis-ci.org/shortishly/erlang-kafka.svg)](https://travis-ci.org/shortishly/erlang-kafka)


## Client

The Kafka protocol client runs as a OTP `gen_server`.

|Parameter|Description                                   |Default  |Notes                            |
|---------|----------------------------------------------|---------|---------------------------------|
|host     |The hostname of the Kafka broker to connect to|localhost|Hostname or IP address as a tuple|
|port     |The port number used by the Kafka broker      |     9092|                                 |
|client_id|The client ID to send to the Kafka broker     |<<"ekc">>|                                 |

To connect:

```erlang
{ok, C} =  ekc:start([{host, {172,16,1,186}}]).
```

Or when included as part of a supervision hierarchy:

```erlang
{ok, C} =  ekc:start_link([{host, {172,16,1,186}}]).
```

## Metadata

Once you have established a connection you can obtain meta data from
the broker. This metadata details which topics are available, how each
topic is partitioned, the broker which is established as the leader,
together with the host and port for each broker.

```erlang
{ok, Metadata} = ekc:metadata(C). 
{ok,{metadata,[{broker,0,<<"localhost">>,9092}],
              [{topic,no_error,<<"test">>,
                      [{partition,no_error,0,0,undefined,[0],[0],[],[]}]}]}}

```

You can also supply a list of topic names for which you want metadata:

```erlang
ekc:metadata(C, [<<"test">>]). 
{ok,{metadata,[{broker,0,<<"localhost">>,9092}],
              [{topic,no_error,<<"test">>,
                      [{partition,no_error,0,0,undefined,[0],[0],[],[]}]}]}}
```

The returned metadata is an opaque type. You can use the functions in `ekc_broker`, `ekc_message_set`,
`ekc_metadata`, `ekc_partition` and `ekc_topic` to extract individual attributes.

```erlang
[Broker | _] = ekc_metadata:brokers(Metadata),
ekc_broker:host(Broker),
```

The underlying Apache Kafka protocol description can be found [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI).

## Fetch

This API is used to fetch logs from one some topic partitions.

```erlang
ekc:fetch(C, -1, 1000, 1024, [{<<"test">>, [{0, 0, 2048}]}]).

{ok,[{topic,undefined,<<"test">>,
            [{partition,no_error,0,undefined,748420,[],[],[],
                        [{message_set,0,-1788956415,0,0,undefined,
                                      <<"{\"@timestamp\":\"2"...>>},
                         {message_set,1,377195728,0,0,undefined,
                                      <<"{\"@timestamp"...>>},
                         {message_set,2,-1948934329,0,0,undefined,<<"{\"@times"...>>},
                         {message_set,3,923743722,0,0,undefined,<<"{\"@t"...>>},
                         {message_set,4,-830525009,0,0,undefined,<<...>>}]}]}]}
```

The returned metadata is an opaque type. You can use the functions in `ekc_broker`, `ekc_message_set`,
`ekc_metadata`, `ekc_partition` and `ekc_topic` to extract individual attributes.

The underlying Apache Kafka protocol description can be found [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI).

## Offset

This API allows you to obtain the valid offset range available for a topic partition.

The latest offset for a partition is available via:

```erlang

{ok, [Topic | _]} = ekc:offset(C, -1, [{<<"test">>, [{0, -1, 1}]}]),
[Partition] = ekc_topic:partitions(Topic),
ekc_partition:offsets(Partition).

```
