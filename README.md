# streaming-03-analytics

## kafka_admin_femi.py

2026-05-18 15:50:31 | INFO | A02 | === RUN START ===
2026-05-18 15:50:31 | INFO | A02 | project=A02
2026-05-18 15:50:31 | INFO | A02 | repo_dir=streaming-03-analytics
2026-05-18 15:50:31 | INFO | A02 | python=3.14.2
2026-05-18 15:50:31 | INFO | A02 | os=Windows 11
2026-05-18 15:50:31 | INFO | A02 | shell=powershell
2026-05-18 15:50:31 | INFO | A02 | cwd=.
2026-05-18 15:50:31 | INFO | A02 | github_actions=False
2026-05-18 15:50:31 | INFO | A02 | ========================
2026-05-18 15:50:31 | INFO | A02 | START admin main()
2026-05-18 15:50:31 | INFO | A02 | ========================
2026-05-18 15:50:31 | INFO | A02 | KAFKA_BOOTSTRAP_SERVERS = localhost:9092
2026-05-18 15:50:31 | INFO | A02 | KAFKA_TOPIC             = streaming-03-analytics-femi
2026-05-18 15:50:31 | INFO | A02 | KAFKA_GROUP_ID          = streaming-consumer-group-A
2026-05-18 15:50:31 | INFO | A02 | KAFKA_AUTO_OFFSET_RESET = earliest
2026-05-18 15:50:31 | INFO | A02 | MESSAGE_INTERVAL_SECONDS = 1.0
2026-05-18 15:50:31 | INFO | A02 | MESSAGE_COUNT           = 6
2026-05-18 15:50:31 | INFO | A02 | Verifying Kafka connection...
2026-05-18 15:50:31 | INFO | A02 | Kafka port is reachable.
%3|1779137433.224|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2040ms in state CONNECT)
%3|1779137435.301|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2030ms in state CONNECT, 1 identical
error(s) suppressed)
%3|1779137437.392|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to
ipv4#127.0.0.1:9092 failed: Unknown error (after 2027ms in state CONNECT)
2026-05-18 15:50:39 | INFO | A02 | Topics currently in Kafka: ['streaming-03-analytics-femi']
2026-05-18 15:50:39 | INFO | A02 | Topic 'streaming-03-analytics-femi' already exists. No action needed.
2026-05-18 15:50:39 | INFO | A02 | To start fresh, run:  uv run python -m streaming.kafka_admin_femi
--recreate
2026-05-18 15:50:39 | INFO | A02 | Topics currently in Kafka: ['streaming-03-analytics-femi']
2026-05-18 15:50:39 | INFO | A02 | ========================
2026-05-18 15:50:39 | INFO | A02 | Admin executed successfully!
2026-05-18 15:50:39 | INFO | A02 | ========================

## kafka_producer_femi.py

2026-05-18 15:51:42 | DEBUG | P03 | KAFKA_BOOTSTRAP_SERVERS           = localhost:9092
2026-05-18 15:51:42 | DEBUG | P03 | KAFKA_BROKER_ADDRESS_FAMILY       = v6
2026-05-18 15:51:42 | DEBUG | P03 | KAFKA_TOPIC                       = streaming-03-analytics-femi
2026-05-18 15:51:42 | DEBUG | P03 | KAFKA_CLEAR_TOPIC_ON_START        = true
2026-05-18 15:51:42 | DEBUG | P03 | PRODUCER_MESSAGE_COUNT            = 3
2026-05-18 15:51:42 | DEBUG | P03 | PRODUCER_MESSAGE_INTERVAL_SECONDS = 2
2026-05-18 15:51:42 | DEBUG | P03 | PRODUCER_MAX_MESSAGES             = 50
2026-05-18 15:51:42 | DEBUG | P03 | PRODUCER_POLL_INTERVAL_SECONDS    = 15
2026-05-18 15:51:42 | DEBUG | P03 | KAFKA_GROUP_ID                    = streaming-consumer-group-A
2026-05-18 15:51:42 | DEBUG | P03 | KAFKA_AUTO_OFFSET_RESET           = earliest
2026-05-18 15:51:42 | DEBUG | P03 | CONSUMER_TIMEOUT_SECONDS          = 10
2026-05-18 15:51:42 | DEBUG | P03 | CONSUMER_MAX_MESSAGES             = 1000
2026-05-18 15:51:42 | INFO | P03 | === RUN START ===
2026-05-18 15:51:42 | INFO | P03 | project=P03
2026-05-18 15:51:42 | INFO | P03 | repo_dir=streaming-03-analytics
2026-05-18 15:51:42 | INFO | P03 | python=3.14.2
2026-05-18 15:51:42 | INFO | P03 | os=Windows 11
2026-05-18 15:51:42 | INFO | P03 | shell=powershell
2026-05-18 15:51:42 | INFO | P03 | cwd=.
2026-05-18 15:51:42 | INFO | P03 | github_actions=False
2026-05-18 15:51:42 | INFO | P03 | ========================
2026-05-18 15:51:42 | INFO | P03 | START producer main()
2026-05-18 15:51:42 | INFO | P03 | ========================
2026-05-18 15:51:42 | INFO | P03 | ROOT_DIR = .
2026-05-18 15:51:42 | INFO | P03 | DATA_DIR = data
2026-05-18 15:51:42 | INFO | P03 | SALES_CSV = data\sales.csv
2026-05-18 15:51:42 | INFO | P03 | REGIONS_CSV = data\regions.csv
2026-05-18 15:51:42 | INFO | P03 | PRODUCTS_CSV = data\products.csv
2026-05-18 15:51:42 | INFO | P03 | REJECTED_SALES_CSV = data\output\producer_rejected_sales.csv
2026-05-18 15:51:42 | INFO | P03 | ========================
2026-05-18 15:51:42 | INFO | P03 | SECTION A. Acquire
2026-05-18 15:51:42 | INFO | P03 | ========================
2026-05-18 15:51:42 | INFO | P03 | Loading settings from .env...
2026-05-18 15:51:42 | INFO | P03 | KAFKA_BOOTSTRAP_SERVERS           = localhost:9092
2026-05-18 15:51:42 | INFO | P03 | KAFKA_TOPIC                       = streaming-03-analytics-femi
2026-05-18 15:51:42 | INFO | P03 | PRODUCER_MESSAGE_COUNT            = 3
2026-05-18 15:51:42 | INFO | P03 | PRODUCER_MESSAGE_INTERVAL_SECONDS = 2.0
2026-05-18 15:51:42 | INFO | P03 | KAFKA_CLEAR_TOPIC_ON_START        = True
2026-05-18 15:51:42 | INFO | P03 | Verifying Kafka connection...
2026-05-18 15:51:42 | INFO | P03 | Kafka port is reachable.
%3|1779137504.905|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2032ms in state CONNECT)
%3|1779137506.969|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2016ms in state CONNECT, 1 identical
error(s) suppressed)
%3|1779137511.958|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to
ipv4#127.0.0.1:9092 failed: Unknown error (after 2019ms in state CONNECT)
2026-05-18 15:51:52 | INFO | P03 | Loading validation reference data...
2026-05-18 15:51:52 | INFO | P03 | Found 6 valid regions,
2026-05-18 15:51:52 | INFO | P03 | 6 valid products.
2026-05-18 15:51:52 | INFO | P03 | ========================
2026-05-18 15:51:52 | INFO | P03 | SECTION P. Produce Messages
2026-05-18 15:51:52 | INFO | P03 | ========================
2026-05-18 15:51:52 | INFO | P03 | Initializing output...
2026-05-18 15:51:52 | INFO | P03 | Output directory ready: output
2026-05-18 15:51:52 | INFO | P03 | Sending messages...
2026-05-18 15:51:52 | INFO | P03 | Sending up to 3 message(s) to topic 'streaming-03-analytics-femi'.
2026-05-18 15:51:52 | INFO | P03 | Watch each sale arrive. Press CTRL+C to stop early.

2026-05-18 15:51:52 | INFO | P03 | {
  order_id: e7324981-a9f0-419f-b708-d0a333451fff
  datetime: 2026-05-04T08:11:00Z
  region_id: US-TX
  currency_code: USD
  product_id: PY-STREAM-005
  unit_price: 59.99
  quantity: 3
  is_online: true
  customer_id: CUST-4150
  is_new_customer: false
  device_type: tablet
  payment_method: paypal
  referral_source: paid_search
  discount_code:
  customer_note: Gift for my team
}
2026-05-18 15:51:52 | INFO | P03 |   Sending message with key=US-TX
2026-05-18 15:51:52 | INFO | P03 |   MESSAGE SENT  sent=1
2026-05-18 15:51:54 | INFO | P03 | {
  order_id: d61943e0-f543-4b5f-9c9a-18605ea4cfe5
  datetime: 2026-05-04T08:23:00Z
  region_id: US-TX
  currency_code: USD
  product_id: PY-DATA-002
  unit_price: 49.99
  quantity: 1
  is_online: true
  customer_id: CUST-1106
  is_new_customer: false
  device_type: mobile
  payment_method: paypal
  referral_source: paid_search
  discount_code:
  customer_note: Gift for my team
}
2026-05-18 15:51:54 | INFO | P03 |   Sending message with key=US-TX
2026-05-18 15:51:54 | INFO | P03 |   MESSAGE SENT  sent=2
2026-05-18 15:51:56 | INFO | P03 | {
  order_id: 14da1915-8e74-47be-9e10-f7275d31af46
  datetime: 2026-05-04T08:28:00Z
  region_id: CA-QC
  currency_code: CAD
  product_id: PY-NLP-006
  unit_price: 54.99
  quantity: 1
  is_online: true
  customer_id: CUST-2133
  is_new_customer: false
  device_type: desktop
  payment_method: paypal
  referral_source: organic
  discount_code:
  customer_note: Learning at my own pace
}
2026-05-18 15:51:56 | INFO | P03 |   Sending message with key=CA-QC
2026-05-18 15:51:56 | INFO | P03 |   MESSAGE SENT  sent=3
2026-05-18 15:51:58 | INFO | P03 | Checking for rejected records...
2026-05-18 15:51:58 | INFO | P03 |   No records rejected.
2026-05-18 15:51:58 | INFO | P03 | ========================
2026-05-18 15:51:58 | INFO | P03 | SECTION E. Exit
2026-05-18 15:51:58 | INFO | P03 | ========================
2026-05-18 15:51:58 | INFO | P03 | Summary:
2026-05-18 15:51:58 | INFO | P03 | Sent 3 message(s) to topic 'streaming-03-analytics-femi'.
2026-05-18 15:51:58 | INFO | P03 | Rejected 0 message(s).
2026-05-18 15:51:58 | INFO | P03 | ========================
2026-05-18 15:51:58 | INFO | P03 | Producer executed successfully!
2026-05-18 15:51:58 | INFO | P03 | ========================

## kafka_consumer_femi.py

2026-05-18 15:53:04 | DEBUG | C03 | KAFKA_BOOTSTRAP_SERVERS           = localhost:9092
2026-05-18 15:53:04 | DEBUG | C03 | KAFKA_BROKER_ADDRESS_FAMILY       = v6
2026-05-18 15:53:04 | DEBUG | C03 | KAFKA_TOPIC                       = streaming-03-analytics-femi
2026-05-18 15:53:04 | DEBUG | C03 | KAFKA_CLEAR_TOPIC_ON_START        = true
2026-05-18 15:53:04 | DEBUG | C03 | PRODUCER_MESSAGE_COUNT            = 3
2026-05-18 15:53:04 | DEBUG | C03 | PRODUCER_MESSAGE_INTERVAL_SECONDS = 2
2026-05-18 15:53:04 | DEBUG | C03 | PRODUCER_MAX_MESSAGES             = 50
2026-05-18 15:53:04 | DEBUG | C03 | PRODUCER_POLL_INTERVAL_SECONDS    = 15
2026-05-18 15:53:04 | DEBUG | C03 | KAFKA_GROUP_ID                    = streaming-consumer-group-A
2026-05-18 15:53:04 | DEBUG | C03 | KAFKA_AUTO_OFFSET_RESET           = earliest
2026-05-18 15:53:04 | DEBUG | C03 | CONSUMER_TIMEOUT_SECONDS          = 10
2026-05-18 15:53:04 | DEBUG | C03 | CONSUMER_MAX_MESSAGES             = 1000
2026-05-18 15:53:04 | INFO | C03 | === RUN START ===
2026-05-18 15:53:04 | INFO | C03 | project=C03
2026-05-18 15:53:04 | INFO | C03 | repo_dir=streaming-03-analytics
2026-05-18 15:53:04 | INFO | C03 | python=3.14.2
2026-05-18 15:53:04 | INFO | C03 | os=Windows 11
2026-05-18 15:53:04 | INFO | C03 | shell=powershell
2026-05-18 15:53:04 | INFO | C03 | cwd=.
2026-05-18 15:53:04 | INFO | C03 | github_actions=False
2026-05-18 15:53:04 | INFO | C03 | ========================
2026-05-18 15:53:04 | INFO | C03 | START consumer main()
2026-05-18 15:53:04 | INFO | C03 | ========================
2026-05-18 15:53:04 | INFO | C03 | ROOT_DIR = .
2026-05-18 15:53:04 | INFO | C03 | DATA_DIR = data
2026-05-18 15:53:04 | INFO | C03 | OUTPUT_CSV = data\output\consumed_sales.csv
2026-05-18 15:53:04 | INFO | C03 | REGIONS_CSV = data\regions.csv
2026-05-18 15:53:04 | INFO | C03 | PRODUCTS_CSV = data\products.csv
2026-05-18 15:53:04 | INFO | C03 | CURRENCIES_CSV = data\currencies.csv
2026-05-18 15:53:04 | INFO | C03 | DISCOUNT_CODES_CSV = data\discount_codes.csv
2026-05-18 15:53:04 | INFO | C03 | ========================
2026-05-18 15:53:04 | INFO | C03 | SECTION A. Acquire
2026-05-18 15:53:04 | INFO | C03 | ========================
2026-05-18 15:53:04 | INFO | C03 | Loading settings from .env...
2026-05-18 15:53:04 | INFO | C03 | KAFKA_BOOTSTRAP_SERVERS  = localhost:9092
2026-05-18 15:53:04 | INFO | C03 | KAFKA_TOPIC              = streaming-03-analytics-femi
2026-05-18 15:53:04 | INFO | C03 | KAFKA_GROUP_ID           = streaming-consumer-group-A
2026-05-18 15:53:04 | INFO | C03 | CONSUMER_TIMEOUT_SECONDS = 10.0
2026-05-18 15:53:04 | INFO | C03 | CONSUMER_MAX_MESSAGES    = 1000
2026-05-18 15:53:04 | INFO | C03 | Verifying Kafka connection...
2026-05-18 15:53:04 | INFO | C03 | Kafka port is reachable.
2026-05-18 15:53:04 | INFO | C03 | Verifying Kafka topic...
%3|1779137586.965|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2029ms in state CONNECT)
%3|1779137588.996|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2029ms in state CONNECT)
%3|1779137589.059|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1:
Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2031ms in state CONNECT, 1 identical
error(s) suppressed)
2026-05-18 15:53:09 | INFO | C03 | Topic 'streaming-03-analytics-femi' exists.
2026-05-18 15:53:09 | INFO | C03 | Found 3 message(s) available.
2026-05-18 15:53:09 | INFO | C03 | Creating Kafka consumer...
2026-05-18 15:53:09 | INFO | C03 | Subscribed to topic: 'streaming-03-analytics-femi'
(reading from beginning)
2026-05-18 15:53:09 | INFO | C03 | ========================
2026-05-18 15:53:09 | INFO | C03 | SECTION C. Consume and Process Messages
2026-05-18 15:53:09 | INFO | C03 | ========================
2026-05-18 15:53:09 | INFO | C03 | Initializing output...
2026-05-18 15:53:09 | INFO | C03 | Output CSV cleared: consumed_sales.csv
2026-05-18 15:53:09 | INFO | C03 | Loading enrichment reference data...
2026-05-18 15:53:09 | INFO | C03 | Found 6 region tax rates.
2026-05-18 15:53:09 | INFO | C03 | Consuming messages...
2026-05-18 15:53:09 | INFO | C03 | Waiting for up to 1000 message(s).
2026-05-18 15:53:09 | INFO | C03 | Press CTRL+C to stop early.

2026-05-18 15:53:15 | INFO | C03 | {'currency_code': 'USD', 'customer_id': 'CUST-4150',
'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:11:00Z', 'device_type': 'tablet',
'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id':
'e7324981-a9f0-419f-b708-d0a333451fff', 'payment_method': 'paypal', 'product_id': 'PY-STREAM-005',
'quantity': '3', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '59.99',
'_kafka_key': 'US-TX', '_kafka_partition': 0, '_kafka_offset': 0}
2026-05-18 15:53:15 | INFO | C03 | subtotal=179.97
2026-05-18 15:53:15 | INFO | C03 | tax=14.85
2026-05-18 15:53:15 | INFO | C03 | total=194.82
2026-05-18 15:53:15 | INFO | C03 | running_total=194.82
2026-05-18 15:53:15 | INFO | C03 | MESSAGE ACCEPTED
2026-05-18 15:53:15 | INFO | C03 | order=e7324981-a9f0-419f-b708-d0a333451fff
2026-05-18 15:53:15 | INFO | C03 | total=$194.82
2026-05-18 15:53:15 | INFO | C03 | consumed=1
2026-05-18 15:53:15 | INFO | C03 | RUNNING STATS
2026-05-18 15:53:15 | INFO | C03 | total_sales=$194.82
2026-05-18 15:53:15 | INFO | C03 | average=$194.82
2026-05-18 15:53:15 | INFO | C03 | min=$194.82
2026-05-18 15:53:15 | INFO | C03 | max=$194.82
2026-05-18 15:53:15 | INFO | C03 | {'currency_code': 'USD', 'customer_id': 'CUST-1106',
'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:23:00Z', 'device_type': 'mobile',
'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id':
'd61943e0-f543-4b5f-9c9a-18605ea4cfe5', 'payment_method': 'paypal', 'product_id': 'PY-DATA-002',
'quantity': '1', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '49.99',
'_kafka_key': 'US-TX', '_kafka_partition': 0, '_kafka_offset': 1}
2026-05-18 15:53:15 | INFO | C03 | subtotal=49.99
2026-05-18 15:53:15 | INFO | C03 | tax=4.12
2026-05-18 15:53:15 | INFO | C03 | total=54.11
2026-05-18 15:53:15 | INFO | C03 | running_total=248.93
2026-05-18 15:53:15 | INFO | C03 | MESSAGE ACCEPTED
2026-05-18 15:53:15 | INFO | C03 | order=d61943e0-f543-4b5f-9c9a-18605ea4cfe5
2026-05-18 15:53:15 | INFO | C03 | total=$54.11
2026-05-18 15:53:15 | INFO | C03 | consumed=2
2026-05-18 15:53:15 | INFO | C03 | RUNNING STATS
2026-05-18 15:53:15 | INFO | C03 | total_sales=$248.93
2026-05-18 15:53:15 | INFO | C03 | average=$124.47
2026-05-18 15:53:15 | INFO | C03 | min=$54.11
2026-05-18 15:53:15 | INFO | C03 | max=$194.82
2026-05-18 15:53:15 | INFO | C03 | {'currency_code': 'CAD', 'customer_id': 'CUST-2133',
'customer_note': 'Learning at my own pace', 'datetime': '2026-05-04T08:28:00Z', 'device_type':
'desktop', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id':
'14da1915-8e74-47be-9e10-f7275d31af46', 'payment_method': 'paypal', 'product_id': 'PY-NLP-006',
'quantity': '1', 'referral_source': 'organic', 'region_id': 'CA-QC', 'unit_price': '54.99',
'_kafka_key': 'CA-QC', '_kafka_partition': 0, '_kafka_offset': 2}
2026-05-18 15:53:15 | INFO | C03 | subtotal=54.99
2026-05-18 15:53:15 | INFO | C03 | tax=8.23
2026-05-18 15:53:15 | INFO | C03 | total=63.22
2026-05-18 15:53:15 | INFO | C03 | running_total=312.15
2026-05-18 15:53:15 | INFO | C03 | MESSAGE ACCEPTED
2026-05-18 15:53:15 | INFO | C03 | order=14da1915-8e74-47be-9e10-f7275d31af46
2026-05-18 15:53:15 | INFO | C03 | total=$63.22
2026-05-18 15:53:15 | INFO | C03 | consumed=3
2026-05-18 15:53:15 | INFO | C03 | RUNNING STATS
2026-05-18 15:53:15 | INFO | C03 | total_sales=$312.15
2026-05-18 15:53:15 | INFO | C03 | average=$104.05
2026-05-18 15:53:15 | INFO | C03 | min=$54.11
2026-05-18 15:53:15 | INFO | C03 | max=$194.82
2026-05-18 15:53:25 | INFO | C03 | No message received within 10.0s timeout.
2026-05-18 15:53:25 | INFO | C03 | Producer finished or paused. Stopping consumer.
2026-05-18 15:53:25 | INFO | C03 | Kafka consumer closed.
2026-05-18 15:53:25 | INFO | C03 | ========================
2026-05-18 15:53:25 | INFO | C03 | SECTION E. Exit
2026-05-18 15:53:25 | INFO | C03 | ========================
2026-05-18 15:53:25 | INFO | C03 | Summary:
2026-05-18 15:53:25 | INFO | C03 | Consumed 3 message(s) from topic 'streaming-03-analytics-femi'.
2026-05-18 15:53:25 | INFO | C03 | Skipped  0 message(s).
2026-05-18 15:53:25 | INFO | C03 | OUTPUT_CSV = data\output\consumed_sales.csv
2026-05-18 15:53:25 | INFO | C03 |   Total sales:  $312.15
2026-05-18 15:53:25 | INFO | C03 |   Average sale: $104.05
2026-05-18 15:53:25 | INFO | C03 |   Minimum sale: $54.11
2026-05-18 15:53:25 | INFO | C03 |   Maximum sale: $194.82
2026-05-18 15:53:25 | INFO | C03 | ========================
2026-05-18 15:53:25 | INFO | C03 | Consumer executed successfully!
2026-05-18 15:53:25 | INFO | C03 | ========================

[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: validate and summarize messages.

Streaming analytics requires working with data in motion
and distributed, scalable systems.
This course builds capabilities through working projects.
In the age of generative AI, durable skills are grounded in real work:
setting up a professional environment,
reading and running code,
understanding the logic,
and pushing work to a shared repository.
Each project follows the structure of professional Python projects.
We learn by doing.

## This Project

This project focuses on analytics performed as messages are consumed.

The project uses Kafka to move sales messages from a producer to a consumer.
The consumer reads each message, validates required fields, computes derived values,
writes processed records to CSV, and logs running summary statistics.

This module adds validation and message-by-message analytics to the streaming workflow.

The goal is to see how each incoming message can be checked, transformed,
and summarized without waiting for a batch process.

## Working Files

You'll work with just these areas:

- **data/** - input data and generated output files
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - update authorship & links
- **zensical.toml** - update authorship & links

## Instructions

Follow the
[step-by-step workflow guide](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to complete:

1. Phase 1. **Start & Run**
2. Phase 2. **Change Authorship**
3. Phase 3. **Read & Understand**
4. Phase 4. **Modify**
5. Phase 5. **Apply**

## Challenges

Challenges are expected.
Sometimes instructions may not quite match your operating system.
When issues occur, share screenshots, error messages, and details about what you tried.
Working through issues is part of implementing professional projects.

## Success

After completing Phase 1. **Start & Run**, you'll have your own GitHub project
running with Kafka.

Use four named terminals:

1. **kafka** - keep the Kafka message broker running
2. **topics** - create, list, or reset Kafka topics
3. **producer** - run the project and producer
4. **consumer** - run the consumer

After the producer and consumer run successfully, you should see:

```shell
========================
Consumer executed successfully!
========================
```

A new file `project.log` will appear in the root project folder
and processed data will appear in data/output/.

## Command Reference

The commands below are used in the workflow guide above.
They are provided here for convenience.

**Important:** the first few times you run a project,
follow the guide with the **complete instructions**.

<details>
<summary>Show command reference</summary>

### In a machine terminal (open in your `Repos` folder)

After you get a copy of this repo in your own GitHub account,
open a machine terminal in your `Repos` folder:

```bash
# Replace username with YOUR GitHub username.
git clone https://github.com/Airfirm/streaming-03-analytics

cd streaming-03-analytics
code .
```

### In VS Code Terminal 1: Start Kafka (kafka)

For full instructions see
[**start kafka**](https://denisecase.github.io/pro-analytics-02/kafka/start-kafka/).

If any command fails,
repeat the steps at
[**install kafka**](https://denisecase.github.io/pro-analytics-02/kafka/install-kafka/)
until starting up is reliable.

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"

"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

### In VS Code terminal 2: Create Topic (topics)

For full instructions see
[**create topic**](https://denisecase.github.io/pro-analytics-02/kafka/create-topic/).

The topic name must match the name defined in your
`.env` file (copy `.env.example` to `.env`).

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-03-analytics-case
```

### In VS Code Terminal 3: Run Project and Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
# reset uv cache only if/when you start getting strange dependency errors
# uv cache clean

uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

uvx pre-commit install

git add -A
uvx pre-commit run --all-files
# repeat if changes were made
git add -A
uvx pre-commit run --all-files

# run the producer
clear
uv run python -m streaming.kafka_producer_case

# do chores
uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build

# save progress
git add -A
git commit -m "update"
git push -u origin main
```

### In VS Code Terminal 4: Run Consumer (consumer)

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.
Clear the terminal, then start the consumer.

```shell
clear
uv run python -m streaming.kafka_consumer_case
```

To start fresh, see
[manage topics](https://denisecase.github.io/pro-analytics-02/kafka/manage-topics/)
to delete the topic and recreate it.

</details>

## Notes

- Use the **UP ARROW** and **DOWN ARROW** in the terminal to scroll through past commands.
- Use `CTRL+f` to find (and replace) text within a file.
- You do not need to add to or modify `tests/`. They are provided for example only.
- Many files are silent helpers. Explore as you like, but nothing is required.
- You do NOT not to understand everything; understanding builds naturally over time.

## Troubleshooting >>> or

If you see something like this in your terminal: `>>>` or `...`
You accidentally started Python interactive mode.
It happens.
Press `Ctrl+c` (both keys together) or `Ctrl+Z` then `Enter` on Windows.
