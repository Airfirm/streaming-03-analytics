# Streaming 03 Analytics Project

## Hosted Documentation

Readers can access my hosted documentation site from the GitHub repository About section. The About
section should include the GitHub Pages link for this project.

Example site format:

`https://Airfirm.github.io/streaming-03-analytics/`

## Custom Project

### Dataset

For this custom Kafka streaming project, my producer uses the dataset file:

`data/sales.csv`

This file contains simulated online sales transaction records. Each row represents one sales order.
The records include information about the order, customer, product, region, payment method, device
type, referral source, quantity, and unit price.

The sales records include fields such as:

- `order_id`
- `datetime`
- `region_id`
- `currency_code`
- `product_id`
- `unit_price`
- `quantity`
- `is_online`
- `customer_id`
- `is_new_customer`
- `device_type`
- `payment_method`
- `referral_source`
- `discount_code`
- `customer_note`

I used the original sales dataset as the main streaming source. I did not change the original
`sales.csv` file itself. Instead, I modified the producer and consumer logic to validate, enrich,
and analyze the records as they move through the Kafka pipeline.

I also used reference datasets for validation and enrichment:

- `data/regions.csv`
- `data/products.csv`
- `data/currencies.csv`
- `data/discount_codes.csv`

The producer uses `regions.csv` and `products.csv` to validate sales records before sending them to
Kafka. The consumer uses `regions.csv` to look up tax rates for each region and enrich the messages
with tax-related calculations.

### Data Contract

The data contract defines the rules that each sales message must follow before it can be considered
valid.

Required sales fields include key fields such as:

- `order_id`
- `datetime`
- `region_id`
- `currency_code`
- `product_id`
- `unit_price`
- `quantity`
- `is_online`
- `customer_id`
- `is_new_customer`
- `device_type`
- `payment_method`
- `referral_source`

Optional fields may include fields such as:

- `discount_code`
- `customer_note`

The producer validates sales records before sending them to Kafka. A valid message must include the
required fields and must also use values that match the available reference data. For example, the
`region_id` must exist in the region reference data, and the `product_id` must exist in the product
reference data.

A message is valid if it has the required fields and its key lookup values match the allowed
reference data. A message is invalid if it is missing required fields, has invalid values, or
references a region or product that does not exist in the reference tables.

Invalid sales records are not sent to Kafka as valid messages. Instead, they are written to:

`data/output/producer_rejected_sales.csv`

This helps protect the Kafka topic from receiving bad data.

### Kafka Messages

The producer sends validated sales records to a Kafka topic one message at a time. Each Kafka
message represents one online sales transaction from `data/sales.csv`.

The Kafka topic used for my custom project is:

`streaming-03-analytics-femi`

If my `.env` file uses a different topic name, then the producer and consumer use the topic listed
in the `KAFKA_TOPIC` environment variable.

The producer uses `region_id` as the Kafka message key. This means messages are keyed by sales
region, such as:

- `US-TX`
- `US-CA`
- `US-MO`
- `CA-QC`
- `CA-ON`

Using `region_id` as the message key is useful because it connects each sales event to a geographic
region. This can help keep sales from the same region organized together in the stream.

The producer does not change the original sales fields before sending valid messages to Kafka.
Instead, the main message changes happen later in the consumer, where the message is validated
again, enriched, and analyzed.

### Consumer Validation

The consumer receives sales messages from Kafka and validates each message before using it for analytics.

The consumer checks each message against the required sales fields. If a message is missing required
fields, the consumer logs a validation warning and skips that message.

When a message is accepted, the consumer enriches it with additional calculated fields, writes it to
the output CSV file, and updates running statistics.

When a message is rejected or skipped, the consumer does not include it in the final analytics output.
It logs the rejected order and increases the skipped count.

Validation helps protect the results because it prevents missing or incomplete records from
affecting the calculations. For example, if a message is missing `quantity`, `unit_price`, or
`region_id`, then calculations such as subtotal, tax amount, total, and regional sales summaries could
be wrong or fail. By validating first, the consumer makes sure only usable messages affect the analytics.

### Data Engineering and Enrichment

The consumer performs several data engineering steps after receiving messages from Kafka.

First, it validates each message. Then, it enriches the message with derived fields. The consumer uses
the `region_id` from each sales message and looks up the related tax rate from `data/regions.csv`.

The consumer creates derived fields such as:

- `subtotal`
- `tax_amount`
- `total`

The `subtotal` is calculated from `quantity` and `unit_price`.

The `tax_amount` is calculated using the tax rate from the region reference data.

The `total` is calculated by combining the subtotal and tax amount.

I also added custom fields to support a new analytics problem: determining which regions are generating
the most sales in real time and monitoring tax impact as orders arrive.

The custom fields I added include:

- `tax_alert`
- `region_running_sales`
- `region_order_count`
- `top_region_so_far`
- `top_region_sales_so_far`

The `tax_alert` field identifies whether an order has a higher tax amount. If the tax amount is greater
than or equal to the high-tax threshold, the message is labeled `high_tax`. Otherwise, it is labeled
`normal_tax`.

The `region_running_sales` field shows the total sales amount for that message’s region so far.

The `region_order_count` field shows how many accepted orders have been processed for that region.

The `top_region_so_far` field shows which region is currently generating the most sales.

The `top_region_sales_so_far` field shows the current sales total for the leading region.

These changes make the streaming data more useful because the consumer is not only saving messages.
It is also turning raw sales events into business insights as the data arrives.

### Streaming Analytics

The consumer creates running summaries as messages arrive.

The running analytics include:

- total sales
- average sale
- minimum sale
- maximum sale

Each accepted message updates the running statistics. As more messages are consumed, the total sales
amount increases, the average sale adjusts, and the minimum and maximum values update based on the
sales totals that have been processed.

I also added regional running analytics. The consumer tracks sales totals by region while messages
are being consumed. This allows the stream to show which region is currently generating the most sales
without waiting until the end of the day.

The running regional values include:

- running sales total by region
- order count by region
- top sales region so far
- top region sales amount so far

This is useful because a business could monitor regional performance in real time.

### Experiments

For my Phase 4 technical change, I customized the Kafka project by creating my own files:

- `kafka_admin_femi.py`
- `kafka_producer_femi.py`
- `kafka_consumer_femi.py`

I also added producer-side validation. The producer now validates sales records before sending them
to Kafka. If a message does not meet the data contract, it is written to a rejected sales CSV file
instead of being sent as a valid Kafka message.

For my Phase 5 application, I applied the streaming analytics skills to a new business problem:
identifying which regions are generating the most sales in real time and monitoring tax impact as
each order arrives.

I modified the consumer so it adds tax alerts and regional running sales insights. This allows the
consumer to flag higher-tax orders and track the leading sales region while the stream is running.

### Results

When I ran the Kafka admin file, it verified that Kafka was reachable and checked that the Kafka
topic existed. If the topic did not exist, the admin file created it.

When I ran the producer, it read records from `data/sales.csv`, validated the records using the data
contract and reference data, and sent valid records to the Kafka topic. Invalid records were written
to the rejected sales output file.

When I ran the consumer, it consumed messages from the Kafka topic, validated each message, enriched
accepted messages, calculated derived fields, updated running statistics, and wrote the processed
records to:

`data/output/consumed_sales.csv`

The output CSV file included the original sales message fields, Kafka metadata fields, derived fields,
and my custom analytics fields.

The logs showed the full streaming process, including:

- Kafka connection checks
- topic verification
- message consumption
- validation results
- accepted messages
- rejected or skipped messages
- subtotal, tax amount, and total calculations
- running sales statistics
- regional sales summaries

The consumer logs also showed the final summary, including how many messages were consumed, how many
were skipped, and the total, average, minimum, and maximum sales values.

### Interpretation

This validation and analytics workflow showed me how a Kafka streaming pipeline can do more than
simply move messages from one place to another.

The original example focused on producing and consuming Kafka messages. My custom version adds
validation, rejected record handling, enrichment, derived fields, tax alerts, and regional running analytics.

I learned that validating streaming messages is important because bad data can affect calculations
very quickly. In a streaming system, messages are processed as they arrive, so validation protects
the quality of the results.

I also learned that enrichment makes raw messages more useful. A raw sales message may only include
a `region_id`, `quantity`, and `unit_price`, but the consumer can use reference data to add tax
calculations and create more meaningful business fields.

The running summaries could tell a business how sales are changing while the stream is active.
Instead of waiting for a batch report at the end of the day, the business could see total sales,
average sale amount, minimum sale, maximum sale, and top sales region as messages arrive.

The business intelligence gained from the validated and processed messages includes:

- which sales orders were accepted
- which messages were rejected or skipped
- how much each order was worth before and after tax
- which orders had higher tax impact
- which regions were generating the most sales
- how total sales changed as each message arrived
- what the average, minimum, and maximum sales values were during the stream

Overall, this project helped me understand how data engineering and streaming analytics work together.
Kafka moves the messages, but validation, enrichment, and running analytics turn those messages into
reliable and useful business information.
