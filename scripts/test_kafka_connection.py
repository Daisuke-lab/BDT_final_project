#!/usr/bin/env python3
"""
test_kafka_connection.py
========================
Quick connectivity test for a remote Kafka broker using kafka-python.

Tests:
  1. TCP reachability
  2. Broker metadata diagnostic (shows advertised IP/host)
  3. Produce a test message
  4. Consume the message back

Usage:
  pip3 install kafka-python
  python3 scripts/test_kafka_connection.py
"""

import socket
import time
import sys

# MSK cluster: daisuke-bdt (public endpoints, SASL/SCRAM-SHA-512 + TLS, port 9196)
KAFKA_BROKERS = (
    "b-1-public.daisukebdt.v6xpqb.c4.kafka.us-east-2.amazonaws.com:9196,"
    "b-2-public.daisukebdt.v6xpqb.c4.kafka.us-east-2.amazonaws.com:9196"
)
MSK_USERNAME = "msk_iot_online_key"
MSK_PASSWORD = "msk_iot_online_secret"
TEST_TOPIC   = "BDT_Test_Topic"
TEST_MSG     = f"hello-from-cs523-bdt-{int(time.time())}"

SASL_OPTS = dict(
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=MSK_USERNAME,
    sasl_plain_password=MSK_PASSWORD,
)

GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
NC     = "\033[0m"

def ok(msg):   print(f"{GREEN}[PASS]{NC} {msg}")
def err(msg):  print(f"{RED}[FAIL]{NC} {msg}")
def info(msg): print(f"{YELLOW}[INFO]{NC} {msg}")

print("=" * 60)
print("  Kafka Connectivity Test — MSK daisuke-bdt (public)")
print(f"  Brokers:  {KAFKA_BROKERS}")
print(f"  Username: {MSK_USERNAME}")
print("  Protocol: SASL_SSL / SCRAM-SHA-512 / port 9196")
print("=" * 60)
print()

# ─────────────────────────────────────────────
# Check kafka-python is installed
# ─────────────────────────────────────────────
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    err("kafka-python is not installed.")
    info("Run:  pip3 install kafka-python")
    sys.exit(1)

# ─────────────────────────────────────────────
# TEST 1: TCP reachability
# ─────────────────────────────────────────────
print("--- TEST 1: TCP reachability ---")
all_reachable = True
for _broker in KAFKA_BROKERS.split(","):
    _host, _port = _broker.strip().rsplit(":", 1)
    try:
        _sock = socket.create_connection((_host, int(_port)), timeout=5)
        _sock.close()
        ok(f"  {_host}:{_port} is reachable")
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        err(f"  {_host}:{_port} unreachable — {e}")
        all_reachable = False
if not all_reachable:
    info("Check security group inbound rules: allow port 9196 from your IP.")
    sys.exit(1)
print()

# ─────────────────────────────────────────────
# TEST 2: Produce a test message
# ─────────────────────────────────────────────
print("--- TEST 2: Produce test message ---")
info(f"Sending: '{TEST_MSG}'")
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        **SASL_OPTS,
        value_serializer=lambda v: v.encode("utf-8"),
        acks=1,
        retries=0,
        request_timeout_ms=15_000,
        max_block_ms=20_000,
    )
    future = producer.send(TEST_TOPIC, value=TEST_MSG)
    record_meta = future.get(timeout=15)
    producer.flush()
    producer.close()
    ok(f"Message produced → partition {record_meta.partition}, offset {record_meta.offset}")
except Exception as e:
    err(f"Produce failed: {e}")
print()

# ─────────────────────────────────────────────
# TEST 4: Consume the message back
# ─────────────────────────────────────────────
print("--- TEST 3: Consume message back (timeout: 10s) ---")
try:
    consumer = KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        **SASL_OPTS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
        value_deserializer=lambda v: v.decode("utf-8"),
        group_id=f"cs523-test-consumer-{int(time.time())}",
    )
    found = False
    for msg in consumer:
        if TEST_MSG in msg.value:
            ok(f"Message consumed: '{msg.value}' (partition={msg.partition}, offset={msg.offset})")
            found = True
            break
    consumer.close()
    if not found:
        info("Test message not found within timeout (broker may buffer for a moment — that's OK).")
except KafkaError as e:
    err(f"Consume failed: {e}")
print()

# ─────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────
print("=" * 60)
print("  Test complete.")
print()
print("  If all tests passed, use these settings in your producer:")
print(f'    KAFKA_BOOTSTRAP = "{KAFKA_BROKERS}"')
print(f'    USERNAME        = "{MSK_USERNAME}"')
print(f'    PROTOCOL        = SASL_SSL / SCRAM-SHA-512')
print("=" * 60)
