#!/usr/bin/env python3
"""
setup_kafka_acls.py
===================
Step 1 — Connect via IAM (superuser) and create ACLs for the SCRAM user.
Step 2 — Verify the ACLs were created.
Step 3 — Produce & consume a test message using SCRAM credentials.

Requirements:
  pip3 install aws-msk-iam-sasl-signer-python kafka-python

AWS credentials must be configured locally (aws configure / env vars).
The IAM identity must have kafka:* permissions on the cluster.

Usage:
  python3 scripts/setup_kafka_acls.py
"""

import sys
import time
import socket

# ── Config ────────────────────────────────────────────────────────────────────
AWS_REGION   = "us-east-2"

# Public IAM endpoint (port 9198)
IAM_BROKERS  = (
    "b-1-public.daisukebdt.v6xpqb.c4.kafka.us-east-2.amazonaws.com:9198,"
    "b-2-public.daisukebdt.v6xpqb.c4.kafka.us-east-2.amazonaws.com:9198"
)

# Public SCRAM endpoint (port 9196)
SCRAM_BROKERS = (
    "b-1-public.daisukebdt.v6xpqb.c4.kafka.us-east-2.amazonaws.com:9196,"
    "b-2-public.daisukebdt.v6xpqb.c4.kafka.us-east-2.amazonaws.com:9196"
)

SCRAM_USERNAME = "msk_iot_online_key"
SCRAM_PASSWORD = "msk_iot_online_secret"

# Topic to create ACLs for (and test)
TEST_TOPIC  = "BDT_Test_Topic"
# All Kafka consumer groups the SCRAM user may use (wildcard)
GROUP_PATTERN = "*"

# ── Colours ───────────────────────────────────────────────────────────────────
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
NC     = "\033[0m"

def ok(msg):    print(f"{GREEN}[PASS]{NC} {msg}")
def err(msg):   print(f"{RED}[FAIL]{NC} {msg}")
def info(msg):  print(f"{YELLOW}[INFO]{NC} {msg}")
def step(msg):  print(f"\n{CYAN}{'─'*60}\n  {msg}\n{'─'*60}{NC}")

# ── Imports ───────────────────────────────────────────────────────────────────
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    err("kafka-python not installed.  Run: pip3 install kafka-python")
    sys.exit(1)

try:
    from confluent_kafka import Producer as CProducer
    from confluent_kafka.admin import (
        AdminClient, AclBinding, AclBindingFilter,
        ResourceType, ResourcePatternType, AclOperation, AclPermissionType,
    )
except ImportError:
    err("confluent-kafka not installed.  Run: pip3 install confluent-kafka")
    sys.exit(1)

try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
except ImportError:
    err("aws-msk-iam-sasl-signer-python not installed.  Run: pip3 install aws-msk-iam-sasl-signer-python")
    sys.exit(1)

# ── IAM OAuth callback for confluent-kafka ─────────────────────────────────────
def msk_oauth_cb(config):
    """confluent-kafka calls this when it needs a fresh OAUTHBEARER token."""
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return token, expiry_ms / 1000  # confluent expects expiry in seconds

IAM_CONF = {
    "bootstrap.servers": IAM_BROKERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": msk_oauth_cb,
}

SCRAM_OPTS = dict(
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=SCRAM_USERNAME,
    sasl_plain_password=SCRAM_PASSWORD,
)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — TCP reachability for both ports
# ─────────────────────────────────────────────────────────────────────────────
step("STEP 1: TCP reachability")
for label, brokers in [("IAM (9198)", IAM_BROKERS), ("SCRAM (9196)", SCRAM_BROKERS)]:
    for broker in brokers.split(","):
        host, port = broker.strip().rsplit(":", 1)
        try:
            s = socket.create_connection((host, int(port)), timeout=6)
            s.close()
            ok(f"[{label}]  {host}:{port}")
        except Exception as e:
            err(f"[{label}]  {host}:{port}  →  {e}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Create ACLs via IAM
# ─────────────────────────────────────────────────────────────────────────────
step("STEP 2: Create Kafka ACLs via IAM auth")

SCRAM_PRINCIPAL = f"User:{SCRAM_USERNAME}"

def make_acl(res_type, res_name, pat_type, operation):
    return AclBinding(
        restype=res_type,
        name=res_name,
        resource_pattern_type=pat_type,
        principal=SCRAM_PRINCIPAL,
        host="*",
        operation=operation,
        permission_type=AclPermissionType.ALLOW,
    )

acls_to_create = [
    make_acl(ResourceType.TOPIC, TEST_TOPIC,      ResourcePatternType.LITERAL, AclOperation.WRITE),
    make_acl(ResourceType.TOPIC, TEST_TOPIC,      ResourcePatternType.LITERAL, AclOperation.READ),
    make_acl(ResourceType.TOPIC, TEST_TOPIC,      ResourcePatternType.LITERAL, AclOperation.DESCRIBE),
    make_acl(ResourceType.TOPIC, TEST_TOPIC,      ResourcePatternType.LITERAL, AclOperation.CREATE),
    make_acl(ResourceType.GROUP, GROUP_PATTERN,   ResourcePatternType.LITERAL, AclOperation.READ),
]

try:
    info(f"Connecting to IAM brokers: {IAM_BROKERS}")
    admin = AdminClient(IAM_CONF)
    futures = admin.create_acls(acls_to_create)
    all_ok = True
    for acl, f in futures.items():
        try:
            f.result()  # raises on error
            info(f"  ALLOW  {acl.restype.name}:{acl.name}  →  {acl.operation.name}")
        except Exception as e:
            err(f"  ACL failed ({acl.restype.name}:{acl.name} {acl.operation.name}): {e}")
            all_ok = False
    if all_ok:
        ok(f"All {len(acls_to_create)} ACLs created for {SCRAM_PRINCIPAL}")
except Exception as e:
    err(f"IAM admin connection failed: {e}")
    info("Make sure your local AWS credentials have kafka:* permissions on this cluster.")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2b — Create the topic via IAM (auto.create.topics.enable=false on this cluster)
# ─────────────────────────────────────────────────────────────────────────────
step("STEP 2b: Create topic via IAM")
try:
    from confluent_kafka.admin import NewTopic
    admin = AdminClient(IAM_CONF)
    futures = admin.create_topics([NewTopic(TEST_TOPIC, num_partitions=1, replication_factor=2)])
    for topic, f in futures.items():
        try:
            f.result()
            ok(f"Topic '{topic}' created (1 partition, RF=2)")
        except Exception as e:
            # TopicExistsException is fine
            if "TopicExistsException" in str(type(e)) or "already exists" in str(e).lower() or "TOPIC_ALREADY_EXISTS" in str(e):
                info(f"Topic '{topic}' already exists — OK")
            else:
                err(f"Topic creation failed: {e}")
except Exception as e:
    err(f"Topic creation error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Verify ACLs were stored (describe via IAM)
# ─────────────────────────────────────────────────────────────────────────────
step("STEP 3: Verify ACLs via IAM describe")
try:
    admin = AdminClient(IAM_CONF)
    acl_filter = AclBindingFilter(
        restype=ResourceType.ANY,
        name=None,
        resource_pattern_type=ResourcePatternType.ANY,
        principal=SCRAM_PRINCIPAL,
        host=None,
        operation=AclOperation.ANY,
        permission_type=AclPermissionType.ANY,
    )
    f = admin.describe_acls(acl_filter)
    acls = f.result()
    if acls:
        ok(f"Found {len(acls)} ACL(s) for {SCRAM_PRINCIPAL}:")
        for a in acls:
            info(f"  {a.permission_type.name}  {a.restype.name}:{a.name}  →  {a.operation.name}")
    else:
        info("No ACLs found yet (may take a few seconds to propagate).")
except Exception as e:
    err(f"Describe ACLs failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Produce a message via SCRAM
# ─────────────────────────────────────────────────────────────────────────────
step("STEP 4: Produce test message via SCRAM")
TEST_MSG = f"cs523-bdt-iam-acl-test-{int(time.time())}"
info(f"Topic:   {TEST_TOPIC}")
info(f"Message: {TEST_MSG}")

try:
    producer = KafkaProducer(
        bootstrap_servers=SCRAM_BROKERS,
        **SCRAM_OPTS,
        value_serializer=lambda v: v.encode("utf-8"),
        acks=1,
        retries=0,
        request_timeout_ms=15_000,
        max_block_ms=20_000,
    )
    meta = producer.send(TEST_TOPIC, value=TEST_MSG).get(timeout=15)
    producer.flush()
    producer.close()
    ok(f"Produced → partition {meta.partition}, offset {meta.offset}")
except Exception as e:
    err(f"Produce failed: {e}")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Consume the message back via SCRAM
# ─────────────────────────────────────────────────────────────────────────────
step("STEP 5: Consume message via SCRAM (timeout: 12s)")
try:
    consumer = KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=SCRAM_BROKERS,
        **SCRAM_OPTS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=12_000,
        value_deserializer=lambda v: v.decode("utf-8"),
        group_id=f"cs523-iam-acl-verify-{int(time.time())}",
    )
    found = False
    for msg in consumer:
        if TEST_MSG in msg.value:
            ok(f"Consumed: '{msg.value}' (partition={msg.partition}, offset={msg.offset})")
            found = True
            break
    consumer.close()
    if not found:
        info("Message not found in window (may already have been consumed earlier).")
except KafkaError as e:
    err(f"Consume failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("  Done! If Steps 4 & 5 passed, SCRAM access is fully working.")
print(f"  Use these settings in GitHubKafkaProducer.java:")
print(f"    bootstrap.servers = {SCRAM_BROKERS}")
print(f"    security.protocol = SASL_SSL")
print(f"    sasl.mechanism    = SCRAM-SHA-512")
print(f"    topic             = github-events")
print(f"{'='*60}\n")
