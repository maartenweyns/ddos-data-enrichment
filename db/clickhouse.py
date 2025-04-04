import clickhouse_connect
import os

from dotenv import load_dotenv


# Get a clickhouse client
def get_client():
    load_dotenv()

    us = os.getenv('CLICKHOUSE_USER')
    pw = os.getenv('CLICKHOUSE_PASSWORD')
    
    client = clickhouse_connect.get_client(
        host='localhost',
        username=us,
        password=pw,
        database='gorilla_neo'
    )
    
    return client


def init_table(client):
    query = """CREATE TABLE IF NOT EXISTS ip_metadata (
    ip IPv4,
    isocountry Nullable(FixedString(2)),
    asn Nullable(UInt32),
    org Nullable(String)
)
PRIMARY KEY ip
ORDER BY ip
COMMENT 'Metadata for IP targets'"""

    client.command(query)


def get_targets(client, start_date):
    if start_date:
       query = f"""SELECT DISTINCT target.1 AS ip
FROM
(
    SELECT arrayJoin(targets) AS target
    FROM commands
    WHERE timestamp > parseDateTimeBestEffortOrNull('2025-04-04 11:50:17.222346')
)"""
    else:
        query = """SELECT DISTINCT target.1 AS ip
FROM (
    SELECT arrayJoin(targets) AS target
    FROM commands
);"""
    
    return client.query(query).result_rows


def insert_metadata(client, data):
    columns = [
        'ip',
        'isocountry',
        'asn',
        'org'
    ]
    client.insert(
        "ip_metadata",
        data,
        column_names=columns
    )
