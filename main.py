import time

from datetime import datetime
from common.asn import get_ip_asn
from common.geoip import get_ip_country
from db.clickhouse import get_client, init_table, get_targets, insert_metadata


def create_metadata(client):
    # Get a list of IPs
    ips = []
    ips = get_targets(client)
    ips = list(set(ips))
    print(f"Need to look up {len(ips)} ips...")
    
    # Construct the data for every IP
    data = []
    for ip in ips:
        ip = ip[0]
        country = get_ip_country(ip)
        asn = get_ip_asn(ip)
        data.append([
            ip,
            country if country else "",
            asn['num'] if asn else 0,
            asn['org'] if asn else ""
        ])

    insert_metadata(client, data)
    print(f"Inserted {len(data)} entries.")


# Create ip_metadata in the clickhouse db
def main():
    client = get_client()
    init_table(client)
    create_metadata(client)


if __name__ == "__main__":
    while True:
        main()
        print("Sleeping now...")
        time.sleep(300)
