import time

from datetime import datetime
from common.asn import get_ip_asn
from common.geoip import get_ip_country
from db.clickhouse import get_client, init_table, get_targets, insert_metadata


def create_metadata(client, start_date=None):
    if not start_date:
        # Try to read the start date from a file
        try:
            f = open('meta', 'r')
            start_date = f.read().strip()
            start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f")
            f.close()
            print(f"Found start time: {str(start_date)}")
        except:
            print("No starting date found. Doing a full database metadata lookup.")
    
    # Save the current time (UTC)
    now = datetime.now()
    f = open('meta', 'w')
    f.write(str(now))
    f.close()

    # Get a list of IPs
    ips = []
    ips = get_targets(client, start_date)

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