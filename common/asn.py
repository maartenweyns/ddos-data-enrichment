import maxminddb


def get_ip_asn(ip: str) -> dict:
    with maxminddb.open_database('data/GeoLite2-ASN.mmdb') as reader:
        data = reader.get(ip)
        if data:
            return {
                'num': data['autonomous_system_number'],
                'org': data['autonomous_system_organization']
            }
        else:
            return None


if __name__ == "__main__":
    print(get_ip_asn('176.65.134.15'))
