import maxminddb


# Get the ISO-code of the country corresponding with a given IP address
def get_ip_country(ip: str) -> str:
    with maxminddb.open_database('data/GeoLite2-Country.mmdb') as reader:
        data = reader.get(ip)
        if data:
            res = data.get('iso_code', None)
            if res: return res
            res = data.get('country', None)
            if res: res = res['iso_code']
            if res: return res
        return None 


if __name__ == "__main__":
    print(get_ip_country('152.216.7.110'))
