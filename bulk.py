import argparse
import datetime
import json
import random
import requests
import string
import sys
import time

# Generate a random string of given length
def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Field configuration parser
def parse_field_spec(spec):
    """Parse a field spec string: e.g. user_id:int:1000:2000 or action:string:8 or duration:float:1:10"""
    parts = spec.split(':')
    field = parts[0]
    ftype = parts[1]
    opts = parts[2:]
    config = {'type': ftype}
    if ftype == 'int':
        config['min'] = int(opts[0])
        config['max'] = int(opts[1])
    elif ftype == 'string':
        config['length'] = int(opts[0])
    elif ftype == 'float':
        config['min'] = float(opts[0])
        config['max'] = float(opts[1])
    elif ftype == 'const':
        config['value'] = opts[0]
    return (field, config)

def generate_field(config):
    if config['type'] == 'int':
        return random.randint(config['min'], config['max'])
    elif config['type'] == 'float':
        return round(random.uniform(config['min'], config['max']), 2)
    elif config['type'] == 'string':
        return random_string(config['length'])
    elif config['type'] == 'const':
        return config['value']
    else:
        raise ValueError("Unknown type: " + config['type'])

def main():
    parser = argparse.ArgumentParser(description="Log Entry Generator for ES Bulk API")
    parser.add_argument('--fields', nargs='+', required=True,
                        help="Fields specs: field:type:min:max, field:type:length, field:type:value. E.g. user_id:int:9000:10000 action:string:8 duration:float:0.1:100 const_field:const:foobar")
    parser.add_argument('--bulk_url', type=str, required=True, help="Bulk API URL")
    parser.add_argument('--bulk_size', type=int, required=True, help="Bulk size (number of logs per request)")
    parser.add_argument('--duration', type=int, required=True, help="Duration in seconds")
    parser.add_argument('--bins', type=int, default=0,
                        help="Distribute timestamps equally across N one-minute bins starting from now. "
                             "If not set, timestamps increment by 100ms per document.")

    args = parser.parse_args()

    field_confs = dict(parse_field_spec(f) for f in args.fields)
    bulk_url = args.bulk_url

    start_time = time.time()
    now = datetime.datetime.now(datetime.timezone.utc)
    bins = [now + datetime.timedelta(minutes=i) for i in range(args.bins)] if args.bins > 0 else None
    doc_time = now
    sent = 0
    doc_index = 0

    while time.time() - start_time < args.duration:
        bulk_lines = []
        for _ in range(args.bulk_size):
            if bins:
                ts = bins[doc_index % len(bins)]
            else:
                ts = doc_time
                doc_time += datetime.timedelta(milliseconds=100)
            bulk_lines.append(json.dumps({"index": {"_index": "logs"}}))
            log_entry = {"timestamp": ts.isoformat()}
            for field, conf in field_confs.items():
                log_entry[field] = generate_field(conf)
            bulk_lines.append(json.dumps(log_entry))
            doc_index += 1
        bulk_body = "\n".join(bulk_lines) + "\n"
        resp = requests.post(bulk_url, data=bulk_body, headers={'Content-Type': 'application/x-ndjson'})
        if resp.status_code >= 300:
            print(f"Failed to send logs! Status: {resp.status_code}, Response: {resp.text}", file=sys.stderr)
        sent += args.bulk_size
    print(f"Sent {sent} logs.")

if __name__ == "__main__":
    main()
