import urllib.request
import json
import dotenv
import os

dotenv.load_dotenv()

slack_channel = os.getenv('slack_channel')
slack_webhook_url = os.getenv('slack_webhook_url')

log_file = "vat_abcd_crawler.log"
log_file_content = open(log_file, 'r').readlines()

reduced_content = []
info_count = 0
for line in log_file_content:
    if "[INFO]" in line:
        info_count += 1
    else:
        reduced_content.append(line)

summary = "[SUMMARY] Log contains {} lines and {}x [INFO].".format(
    len(log_file_content),
    info_count,
)

body = {
    'channel': '#{}'.format(slack_channel),
    'username': 'VAT Notifications',
    'icon_emoji': ':volcano:',
    'attachments': [
        {
            'fallback': 'VAT ABCD Importer',
            'color': 'good',
            'title': 'VAT ABCD Importer',
            'type': 'plain_text',
            'verbatim': True,
            'text': summary,
        },
{
            'color': 'warn',
            'type': 'plain_text',
            'verbatim': True,
            'text': "\n".join(reduced_content),
        },
    ],
}

print(log_file_content)
print(body)

json_data = json.dumps(body)
json_data_bytes = json_data.encode('utf-8')

request = urllib.request.Request(slack_webhook_url)
request.add_header('Content-Type', 'application/json; charset=utf-8')
request.add_header('Content-Length', len(json_data_bytes))

response = urllib.request.urlopen(request, json_data_bytes)

print(response.getcode())

