[![Build Status](https://travis-ci.org/gfbio/vat-abcd-crawler.svg?branch=master)](https://travis-ci.org/gfbio/vat-abcd-crawler)

# VAT ABCD Crawler

This repository contains the ABCD crawler for the VAT system.
It retrieves archive information from the BMS and parses its ABCD archives one by one.
The results are stored into a PostgreSQL database.

## Settings
Call the program with the path to the settings file (`settings.toml`) as first parameter.
This file contains several parameters regarding BMS url, ABCD field map and database connection. 
The `abcd-fields.json` provides a listing of all GFBio mandatory and recommended fields plus additional metadata.

## Slack
In order to post log files to slack, create a `.env` file with the following content

```
slack_channel = vat_status
slack_webhook_url = https://hooks.slack.com/services/<YOURWEBHOOKKEYHERE>
```
