# purple_jo

API and backend of Purple Pee an open source ASN lookup project. Made for the public.


Example output of latest ASN entries

```json
[{
	"ip": "52.20.80.64",
	"host": "ec2-52-20-80-64.compute-1.amazonaws.com",
	"as": [{
		"prefix": "52.20.0.0/14",
		"name": "AMAZON-AES - Amazon.com, Inc., US",
		"asn": 14618,
		"created": "2019-11-30T00:14:08.527Z"
	}]
}, {
	"ip": "69.65.13.216",
	"host": "ip-69.65.13.216.servernap.net",
	"as": [{
		"prefix": "69.65.0.0/18",
		"name": "ASN-GIGENET - GigeNET, US",
		"asn": 32181,
		"created": "2019-11-30T00:29:03.172Z"
	}]
}, {
	"ip": "199.47.217.179",
	"host": null,
	"as": [{
		"prefix": "199.47.217.0/24",
		"name": "DROPBOX - Dropbox, Inc., US",
		"asn": 19679,
		"created": "2019-11-30T00:29:04.087Z"
	}]
}]
```

## Build Setup

```bash
# install build dependencies
sudo apt install virtualenv python3 python3-dev gcc

# create a virtualenv
virtualenv -p /usr/bin/python3.6 venv

# activate virtualenv
. venv/bin/activate

# install dependencies
$ pip3 install -r requirements

# serve at 127.0.0.1:5000
$ gunicorn --bind 127.0.0.1:5000 wsgi:app --access-logfile - --error-logfile - --log-level info
```


## Systemd Setup

Create a file `/etc/systemd/system/purplejo.service` with following content

```bash
[Unit]
Description=Gunicorn instance to serve purplejo
After=network.target

[Service]
User=<user>
Group=www-data
WorkingDirectory=/home/<user>/git/purple_jo
Environment="PATH=/home/<user>/git/purple_jo/venv/bin"
ExecStart=/home/<user>/git/purple_jo/venv/bin/gunicorn --workers 4 --bind 127.0.0.1:9000 wsgi:app --access-logfile /var/log/purplejo/access.log --error-logfile /var/log/purplejo/error.log --log-level info

[Install]
WantedBy=multi-user.target
```


Start the service and enable the service

```bash
sudo systemctl start purplepee
sudo systemctl enable purplepee
```
