# NetScanner - Network Intelligence API

A comprehensive network intelligence and domain analysis API built with **FastAPI** and **PostgreSQL**. NetScanner provides ASN lookups, domain analysis, live scanning capabilities, and network relationship mapping through a high-performance async API.

## 🚀 Features

- **Real-time Domain Scanning**: Live scan domains with WebSocket streaming progress
- **ASN Intelligence**: Comprehensive ASN lookup and country-based filtering
- **Network Analysis**: IPv4/IPv6 analysis, CIDR mapping, and subnet exploration
- **DNS Intelligence**: DNS record analysis and domain relationships
- **Graph Analysis**: Network relationship mapping and visualization data
- **Trend Analytics**: Request patterns and usage statistics
- **High Performance**: Async FastAPI with PostgreSQL backend and connection pooling
- **Interactive Documentation**: Auto-generated OpenAPI/Swagger docs
- **Modern Migration**: Fully migrated from MongoDB to PostgreSQL for better performance

## 📋 Prerequisites

- **Python 3.10+**
- **PostgreSQL 14+**
- **Redis** (for caching)
- **Chromium/Chrome** (for web scraping features)

## 🛠️ Installation

### Database Setup (PostgreSQL)

```bash
# Install PostgreSQL (Ubuntu example)
sudo apt update
sudo apt install postgresql postgresql-contrib

# Create database and user
sudo -u postgres psql -c "CREATE DATABASE netscanner;"
sudo -u postgres psql -c "CREATE USER netscanner WITH PASSWORD 'change-me';"
sudo -u postgres psql -c "GRANT CONNECT ON DATABASE netscanner TO netscanner;"
sudo -u postgres psql -c "GRANT USAGE ON SCHEMA public TO netscanner;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE netscanner TO netscanner;"
```

Set the DSN in your `.env` (or shell):

```
POSTGRES_DSN=postgresql+asyncpg://netscanner:change-me@127.0.0.1/netscanner
```

Apply migrations to prepare the schema:

```bash
alembic upgrade head
```

### Application Setup

```bash
# Install system dependencies
sudo apt install redis-server chromium-chromedriver python3 python3-dev gcc

# Clone and setup the project
git clone https://github.com/p3t3r67x0/purple_jo.git
cd purple_jo

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Configure environment (copy and edit as needed)
cp config.cfg.example config.cfg
# Or create .env file with your configuration

# Download GeoLite2-City.mmdb from MaxMind (free account required)
# Create data folder and save GeoLite2-City.mmdb there: data/GeoLite2-City.mmdb
mkdir -p data
# Visit https://www.maxmind.com/en/geolite2/signup to download the database
```

## 🚦 Running the API

### Development Server

```bash
# With auto-reload for development
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### Production Server

```bash
# Production deployment
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4 --log-level info
```

### 📚 API Documentation

Once running, interactive documentation is available at:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## 📖 API Endpoints

### Core Endpoints

| Endpoint                  | Method | Description                        | Parameters                                      |
| ------------------------- | ------ | ---------------------------------- | ----------------------------------------------- |
| `/query/{domain}`         | GET    | Query domain information           | `domain`, `page`, `page_size`                   |
| `/dns`                    | GET    | Latest DNS records                 | `page`, `page_size`                             |
| `/ip/{ipv4}`              | GET    | IPv4 address lookup                | `ipv4`, `page`, `page_size`                     |
| `/asn`                    | GET    | ASN information                    | `page`, `page_size`, `country_code`             |
| `/match/{query}`          | GET    | Advanced search with conditions    | `query`, `page`, `page_size`                    |
| `/graph/{site}`           | GET    | Network relationship graph         | `site`, `page`, `page_size`                     |
| `/contact`                | POST   | Submit contact form (email)        | `name`, `email`, `subject`, `message`, `token?` |
| `/admin/contact/messages` | GET    | (Admin) Recent contact submissions | `limit`, `since_minutes` (auth required)        |

### Real-time Features

| Endpoint              | Method    | Description                        | Parameters |
| --------------------- | --------- | ---------------------------------- | ---------- |
| `/live/scan/{domain}` | WebSocket | Live domain scanning with progress | `domain`   |

### Analytics
### Contact Submission

Submit a contact message that will be delivered via SMTP to the configured
recipient (default: `hello@netscanner.io`). The endpoint now includes:

- Optional shared-secret token validation (simple fallback mode)
- hCaptcha or reCAPTCHA validation (if secrets configured)
- Per-IP rate limiting (configurable window & count)
- IP denylist & CIDR blocking (`CONTACT_IP_DENYLIST`)
- MongoDB persistence of submissions (`contact_messages` collection)
- Async email sending using `aiosmtplib` with synchronous fallback

Endpoint: `POST /contact`

JSON body:

```json
{
    "name": "Jane Doe",
    "email": "jane@example.com",
    "subject": "Inquiry",
    "message": "Please send more information.",
    "token": "OPTIONAL_SECRET"
}
```

Successful response:

```json
{ "status": "accepted" }
```

Rate limit exceeded response (HTTP 429):

```json
{ "detail": "Rate limit exceeded. Please try again later." }
```

Invalid token response (HTTP 401):

```json
{ "detail": "Invalid token" }
```

Environment variables controlling the contact subsystem:

| Variable              | Default               | Description                                   |
| --------------------- | --------------------- | --------------------------------------------- |
| `SMTP_HOST`           | `localhost`           | SMTP server host                              |
| `SMTP_PORT`           | `25`                  | SMTP server port                              |
| `SMTP_USER`           | (unset)               | Username (if authentication needed)           |
| `SMTP_PASSWORD`       | (unset)               | Password (if authentication needed)           |
| `SMTP_STARTTLS`       | `false`               | Enable STARTTLS (`true/false`)                |
| `CONTACT_TO`          | `hello@netscanner.io` | Destination email address                     |
| `CONTACT_FROM`        | (falls back to user)  | Envelope/Sender address                       |
| `CONTACT_RATE_LIMIT`  | `5`                   | Max submissions per window per IP             |
| `CONTACT_RATE_WINDOW` | `3600`                | Window length in seconds                      |
| `CONTACT_TOKEN`       | (unset)               | Shared secret token (used only if no captcha) |
| `HCAPTCHA_SECRET`     | (unset)               | hCaptcha secret (enables hCaptcha)            |
| `RECAPTCHA_SECRET`    | (unset)               | reCAPTCHA secret (enables reCAPTCHA)          |
| `CONTACT_IP_DENYLIST` | (unset)               | Comma list of IPs / CIDRs to block            |

Admin OAuth flow:

```bash
# 1) Create an admin account (run once)
curl -X POST http://localhost:8000/admin/signup \
    -H 'Content-Type: application/json' \
    -d '{
        "email": "admin@example.com",
        "password": "changeme123",
        "full_name": "Operations"
    }'

# 2) Exchange credentials for an access token
curl -X POST http://localhost:8000/admin/token \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d 'username=admin@example.com&password=changeme123'

# 3) Call protected endpoints with the bearer token
curl -H "Authorization: Bearer <access_token>" \
    'http://localhost:8000/admin/contact/messages?limit=10&since_minutes=60'
```

Example captcha (hCaptcha) variables:

```bash
export HCAPTCHA_SECRET=0x0000000000000000000000000000000000000000
```

Example IP denylist (single and CIDR):

```bash
export CONTACT_IP_DENYLIST="192.0.2.10,203.0.113.0/24,2001:db8::/32"
```


Example `curl` (with token):

```bash
curl -X POST http://localhost:8000/contact \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "Jane Doe",
        "email": "jane@example.com",
        "subject": "Test",
        "message": "Hello from API",
        "token": "supersecret"
    }'
```

Set environment variables before starting the server:

```bash
export CONTACT_TOKEN=supersecret
export CONTACT_RATE_LIMIT=10
export CONTACT_RATE_WINDOW=1800
export CONTACT_TO=hello@netscanner.io
```

If email sending fails, the endpoint returns HTTP 500 with an error message.


| Endpoint           | Method | Description                     | Parameters                                                                     |
| ------------------ | ------ | ------------------------------- | ------------------------------------------------------------------------------ |
| `/trends/requests` | GET    | API usage trends and statistics | `interval`, `lookback_minutes`, `buckets`, `top_paths`, `recent_limit`, `path` |

### Advanced Search Conditions

The `/match/{query}` endpoint supports various search conditions:

- `ipv6:{query}` - IPv6 address search
- `ca:{query}` - Certificate Authority search
- `crl:{query}` - Certificate Revocation List search
- `org:{query}` - Organization search
- `ocsp:{query}` - OCSP responder search
- `before:{date}` - Records before date
- `after:{date}` - Records after date

### Response Format

All endpoints return paginated JSON responses:

```json
{
  "results": [...],
  "pagination": {
    "current_page": 1,
    "page_size": 20,
    "total_pages": 5,
    "total_items": 100
  }
}
```

### WebSocket Live Scanning

Connect to `/live/scan/{domain}` for real-time scanning progress:

```javascript
const ws = new WebSocket('ws://localhost:8000/live/scan/example.com');
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Progress:', data);
};
```

Progress events include:
- `type: "start"` - Scan initiated
- `type: "progress"` - Step progress (dns, headers, ssl, geoip, etc.)
- `type: "result"` - Final scan results
- `type: "error"` - Error occurred

## 🔧 Configuration

Configure via `config.cfg` or environment variables:

```ini
[DEFAULT]
MONGO_HOST = localhost
MONGO_PORT = 27017
REDIS_HOST = localhost
REDIS_PORT = 6379
API_HOST = 0.0.0.0
API_PORT = 8000
LOG_LEVEL = info
```

## 📊 Monitoring & Analytics

### Request Trends

Monitor API usage patterns:

```bash
curl "http://localhost:8000/trends/requests?interval=hour&lookback_minutes=1440&buckets=24"
```

Parameters:
- `interval`: `minute`, `hour`, or `day`
- `lookback_minutes`: Time window (max 7 days)
- `buckets`: Number of time buckets (max 500)
- `top_paths`: Most popular endpoints (max 50)
- `recent_limit`: Recent requests sample (max 100)

## 🐳 Production Deployment

### Systemd Service

Create `/etc/systemd/system/netscanner-api.service`:

```ini
[Unit]
Description=NetScanner API Server
After=network.target mongodb.service redis.service

[Service]
User=www-data
Group=www-data
WorkingDirectory=/opt/git/purple_jo
Environment="PATH=/opt/git/purple_jo/venv/bin"
ExecStart=/opt/git/purple_jo/venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 4 --log-level info --proxy-headers
Restart=on-failure
RestartSec=2s
KillMode=mixed
TimeoutStopSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable netscanner-api
sudo systemctl start netscanner-api
```

### Nginx Configuration

Example Nginx config with CORS support and rate limiting:

```nginx
# ===============================
# Rate limiting zones
# ===============================
limit_req_zone $binary_remote_addr zone=api_search:10m rate=10r/s;
limit_req_zone $binary_remote_addr zone=api_live:10m rate=5r/s;
limit_req_zone $binary_remote_addr zone=api_trends:10m rate=3r/s;
limit_req_zone $binary_remote_addr zone=api_general:10m rate=20r/s;
limit_conn_zone $binary_remote_addr zone=conn_limit_per_ip:10m;


server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name api.netscanner.io;

    # ===========================
    # Security headers
    # ===========================
    add_header X-Frame-Options "DENY" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    # ===========================
    # Global CORS headers
    # ===========================
    add_header 'Access-Control-Allow-Origin' '*' always;
    add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
    add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;

    # ===========================
    # Handle OPTIONS preflight requests
    # ===========================
    location / {
        if ($request_method = OPTIONS) {
            add_header 'Access-Control-Allow-Origin' '*';
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS';
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range';
            add_header 'Access-Control-Max-Age' 1728000;
            add_header 'Content-Type' 'text/plain; charset=utf-8';
            add_header 'Content-Length' 0;
            return 204;
        }

        limit_req zone=api_general burst=20 nodelay;
        limit_req_status 429;

        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # ===========================
    # Search/query endpoints
    # ===========================
    location ~ ^/(match|query|graph) {
        limit_req zone=api_search burst=10 nodelay;
        limit_req_status 429;

        # Handle CORS preflight
        if ($request_method = OPTIONS) {
            add_header 'Access-Control-Allow-Origin' '*' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
            add_header 'Access-Control-Max-Age' 1728000 always;
            add_header 'Content-Type' 'text/plain; charset=utf-8';
            add_header 'Content-Length' 0;
            return 204;
        }

        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # ===========================
    # Live endpoints (WebSocket)
    # ===========================
    location ^~ /live {
        limit_req zone=api_live burst=5 nodelay;
        limit_req_status 429;

        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
        proxy_send_timeout 86400;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # ===========================
    # Trends/analytics endpoints
    # ===========================
    location ^~ /trends {
        limit_req zone=api_trends burst=3 nodelay;
        limit_req_status 429;

        # Handle CORS preflight
        if ($request_method = OPTIONS) {
            add_header 'Access-Control-Allow-Origin' '*' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
            add_header 'Access-Control-Max-Age' 1728000 always;
            add_header 'Content-Type' 'text/plain; charset=utf-8';
            add_header 'Content-Length' 0;
            return 204;
        }

        # Normal proxying
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # ===========================
    # SSL Certificates (Certbot)
    # ===========================
    ssl_certificate /etc/letsencrypt/live/api.netscanner.io/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.netscanner.io/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;
}

# ===============================
# HTTP → HTTPS redirect
# ===============================
server {
    listen 80;
    listen [::]:80;
    server_name api.netscanner.io;

    if ($host = api.netscanner.io) {
        return 301 https://$host$request_uri;
    }

    return 404;
}
```

#### Rate Limiting Configuration Explained

The rate limiting configuration includes several zones with different limits:

- **api_general**: 10 requests/second with burst of 20 (general API endpoints)
- **api_search**: 5 requests/second with burst of 10 (search/query intensive endpoints)
- **api_live**: 2 requests/second with burst of 5 (live scanning endpoints)
- **api_trends**: 1 request/second with burst of 3 (analytics endpoints)
- **conn_limit_per_ip**: Maximum 20 concurrent connections per IP

#### Customizing Rate Limits

You can adjust the rate limits based on your needs:

```nginx
# Examples of different rate limiting configurations
limit_req_zone $binary_remote_addr zone=strict:10m rate=1r/s;     # Very strict
limit_req_zone $binary_remote_addr zone=moderate:10m rate=5r/s;   # Moderate
limit_req_zone $binary_remote_addr zone=relaxed:10m rate=20r/s;   # Relaxed

# For whitelisted IPs (add to server block)
geo $limit {
    default 1;
    10.0.0.0/8 0;      # Internal network
    192.168.0.0/16 0;  # Local network
    127.0.0.1/32 0;    # Localhost
}

map $limit $limit_key {
    0 "";
    1 $binary_remote_addr;
}

limit_req_zone $limit_key zone=api_with_whitelist:10m rate=10r/s;
```

### SSL Setup with Let's Encrypt

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d api.example.com
sudo systemctl enable certbot.timer
```

## 🛠️ Development Tools

The project includes various PostgreSQL-powered tools in the `tools/` directory:

**✅ Fully Migrated Tools:**
- `ssl_cert_scanner.py` - Distributed SSL certificate analysis with PostgreSQL and RabbitMQ
- `extract_certstream.py` - Certificate Transparency log monitor for domain discovery
- `decode_idna.py` - IDNA domain decoder with async PostgreSQL operations
- `insert_asn.py` - ASN data importer with CIDR/ASN mapping

**🔧 Additional Tools:**
- `extract_domains.py` - Domain extraction from URLs
- `import_domains.py` - Bulk domain import
- `banner_grabber.py` - Network banner collection
- `masscan_scanner.py` - Port scanning integration
- `dns_shared.py` - Shared DNS utilities with PostgreSQL backend

All migrated tools feature:
- Modern async/await patterns with SQLAlchemy
- Click-based CLI interfaces
- Independent operation with `.env` configuration
- Comprehensive error handling and logging

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- Frontend: [Purple Pee Frontend](https://github.com/p3t3r67x0/purplepee)
- Documentation: [API Documentation](https://api.example.com/docs)

---

**NetScanner** - Network Intelligence Made Simple 🟣
