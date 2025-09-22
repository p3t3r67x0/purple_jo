# NetScanner - Network Intelligence API

A comprehensive network intelligence and domain analysis API built with **FastAPI**. NetScanner provides ASN lookups, domain analysis, live scanning capabilities, and network relationship mapping through a high-performance async API.

## 🚀 Features

- **Real-time Domain Scanning**: Live scan domains with WebSocket streaming progress
- **ASN Intelligence**: Comprehensive ASN lookup and country-based filtering
- **Network Analysis**: IPv4/IPv6 analysis, CIDR mapping, and subnet exploration
- **DNS Intelligence**: DNS record analysis and domain relationships
- **Graph Analysis**: Network relationship mapping and visualization data
- **Trend Analytics**: Request patterns and usage statistics
- **High Performance**: Async FastAPI with MongoDB backend
- **Interactive Documentation**: Auto-generated OpenAPI/Swagger docs

## 📋 Prerequisites

- **Python 3.10+**
- **MongoDB 6.0+**
- **Redis** (for caching)
- **Chromium/Chrome** (for web scraping features)

## 🛠️ Installation

### Database Setup (MongoDB)

```bash
sudo apt-get install gnupg
wget --quiet -O - https://www.mongodb.org/static/pgp/server-6.0.asc | gpg --dearmor > mongodb-keyring.gpg
sudo mv mongodb-keyring.gpg /etc/apt/trusted.gpg.d/
sudo chown root:root /etc/apt/trusted.gpg.d/mongodb-keyring.gpg
sudo chmod ugo+r /etc/apt/trusted.gpg.d/mongodb-keyring.gpg
sudo chmod go-w /etc/apt/trusted.gpg.d/mongodb-keyring.gpg
echo "deb [arch=amd64,arm64 signed-by=/etc/apt/trusted.gpg.d/mongodb-keyring.gpg] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
sudo apt update
sudo apt install mongodb-org

# Start MongoDB
sudo systemctl start mongod
sudo systemctl enable mongod
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

| Endpoint          | Method | Description                     | Parameters                          |
| ----------------- | ------ | ------------------------------- | ----------------------------------- |
| `/query/{domain}` | GET    | Query domain information        | `domain`, `page`, `page_size`       |
| `/dns`            | GET    | Latest DNS records              | `page`, `page_size`                 |
| `/ip/{ipv4}`      | GET    | IPv4 address lookup             | `ipv4`, `page`, `page_size`         |
| `/asn`            | GET    | ASN information                 | `page`, `page_size`, `country_code` |
| `/match/{query}`  | GET    | Advanced search with conditions | `query`, `page`, `page_size`        |
| `/graph/{site}`   | GET    | Network relationship graph      | `site`, `page`, `page_size`         |

### Real-time Features

| Endpoint              | Method    | Description                        | Parameters |
| --------------------- | --------- | ---------------------------------- | ---------- |
| `/live/scan/{domain}` | WebSocket | Live domain scanning with progress | `domain`   |

### Analytics

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
# Rate limiting zones - add this to the http block in nginx.conf
http {
    # Rate limiting zones
    limit_req_zone $binary_remote_addr zone=api_general:10m rate=10r/s;
    limit_req_zone $binary_remote_addr zone=api_search:10m rate=5r/s;
    limit_req_zone $binary_remote_addr zone=api_live:10m rate=2r/s;
    limit_req_zone $binary_remote_addr zone=api_trends:10m rate=1r/s;

    # Connection limiting
    limit_conn_zone $binary_remote_addr zone=conn_limit_per_ip:10m;
}

server {
    listen 80;
    server_name api.example.com;

    # Redirect HTTP to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.example.com;

    # SSL configuration
    ssl_certificate /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;

    # Security headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    # Connection limits
    limit_conn conn_limit_per_ip 20;

    # Rate limiting for specific endpoints
    location ~ ^/(match|query|graph) {
        # Stricter rate limiting for search/query endpoints
        limit_req zone=api_search burst=10 nodelay;
        limit_req_status 429;

        # CORS headers
        if ($request_method = 'OPTIONS') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
            add_header 'Access-Control-Max-Age' 1728000 always;
            add_header 'Content-Type' 'text/plain; charset=utf-8' always;
            add_header 'Content-Length' 0 always;
            return 204;
        }

        add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;

        # Proxy to FastAPI
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Forwarded-Port $server_port;
    }

    location /live/ {
        # Rate limiting for live scan endpoints
        limit_req zone=api_live burst=5 nodelay;
        limit_req_status 429;

        # CORS headers
        add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;

        # Proxy to FastAPI with WebSocket support
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Forwarded-Port $server_port;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
        proxy_send_timeout 86400;
    }

    location /trends/ {
        # More restrictive rate limiting for analytics endpoints
        limit_req zone=api_trends burst=3 nodelay;
        limit_req_status 429;

        # CORS headers
        add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;

        # Proxy to FastAPI
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Forwarded-Port $server_port;
    }

    location / {
        # General rate limiting for other endpoints
        limit_req zone=api_general burst=20 nodelay;
        limit_req_status 429;

        # CORS headers
        if ($request_method = 'OPTIONS') {
            add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
            add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
            add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
            add_header 'Access-Control-Max-Age' 1728000 always;
            add_header 'Content-Type' 'text/plain; charset=utf-8' always;
            add_header 'Content-Length' 0 always;
            return 204;
        }

        add_header 'Access-Control-Allow-Origin' 'https://example.com' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'Authorization,DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;

        # Proxy to FastAPI
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $http_host;
        proxy_set_header X-Forwarded-Port $server_port;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
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

The project includes various tools in the `tools/` directory:

- `extract_domains.py` - Domain extraction from URLs
- `import_domains.py` - Bulk domain import
- `banner_grabber.py` - Network banner collection
- `ssl_cert_scanner.py` - SSL certificate analysis
- `masscan_scanner.py` - Port scanning integration

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
