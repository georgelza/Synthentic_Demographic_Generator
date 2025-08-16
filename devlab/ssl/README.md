

your-project/
├── docker-compose.yml
├── .env
├── ssl/
│   ├── certs/                    # Server certificates (readable by Redis server)
│   │   ├── ca-bundle.crt        # Certificate Authority bundle
│   │   └── redis-server.crt     # Redis server certificate
│   ├── private/                  # Server private keys (restricted access)
│   │   └── redis-server.key     # Redis server private key
│   └── client/                   # Client certificates (readable by your app)
│       ├── ca-bundle.crt        # CA bundle for client verification
│       ├── client.crt           # Client certificate
│       └── client.key           # Client private key
├── redis/
│   └── redis.conf               # Redis configuration file
├── your_app/
│   ├── connections.py           # Your database connection code
│   └── main.py                  # Your application code
└── Dockerfile                   # Your application Dockerfile



# Make sure certificate directories exist
mkdir -p ssl/certs ssl/private ssl/client redis

# Set permissions for server certificates
chmod 644 ssl/certs/*.crt
chmod 600 ssl/private/*.key

# Set permissions for client certificates  
chmod 644 ssl/client/*.crt
chmod 600 ssl/client/*.key

# Ensure proper ownership (replace with your user)
chown -R $(whoami):$(whoami) ssl/




# Create Certificate Authority
openssl genrsa -out ssl/private/ca.key 4096
openssl req -new -x509 -days 365 -key ssl/private/ca.key -out ssl/certs/ca-bundle.crt

# Create Redis Server Certificate
openssl genrsa -out ssl/private/redis-server.key 4096
openssl req -new -key ssl/private/redis-server.key -out ssl/redis-server.csr
openssl x509 -req -days 365 -in ssl/redis-server.csr -CA ssl/certs/ca-bundle.crt -CAkey ssl/private/ca.key -CAcreateserial -out ssl/certs/redis-server.crt

# Create Client Certificate
openssl genrsa -out ssl/client/client.key 4096
openssl req -new -key ssl/client/client.key -out ssl/client.csr
openssl x509 -req -days 365 -in ssl/client.csr -CA ssl/certs/ca-bundle.crt -CAkey ssl/private/ca.key -CAcreateserial -out ssl/client/client.crt

# Copy CA bundle to client directory
cp ssl/certs/ca-bundle.crt ssl/client/

# Clean up CSR files
rm ssl/redis-server.csr ssl/client.csr

# Usage Commands

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f your-python-app

# Test Redis connection (without SSL)
docker-compose exec redis-server redis-cli ping

# Test Redis connection (with SSL)
docker-compose exec redis-server redis-cli --tls --cert /etc/ssl/certs/redis-server.crt --key /etc/ssl/private/redis-server.key --cacert /etc/ssl/certs/ca-bundle.crt -p 6380 ping

# Access Redis Insight (web interface)
# Open browser to http://localhost:8001

# Stop all services
docker-compose down

# Remove volumes (careful - deletes data!)
docker-compose down -v