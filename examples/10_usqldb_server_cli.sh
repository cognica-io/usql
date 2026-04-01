#!/bin/bash
# usqldb-server CLI usage examples.
#
# The usqldb-server command is installed via pip and provides a
# PostgreSQL 17-compatible wire protocol server.
#
# Prerequisites:
#   pip install -e .

set -e

echo "=== usqldb-server CLI examples ==="
echo ""
echo "1. In-memory server (simplest):"
echo "   usqldb-server --port 15432"
echo ""
echo "2. Persistent storage:"
echo "   usqldb-server --port 15432 --db /tmp/mydata.db"
echo ""
echo "3. With SCRAM-SHA-256 authentication:"
echo "   usqldb-server --port 15432 \\"
echo "       --auth scram-sha-256 \\"
echo "       --user admin:secret123 \\"
echo "       --user reader:readonly"
echo ""
echo "4. With MD5 authentication:"
echo "   usqldb-server --port 15432 \\"
echo "       --auth md5 \\"
echo "       --user admin:secret123"
echo ""
echo "5. With cleartext password:"
echo "   usqldb-server --port 15432 \\"
echo "       --auth password \\"
echo "       --user admin:secret123"
echo ""
echo "6. Custom bind address and max connections:"
echo "   usqldb-server --host 0.0.0.0 --port 5432 \\"
echo "       --max-connections 200 \\"
echo "       --db /var/lib/usqldb/data.db"
echo ""
echo "7. With SSL/TLS:"
echo "   usqldb-server --port 15432 \\"
echo "       --ssl-cert /path/to/cert.pem \\"
echo "       --ssl-key /path/to/key.pem"
echo ""
echo "8. Debug logging:"
echo "   usqldb-server --port 15432 --log-level DEBUG"
echo ""
echo "=== Connecting with psql ==="
echo ""
echo "   psql -h 127.0.0.1 -p 15432 -U uqa -d uqa"
echo "   psql -h 127.0.0.1 -p 15432 -U admin -d uqa  # with auth"
echo ""
echo "=== Connecting with psycopg (Python) ==="
echo ""
echo "   import psycopg"
echo "   conn = psycopg.connect('host=127.0.0.1 port=15432 user=uqa dbname=uqa')"
echo "   cur = conn.execute('SELECT 1')"
echo "   print(cur.fetchone())"
