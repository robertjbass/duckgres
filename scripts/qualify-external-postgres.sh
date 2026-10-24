#!/usr/bin/env bash
# CI-only Ubuntu fixture. Run after compiling the native server test executable
# and caching its exact-ABI postgres_scanner extension. No downloads inside this
# script. Parent CI must hold the shared validation lock for the whole script.
# Usage: script /absolute/server.test /absolute/extension-cache /usr/lib/postgresql/17/bin
set -euo pipefail
[[ $# == 3 ]] || exit 2
[[ $EUID != 0 ]] || { echo 'Run as unprivileged CI user with sudo'; exit 2; }
fixture_test=$(realpath "$1")
fixture_extensions=$(realpath "$2")
fixture_pg_bin=$(realpath "$3")
fixture_root=$(mktemp -d /tmp/duckgres-native-pg.XXXXXX)
trap 'rm -rf "$fixture_root"' EXIT
# These credentials are disposable fixture constants, never production secrets.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=FixtureCA \
  -keyout "$fixture_root/ca.key" -out "$fixture_root/ca.crt" >/dev/null 2>&1
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=UntrustedFixtureCA \
  -keyout "$fixture_root/wrong-ca.key" -out "$fixture_root/wrong-ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj /CN=source.invalid \
  -keyout "$fixture_root/server.key" -out "$fixture_root/server.csr" >/dev/null 2>&1
printf 'subjectAltName=DNS:source.invalid\nextendedKeyUsage=serverAuth\n' > "$fixture_root/cert.ext"
openssl x509 -req -days 1 -in "$fixture_root/server.csr" -CA "$fixture_root/ca.crt" \
  -CAkey "$fixture_root/ca.key" -CAcreateserial -extfile "$fixture_root/cert.ext" \
  -out "$fixture_root/server.crt" >/dev/null 2>&1
chmod 600 "$fixture_root/server.key"
printf 'fixture_admin_only\n' > "$fixture_root/password"
"$fixture_pg_bin/initdb" -D "$fixture_root/data" -U fixture_admin \
  --pwfile="$fixture_root/password" --auth-local=trust --auth-host=scram-sha-256 >/dev/null
cat >> "$fixture_root/data/postgresql.conf" <<CFG
listen_addresses = '11.0.0.2'
port = 55432
ssl = on
ssl_cert_file = '$fixture_root/server.crt'
ssl_key_file = '$fixture_root/server.key'
unix_socket_directories = '$fixture_root'
shared_buffers = '32MB'
max_connections = 16
CFG
printf 'local all all trust\nhostssl all all 11.0.0.2/32 scram-sha-256\n' > "$fixture_root/data/pg_hba.conf"
# The synthetic public-looking address exists ONLY inside a brand-new network
# namespace. No veth, default route, or outward interface is created. Never add
# this address to the host network or run the body without unshare --net.
sudo unshare --net -- bash -s -- "$(id -un)" "$fixture_root" "$fixture_pg_bin" "$fixture_test" "$fixture_extensions" <<'INNER'
set -euo pipefail
fixture_user=$1 fixture_root=$2 fixture_pg_bin=$3 fixture_test=$4 fixture_extensions=$5
ip link set lo up
ip addr add 11.0.0.2/32 dev lo
[[ $(ip -o link show | wc -l) == 1 ]]
[[ -z $(ip route show default) ]]
fixture_cleanup() { runuser -u "$fixture_user" -- "$fixture_pg_bin/pg_ctl" -D "$fixture_root/data" -m immediate -w stop >/dev/null 2>&1 || true; }
trap fixture_cleanup EXIT INT TERM
runuser -u "$fixture_user" -- "$fixture_pg_bin/pg_ctl" -D "$fixture_root/data" -l "$fixture_root/postgres.log" -w start >/dev/null
runuser -u "$fixture_user" -- "$fixture_pg_bin/psql" -h "$fixture_root" -p 55432 -U fixture_admin -d postgres -v ON_ERROR_STOP=1 <<'SQL'
CREATE DATABASE qualification;
CREATE ROLE source_reader LOGIN PASSWORD 'fixture_reader_only';
\connect qualification
CREATE TABLE fixture (id integer PRIMARY KEY, value integer NOT NULL);
INSERT INTO fixture VALUES (1,42);
GRANT CONNECT ON DATABASE qualification TO source_reader;
GRANT USAGE ON SCHEMA public TO source_reader;
GRANT SELECT ON fixture TO source_reader;
SQL
runuser -u "$fixture_user" -- env PGCONNECT_TIMEOUT=5 DUCKGRES_PG_QUALIFICATION=1 \
  DUCKGRES_PG_ADMIN_SOCKET="$fixture_root" DUCKGRES_PG_CA="$fixture_root/ca.crt" DUCKGRES_PG_WRONG_CA="$fixture_root/wrong-ca.crt" \
  DUCKGRES_PG_EXTENSION_DIR="$fixture_extensions" GOMAXPROCS=1 GOMEMLIMIT=384MiB \
  timeout --signal=TERM --kill-after=10s 150s "$fixture_test" \
  -test.run '^TestExternalPostgresNativeTLSAndReconnect$' -test.v -test.timeout 120s
INNER
