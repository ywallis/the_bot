# Runbook: accountant export to PostgreSQL

The accountant export (`apps/accountant/src/export.py`) polls each venue
over REST. It writes finished orders and trades to the `orders` and
`trades` tables. It reads the symbols of the production strategies from the
config. It runs on the archive host and uses an existing PostgreSQL server.

## 1. Prepare the database

1. On the PostgreSQL server, connect as a superuser. Make a role and a
   database that the role owns:

   ```sql
   CREATE ROLE the_bot LOGIN PASSWORD '<password>';
   CREATE DATABASE arb_bot OWNER the_bot;
   ```

2. Make the tables. Run this command from a checkout as the new role:

   ```bash
   psql -h <host> -U the_bot -d arb_bot \
     -f docker/database/postgres/scripts/create_tables.sql
   ```

   If `psql` is not installed, run the file through `pg_connect` in
   `apps/accountant/src/utils.py`.

## 2. Configure the host

1. Copy the config of the trading host to the archive host. The export
   reads the symbols from this file, not from the strategies submodule:

   ```bash
   scp <trading host>:<live checkout>/apps/strategies/config/config.toml \
     ~/services/accountant-config.toml
   ```

2. Write `.env` in the root of the checkout with mode 600. Set these
   variables:

   ```
   <PREFIX>_KEY, <PREFIX>_SECRET, <PREFIX>_PASSWORD  for each venue
   POSTGRES_HOST=<host>
   POSTGRES_PORT=5432
   POSTGRES_DB=arb_bot
   POSTGRES_USER=the_bot
   POSTGRES_PASSWORD=<password>
   THE_BOT_CONFIG=/home/<user>/services/accountant-config.toml
   ```

   Do not set `TESTING=True`. That value selects the test strategies.

3. Do one pass by hand. Stop it with Ctrl-C after the first insert lines:

   ```bash
   uv run --frozen -m apps.accountant.src.export
   ```

## 3. Keep the export running

The host has no sudo and no lingering user, so cron starts the export.
`flock -n` keeps one copy. Cron starts a new copy within five minutes after
a crash or a reboot. Add this line with `crontab -e`:

```
*/5 * * * * cd <repo> && flock -n ~/services/export.lock <uv> run --frozen -m apps.accountant.src.export >> ~/services/export.log 2>&1
```

To stop the export, remove the line and kill the process:

```bash
pkill -f apps.accountant.src.export
```

## 4. Serve the status page

`apps/accountant/src/web.py` shows open orders, balances, imbalances and
the daily and monthly performance on one page. It takes a new snapshot
every 15 seconds. It uses the same `.env` and config as the export.

1. Add this line with `crontab -e`:

   ```
   */5 * * * * cd <repo> && flock -n ~/services/web.lock <uv> run --frozen -m apps.accountant.src.web --host 0.0.0.0 --port 8080 >> ~/services/web.log 2>&1
   ```

2. Open `http://<archive host>:8080/` from the local network. The page has
   no login. Do not expose the port outside the local network.
   Without `--host`, the page listens on `127.0.0.1` only.

An imbalance is pending if an open order has its client order id. The page
lists only orders that the bot made. It counts the imbalances on orders
made by hand. The "marked at last price" column adds the inventory change
at the last trade price to the quote gain.

An imbalance shows "unknown" if the open orders of a venue could not be
listed. Do not close an unknown imbalance by hand. Its order can still rest
on that venue. If the database cannot be read, the page says so. An empty
table always means no row.

## 5. After a strategy change

The export and the status page do not see a new symbol until their config
has the symbol. Copy the config again after each change on the trading
host. Then restart both. Cron starts them again within five minutes:

```bash
pkill -f apps.accountant.src.export
pkill -f apps.accountant.src.web
```

The export asks for the newest 100 finished orders and 100 trades per venue
and symbol every 10 seconds. If more than 100 orders finish in that time,
the export loses the oldest ones. Use `apps/accountant/src/history.py` to
fill the gap.
