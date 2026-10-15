# Runbook: public market recorder

The public recorder writes the books and trades of markets that the trading
system does not trade. The simulators replay these recordings before a
market is quoted. The recorder needs no keys and no Redis. It writes the same
layout as the trading recorder, so the shipper and the archive host handle
it without changes (`docs/runbooks/archive-host.md`).

## 1. Prepare the host

1. Make a checkout for the recorder only. Do not use a checkout that you
   switch branches in, because the service runs from it:

   ```bash
   git worktree add --detach ~/services/the_bot-recorder origin/main
   cd ~/services/the_bot-recorder && uv sync --frozen
   ```

   On a host without a clone, use `git clone` and then
   `git checkout --detach origin/main`. The private strategies submodule is
   not necessary.

2. Record two markets for one minute to test the host. Replace `<venue>`
   with a CCXT id and the symbols with real ones:

   ```bash
   timeout 60 uv run --frozen -m apps.maker.src.tools.public_recorder \
       /tmp/rec-test <venue> BASE/QUOTE OTHER/QUOTE
   find /tmp/rec-test -name '*.jsonl' -exec wc -l {} +
   ```

   Each symbol has a book file and a trade file. A quiet market can have
   no trade file after one minute.

## 2. Divide the markets into groups

1. Put at most ten symbols of one venue in a group. One group is one
   process and one websocket connection. Some venues silently drop
   subscriptions past that number.

2. Put spot symbols and contract symbols (`BASE/QUOTE:SETTLE`) in different
   groups. The tool refuses a group that mixes them.

3. For each group, write `~/services/rec/rec-<group>.env` on one line.
   Replace `<data>` with the absolute recording root:

   ```text
   ARGS=<data> <venue> BASE/QUOTE OTHER/QUOTE
   ```

4. If a config venue has a `ccxt_id` that is not its `id`, add
   `--venue-name <id>` to `ARGS`. The files then go to the same directory as
   the files of the trading recorder.

## 3. Run the groups under systemd

1. If you have root, write `/etc/systemd/system/the-bot-rec@.service`.
   Replace `<user>`, `<repo>` and `<uv>` with the user, the recorder
   checkout and the output of `command -v uv`:

   ```ini
   [Unit]
   Description=the_bot public market recorder, group %i
   After=network-online.target
   Wants=network-online.target

   [Service]
   User=<user>
   WorkingDirectory=<repo>
   EnvironmentFile=/home/<user>/services/rec/rec-%i.env
   ExecStart=<uv> run --frozen -m apps.maker.src.tools.public_recorder $ARGS
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```

2. If you do not have root, write the same unit to
   `~/.config/systemd/user/the-bot-rec@.service`. Remove the `User=` line
   and use `WantedBy=default.target`. Then let the user services start at
   boot without a login:

   ```bash
   loginctl enable-linger "$USER"
   ```

3. Start each group. Add `--user` to each `systemctl` command for user
   services:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now the-bot-rec@<group>
   ```

4. After one minute, make sure that each book writes. This command shows
   each stream that has no file for the current hour:

   ```bash
   cd <data>/md && h=$(date -u +%Y-%m-%dT%H)
   for d in book/*/*; do [ -f "$d/$h.jsonl" ] || echo "silent: $d"; done
   ```

   Books of markets that you removed from all groups also show here.

## 4. Stalls and restarts

If the venue does not list a symbol of the group, the process exits with
status 2 and names the symbol. Systemd does not start it again. Correct the
`ARGS` line and start the group.

A book that writes nothing for three minutes makes its process exit with
status 3. Systemd starts it again with new connections and snapshots. This
gap is approximately 30 seconds for each book in the group. Each stall
writes one line to the journal:

```bash
journalctl -u 'the-bot-rec@*' | grep 'silent for'
```

If one group stalls many times each hour, divide it into smaller groups.
`--stall-s` changes the limit. Healthy books were silent for at most
approximately two minutes, so do not set a limit below three minutes.

## 5. Change the markets

1. Edit the `ARGS` line of the group and restart the group. The recorder
   appends to the file of the current hour, so a restart loses only the
   seconds before the new snapshots.

2. A removed market keeps its last hourly file. The shipper never sends the
   newest file of a stream, so this file stays on the host. After the
   archive holds all the earlier hours, delete the directories of the
   removed market by hand.
