# Runbook: archive host and trading host recordings

The archive host pulls sealed recordings from each trading host over SSH.
It keeps them under `archive.root` and deletes them from the trading host
after it verifies them. Design: section 7.5 of
`docs/design/event-driven-framework.md`.

## 1. Prepare the trading host

1. Make a checkout for the shipper only. Do not use a checkout that you
   switch branches in, because the forced command runs from it:

   ```bash
   git worktree add --detach ~/services/the_bot-shipper origin/main
   cd ~/services/the_bot-shipper && uv sync --frozen
   ```

2. Run the shipper once by hand. Replace `<data>` with the absolute path of
   the recording root:

   ```bash
   uv run --frozen -m apps.maker.src.shipper --root <data> manifest
   ```

   The command prints one JSON line for each sealed recording. An empty
   output is correct when each stream has only its newest file.

## 2. Give the archive host a restricted key

1. On the archive host, make a key for this purpose only:

   ```bash
   ssh-keygen -t ed25519 -N "" -f ~/.ssh/archive_pull
   ```

2. On the trading host, add the public key to `~/.ssh/authorized_keys` on
   one line. Replace `<archive ip>` with the address the archive host
   connects from, `<repo>` with the shipper checkout, `<uv>` with the
   output of `command -v uv` and `<data>` with the recording root:

   ```text
   from="<archive ip>",command="cd <repo> && <uv> run --frozen -m apps.maker.src.shipper --root <data> serve",restrict ssh-ed25519 AAAA... archive_pull
   ```

   This key can run `manifest`, `send` and `prune` only. It cannot open a
   shell. Leave out `from=` if the archive host has no fixed address.

3. On the archive host, add an alias to `~/.ssh/config`:

   ```text
   Host trading-a
       HostName <address>
       User <user>
       IdentityFile ~/.ssh/archive_pull
       IdentitiesOnly yes
   ```

4. Accept the host key once:

   ```bash
   ssh trading-a manifest
   ```

## 3. Configure the archive host

The archive host reads only the `[archive]` table. Put it in a separate
file, for example `~/services/archive.toml`, and pass `--config` to every
command. Use one `[[archive.sources]]` entry for each trading host. The
`name` is the directory under the archive root.

```toml
[archive]
root = "archive"
level = 19

[[archive.sources]]
name = "trading-a"
ssh = "trading-a"
```

To archive recordings on the archive host itself, leave out `ssh` and set
`root` to the local recording directory.

## 4. Pull for the first time without pruning

```bash
uv run -m apps.accountant.src.archive --config ~/services/archive.toml --no-prune
```

The command prints one summary line for each source. Check that `failed` is
0. Then read an archived stream with a recording tool, for example:

```bash
uv run -m apps.maker.src.tools.pnl --root archive/trading-a
```

## 5. Pull on a schedule

If the first pull is correct, run the pull with pruning every hour from cron
on the archive host:

```text
15 * * * * cd <repo> && <uv> run --frozen -m apps.accountant.src.archive --config <archive.toml> >> archive.log 2>&1
```

Minute 15 is after the recorder seals the previous hour. The recorder keeps
a file open for `recorder.seal_grace_s`, which is 5 minutes by default.

## 6. Recover from a failure

- If a pull stops halfway, run it again. The ledger keeps every file that
  arrived, and the manifest still lists every file that did not.
- If the summary shows `failed`, read `archive.log`. A failed file stays on
  the trading host, and the next pull tries it again.
- If the trading host disk fills, check that the cron job runs. The shipper
  never deletes a file that the archive does not hold.
