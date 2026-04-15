# Dashboard

Standalone operator dashboard for the Edge repo.

## Run

From the repo root:

```bash
./dashboard/start.sh 7477
```

Then open:

- `http://localhost:7477`
- `http://127.0.0.1:7477`

If port `7477` is busy, pick another one:

```bash
./dashboard/start.sh 8000
```

Then open `http://localhost:8000`.

## WSL

If you are running the server inside WSL and opening it from Windows, use:

- `http://localhost:7477`
- or `http://127.0.0.1:7477`

If localhost forwarding is disabled in your setup, find the WSL IP and open that instead:

```bash
hostname -I
```

Then open `http://<wsl-ip>:7477`.

## Stop

Press `Ctrl+C` in the terminal running `./dashboard/start.sh`.

## Notes

- The dashboard serves static HTML from `dashboard/index.html`.
- The backend is `dashboard/server.py`.
- Refresh in the UI re-reads cached repo state and job status.
