# MBTA Prediction Error Data Collection

A real-time data collection service that captures MBTA train arrival predictions and actual arrival times via streaming APIs.

## How It Works

The service connects to two MBTA V3 Server-Sent Event (SSE) streams in parallel:

- **Alert Stream** — Tracks active service alerts for all rapid transit lines (Red, Orange, Blue, Green). Alerts are stored in memory and matched to predictions by route, direction, stop, and trip.
- **Prediction Stream** — Captures prediction snapshots as they are created and updated, then logs arrivals when predictions are resolved. Trip metadata is fetched lazily in a background thread pool.

All prediction snapshots and arrival events are stored in a local SQLite database with WAL mode enabled for concurrent access.

## Routes Tracked

Orange, Red, Blue, Green-B, Green-C, Green-D, Green-E

## Database Schema

| Table                  | Purpose                                                      |
| ---------------------- | ------------------------------------------------------------ |
| `trips`                | Cached trip metadata (route, headsign, direction)            |
| `prediction_snapshots` | Append-only log of every prediction event with alert context |
| `arrivals`             | Resolved predictions with arrival/cancellation timestamps    |

See [init_db.sql](init_db.sql) for the full schema and indexes.

## Setup

### Prerequisites

- Python 3.12+
- An [MBTA V3 API key](https://api-v3.mbta.com/)

### Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root:

```
MBTA_API_KEY=your_api_key_here
```

## Usage

```bash
source .venv/bin/activate
python main.py ./data/results.db
```

The service runs indefinitely, collecting data from both streams. Press `Ctrl+C` to stop.

## Project Structure

```
├── main.py                 # Entry point — spawns alert and prediction threads
├── alerts.py               # Thread-safe in-memory alert store and matching
├── alert_stream.py         # SSE consumer for MBTA alerts
├── prediction_stream.py    # SSE consumer for MBTA predictions
├── init_db.sql             # Database schema
└── data/
    └── results.db          # SQLite database (created on first run)
```

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE). You are free to use, modify, and distribute this software, provided that any derivative work is also distributed under the same license with proper attribution.
