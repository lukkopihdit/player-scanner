# Player Scanner Discord Bot

Discord bot that monitors kingdom 810 using the MightPulse API.

## What it does

- Scans the top 100 alliances by alliance power.
- Retrieves the full alliance roster for each tracked alliance.
- Stores all fields returned by the alliance/roster endpoint.
- Detects changes to name, Town Center level, power, kills, alliance, alliance rank, online status, last active time, and other roster fields.
- Sends a Discord notification when a player's Town Center level increases.
- Runs one scan immediately on startup and then once every hour.
- Stores data in SQLite so Docker restarts do not erase the history.
- Rotates between multiple API keys when a key receives HTTP 429/401.
- Provides slash commands for status, manual scans, player lookup, alliance lookup, changes, and notification testing.

## Important API limitation

The documented alliance ranking endpoint exposes at most 100 alliances. This bot therefore monitors the top 100 alliances by alliance power, not every alliance in kingdom 810.

The documented player endpoint can return additional player sections (`base,heroes,ranks,gov_gear`). Fetching those sections for every player every hour would require one player request per player and can exceed the documented request limits. The background scanner therefore stores all data available from the alliance roster endpoint, while `/playerfull` can fetch the complete player response on demand.

## Required environment variables

- `DISCORD_TOKEN`: Discord bot token.
- `MIGHTPULSE_API_KEYS`: comma-separated API keys.
- `NOTIFY_CHANNEL_ID`: Discord channel ID where level-increase notifications are sent.
- `DB_PATH`: SQLite path; defaults to `/data/scanner.db`.

## Slash commands

- `/status` - scanner status.
- `/scan` - run an immediate scan.
- `/changes` - select change type and look back a chosen number of hours.
- `/player governor_id:<id>` - show stored roster data.
- `/playerfull governor_id:<id>` - fetch the complete available player API response and return it as JSON.
- `/alliance tag:<TAG>` - show stored players from an exact case-sensitive alliance tag.
- `/webhooktest` - send a notification-channel test.

## Deployment

Build with:

    docker compose up -d --build

The SQLite database lives in the `scanner-data` Docker volume.
