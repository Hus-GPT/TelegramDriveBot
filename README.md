# TelegramDriveBot

Private, single-owner Telegram bot for moving files and URLs into one Google Drive destination from Google Colab.

## Current v1 architecture

`Telegram / URL → temporary Colab → mounted Google Drive → verification → cleanup`

Google Drive API is intentionally not used in v1.

## Implemented foundation

- Owner-only Telegram access.
- Direct URL intake.
- Telegram document/video/audio/voice/photo intake.
- Queue with one active transfer at a time.
- Persistent state on mounted Google Drive.
- Startup recovery of unfinished jobs.
- Temporary-file cleanup after verified storage.
- SHA-256 duplicate detection after obtaining the file.
- Bounded URL download behavior and basic invalid-HTML detection.
- Cancel button for the active operation.
- Large Telegram-file bridge path through `@File2url_rbot`, using Telegram bot-to-bot communication when configured and enabled.
- Secure configuration through Colab Secrets/environment variables; secrets are not stored in GitHub.

## Important

GitHub is the source of truth for the project, not the execution environment. Actual Telegram/Colab/Drive operation must be verified in Google Colab.

See `PROJECT_SPEC.md` for the full specification and `COLAB_SETUP.md` for the current setup path.
