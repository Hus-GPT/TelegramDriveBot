# Colab setup

## First-time setup

1. Open a new Google Colab notebook.
2. Add these values to Colab Secrets (do not put them in GitHub):
   - `TELEGRAM_BOT_TOKEN`
   - `OWNER_TELEGRAM_ID`
   - `DRIVE_DESTINATION` (optional; default: `/content/drive/MyDrive/TelegramDriveBot`)
   - `FILE2URL_BOT_USERNAME` (optional; default: `@File2url_rbot`)
3. The first run mounts Google Drive and may ask for Google authorization.
4. Install the requirements and start the bot from the repository.

## Normal startup

The intended daily workflow is one Colab cell that updates the repository, installs the pinned dependencies, and runs `app.colab_start`.

The bot stores its persistent state under the configured Drive destination in `.telegramdrive_state.json`.

## Large Telegram files

The current implementation uses Telegram's Bot-to-Bot Communication capability to forward a large incoming message to `FILE2URL_BOT_USERNAME` and waits for a URL response. Telegram requires Bot-to-Bot Communication Mode to be enabled for both participating bots for private bot-to-bot communication. This must be enabled/configured in BotFather before this path can work.

If that capability is unavailable, the large-file path fails clearly; it does not silently pretend the file was stored.

## Current v1 scope

Implemented first path:

`Telegram/URL -> temporary Colab -> mounted Google Drive -> verification -> cleanup`

The project deliberately does not use Google Drive API.
