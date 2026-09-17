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

Telegram now supports Bot-to-Bot Communication. In private chats, both the sending and receiving bots must have Bot-to-Bot Communication Mode enabled in @BotFather. The current implementation forwards the original large-file message to `FILE2URL_BOT_USERNAME` and waits for a URL reply from that bot. Telegram documents this capability and the requirement explicitly.

The actual `@File2url_rbot` integration must still be tested end-to-end because Telegram's capability only establishes bot-to-bot messaging; it does not guarantee that a particular third-party bot accepts the forwarded message and returns a usable URL.

If the third-party bot does not respond with a usable URL, the operation fails clearly and is never reported as completed.

## Current v1 scope

Implemented first path:

`Telegram/URL -> temporary Colab -> mounted Google Drive -> verification -> cleanup`

The project deliberately does not use Google Drive API.
