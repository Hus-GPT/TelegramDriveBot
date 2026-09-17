# TelegramDriveBot — Project Specification

## Purpose
A private, single-owner Telegram bot for moving files into one Google Drive destination folder. The bot is a control/interface layer: it accepts files, forwarded Telegram messages, or URLs, then transfers the final result into Google Drive.

## Current architecture decision
The first version deliberately uses the simplest Google Drive connection:

- Google Colab mounts the user's Google Drive after a one-time Google authorization.
- Normal daily startup must reuse that authorized connection and must not ask for Google consent again unless authorization is genuinely lost or revoked.
- The bot writes transferred files to the mounted Drive folder through the normal filesystem interface.
- Google Drive API is **not part of the first version**.
- Colab is temporary execution/storage only; Google Drive is the permanent destination.
- Transfers that require downloading data are staged temporarily in Colab and then moved into the mounted Drive folder. Temporary source files are removed after the Drive copy is verified.
- True cloud-to-cloud transfer that bypasses Colab is deferred until a concrete source/transfer method justifies adding it.

## Collaboration model
- GitHub is the shared source of truth for code, specifications, decisions, and history.
- ChatGPT and Gemini work sequentially, not simultaneously.
- Before changing anything, inspect the current repository state and existing decisions.
- Never overwrite another developer's work without inspecting the latest state first.
- Do not claim a task is complete without verifying the actual result.
- Secrets and credentials must never be committed to GitHub.
- Current branch: `main`.

## Runtime and storage
- Telegram: user interface and input channel.
- Google Colab: execution/runtime environment and temporary working space.
- Google Drive: the single persistent destination for transferred files and persistent project state that must survive Colab restarts.
- GitHub: source code, documentation, configuration templates, and version history.

## Access and credentials
- Only the owner's Telegram account may use the bot.
- Other Telegram users must be rejected.
- The Telegram bot token is entered only during first-time setup and stored securely outside GitHub.
- Google Drive authorization/mounting is performed during first-time setup. Normal daily startup must not request Google consent again when the Colab environment still has valid authorization.
- No Google Drive API credentials, refresh-token implementation, or Drive API service is required by the first version.
- Credentials for `@File2url_rbot`, if any are required by the actual integration, must be stored securely outside GitHub.

## Telegram input and large-file handling
- Ordinary transfers must not require structured commands.
- The user may send a URL directly.
- The user may send/forward a Telegram file or message from a channel, group, or private chat when Telegram makes that message available to the bot.
- The bot must not expose Telegram's normal Bot API download limit as a user-facing failure.
- For a file that exceeds the normal Bot API download path, the bot may forward the original message to `@File2url_rbot`, wait for a usable URL, and continue the same transfer workflow using that URL.
- This Bot-to-Bot path depends on Telegram's Bot-to-Bot Communication Mode being enabled for both participating bots in private chats. The integration must be tested with the actual `@File2url_rbot` service before it is considered production-ready.
- The user should not need to manually send the large file to `@File2url_rbot` when the automatic path is working.
- No Local Bot API Server, MTProto/Telethon, `api_id`, or `api_hash` is required merely for this workflow.

## Input and source handling
- Accept direct URLs and supported Telegram-originating files/messages.
- Analyze URLs automatically, including redirects and shortened links where technically possible.
- Identify the source type and choose the appropriate transfer method available in the current version.
- The user should not need to know the internal transfer details.
- If a source requires authentication that the current implementation cannot support, report that clearly.
- For video sources with selectable qualities, show available quality buttons before transfer; do not silently choose a quality.
- Avoid unnecessary technical metadata in the user interface.

## Transfer engine — first version
The first version uses a simple reliable path:

**Source → temporary Colab storage → mounted Google Drive → verification → cleanup**

Rules:
- Colab temporary storage is used only for the active operation.
- The destination is one configured Google Drive folder.
- Drive completion must be verified before the operation is marked successful.
- Temporary files are deleted only after successful verification.
- Failed or interrupted operations must not be reported as successful.
- URL and Telegram downloads use bounded retries with exponential backoff for transient request failures.
- Cancellation is checked while streaming downloaded data.
- Interrupted jobs retain their temporary path in persistent state when available, allowing startup recovery to resume from an existing temporary file instead of automatically starting from zero.

## Google Drive behavior
- Exactly one active destination folder is used.
- The destination folder can be changed through settings.
- Original file formats are preserved whenever possible.
- ZIP/RAR archive extraction remains a later enhancement unless explicitly enabled in implementation.
- Temporary Colab files are deleted after verified Drive storage.
- Verification checks the actual file present in the mounted Drive destination.

## Duplicate handling
- Avoid duplicates using reliable available identity information rather than filename alone whenever practical.
- Telegram-originating content stores Telegram identifiers as early identity signals.
- External URLs retain their source URL for recovery and future identity improvements.
- SHA-256 is used as the authoritative content identity when a content check is required.
- Existing destination files with the same filename are compared by size and SHA-256 before a numbered filename is created.
- If the same content is already known to exist, reply simply: `الملف موجود مسبقاً`.
- Do not expose the existing file link in the duplicate response.

## Queue and progress
- Multiple files/URLs can be submitted and processed through a queue.
- Show the current operation and waiting operations when useful.
- Provide a clean progress display.
- Long operations should provide meaningful progress and a completion notification.
- The user can cancel the current operation.
- Transient failures are retried automatically with bounded retries and backoff.
- Interrupted work is recovered from persistent state where practical.
- Progress distinguishes source preparation, transfer, verification, and cleanup without exposing unnecessary implementation details.

## Recovery and persistent state
- Colab's local filesystem is not durable across runtime loss.
- Operational state is persisted in a small state file inside the configured mounted Google Drive destination.
- The state records queued jobs, status, source information, transfer method, retry/recovery information, and temporary paths when available.
- On startup, unfinished jobs are restored to the queue.
- If a valid temporary file from an interrupted job still exists, recovery reuses it instead of blindly downloading the source again.
- If a temporary file is unavailable, the job can be retried from its retained source information.
- If Colab stops during an operation, that operation is never marked complete merely because it was running.
- If Colab is offline, the bot cannot process new requests.

## Validation and safety
- Verify that the expected file exists in the mounted Drive destination and that its basic metadata is consistent before reporting success.
- Detect obvious invalid source responses such as HTML error pages returned instead of the requested file.
- Temporary files are cleaned only after the durable Drive result has been verified.
- Advanced archive extraction and advanced integrity verification remain outside the minimal transfer path.

## Settings and history
Settings should cover only useful operational controls, such as:
- destination Drive folder
- retry behavior
- archive handling
- video-quality behavior
- temporary-file cleanup
- notifications
- history/log visibility
- Colab notebook link

Persistent history should record useful operation information such as date, type, source, filename, status, transfer method, size when available, and failure reason when relevant.

## Daily startup requirement
After first-time setup, normal daily use should require exactly one user-run Colab cell.

That startup cell should:
1. obtain the latest stable project state from GitHub;
2. prepare the runtime;
3. connect/mount Google Drive using the already-authorized setup;
4. initialize/restore persistent project state;
5. load secure Telegram credentials;
6. start the bot.

It must not normally ask for the Telegram token again or repeat Google Drive authorization.

If Google authorization has genuinely been revoked or is unavailable, the system should clearly identify the required recovery setup instead of pretending that Drive is available.

## Explicitly deferred
- Google Drive API.
- True cloud-to-cloud transfer bypassing Colab.
- Gemini/AI integration.
- Advanced integrity verification beyond the content checks needed for safe duplicate handling.
- Drive file-management features beyond what the uploader requires.
- CI/CD and GitHub Actions unless later justified.
- ChatGPT↔Colab/MCP direct integration.
- Multi-user architecture.
- Supabase or another external database unless the simple Drive-based recovery design proves insufficient.
- Telegram Web App.
- MultCloud or another third-party cloud-management service as a mandatory transfer layer.

## Implementation principle
Start with the smallest reliable system that satisfies the current requirements. Do not introduce services or infrastructure merely because they could be useful later. Every implementation step must preserve the agreed behavior and update this specification when a decision changes.

## First implementation target
Build and validate the minimal end-to-end flow first:

**Telegram owner → receive URL/file → obtain source → temporary Colab file when needed → move to the single mounted Drive folder → verify → delete temporary data → report result.**

Only after this basic flow is working should archive extraction, quality selection, advanced identity detection, and other enhancements be layered in.
