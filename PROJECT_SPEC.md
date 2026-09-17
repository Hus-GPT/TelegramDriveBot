# TelegramDriveBot — Project Specification

## Purpose
A private, single-owner Telegram bot that receives Telegram files or URLs and safely stores the resulting files in one Google Drive folder. The system runs from Google Colab, uses GitHub as the source of truth, and Google Drive as persistent storage.

## Collaboration model
- GitHub is the shared source of truth for code, specifications, decisions, and history.
- ChatGPT and Gemini work sequentially, not simultaneously.
- Before changing anything, inspect the current repository state and existing decisions.
- Never overwrite another developer's work without inspecting the latest state first.
- Do not claim a task is complete without verifying the actual result.
- Secrets and credentials must never be committed to GitHub.
- Current branch: `main`. Additional branches are optional and should only be introduced when they provide a concrete benefit.
- No application code is required yet; this document records the agreed direction before implementation.

## Runtime and storage
- Telegram: user interface and input channel.
- Google Colab: execution/runtime environment; ephemeral.
- Google Drive: persistent file storage and durable recovery snapshots/application state as defined by the recovery design.
- GitHub: source code, documentation, configuration templates, and version history.
- The normal startup flow should restore the project from persistent state and recover unfinished work safely.

## Access and credentials
- Only the owner's Telegram account may use the bot.
- Other Telegram users must be rejected.
- The Telegram bot token is entered only during first-time setup and then stored securely outside GitHub.
- Google Drive authorization is performed only during first-time setup. Normal daily/runtime startup must not ask for Google login or Drive consent again.
- Google Drive access should use the Drive API rather than interactive Drive mounting as the application's primary storage mechanism.
- OAuth credentials, including any refresh token required by the chosen implementation, must be stored securely in Colab Secrets or an equivalent secure secret mechanism and must never be committed to GitHub or stored as plaintext in project files.
- If Local Telegram Bot API Server is used, its required `api_id` and `api_hash` are also secrets and must be stored outside GitHub.

## Telegram file transport
- The application should support Telegram files larger than the public Bot API download limit.
- The primary planned approach is the official Telegram Local Bot API Server in local mode, because it provides unlimited file download size and up to 2000 MB upload size while retaining the Bot API programming model.
- The Local Bot API Server must be evaluated in the actual Colab runtime before implementation is considered stable.
- Telethon/MTProto is a fallback option if the Local Bot API Server proves unsuitable for the required workflow or reliability in Colab.
- The application itself must not impose an artificial file-size limit; actual limits are those of the selected Telegram transport, source, Colab, network, and Google Drive.

## Input
- The user can send a Telegram file directly.
- The user can send a URL directly.
- No structured command syntax is required for normal operations.
- URLs should be analyzed automatically, including redirects/short links where technically possible.

## Google Drive behavior
- One main destination folder is used.
- The destination folder can be changed through settings.
- Original file formats are preserved.
- Archives such as ZIP/RAR are extracted into Drive while preserving their internal hierarchy; the archive itself is not retained as the primary result unless explicitly required later.
- Temporary Colab files are deleted only after successful and verified Drive storage.
- Drive upload completion must be verified before an operation is recorded as successful.

## Duplicate handling and content identity
- Avoid duplicate uploads using content identity rather than filename alone whenever practical.
- For Telegram files, `file_unique_id` may be used as an immediate first-stage identity signal. It cannot itself be used to download/reuse the file.
- For external URLs, use inexpensive preliminary indicators such as normalized URL, available size, ETag, and other reliable source metadata when available.
- Use stronger verification when preliminary indicators are insufficient or ambiguous.
- Prefer streaming SHA-256 calculation during transfer where practical instead of performing a separate full read solely for hashing.
- The duplicate system must balance correctness, bandwidth, local disk usage, and runtime duration; it must not download a complete file solely to calculate a hash when a safe earlier decision is possible.
- If the same content already exists, reply simply: `الملف موجود مسبقاً`.
- Do not expose the existing file link in the duplicate response.

## URL handling
- Detect the source/type automatically.
- Use the appropriate retrieval method for the source.
- If a suitable method fails transiently, retry or try another suitable method where practical.
- If authentication is required and no supported alternative exists, report that clearly.
- For video sources with selectable qualities, show available quality buttons before downloading; do not silently choose a quality.
- Avoid unnecessary metadata in the user interface.

## Queue and progress
- Multiple files/URLs can be submitted and processed through a queue.
- Show the current operation and waiting operations when useful.
- Provide a clean, professional progress display.
- Long operations should provide meaningful progress and a completion notification.
- The user can cancel the current operation.
- Transient failures should be retried automatically.
- Interrupted work should be recoverable where technically possible.

## Recovery and persistent state
- Colab's local runtime filesystem must not be treated as durable storage.
- Operational state such as queued jobs, current operation state, completion status, retry information, and recovery information must be represented in a persistent design independent of the Colab process lifetime.
- The planned local state engine is SQLite on the fast local Colab filesystem, not SQLite directly on a Google Drive-mounted filesystem.
- Persistent recovery snapshots/state records should be stored on Google Drive through the Drive API.
- Recovery persistence must not rely only on an infrequent periodic backup. Important state transitions should be persisted/snapshotted according to the final recovery design so that an unexpected runtime loss minimizes lost state.
- On startup, restore the latest valid persistent state, inspect unfinished operations, and recover/retry them safely without creating duplicate uploads.
- If Colab stops during an operation, that operation must not be marked complete.
- The system must never pretend that an operation succeeded when verification did not occur.
- If the runtime is offline, do not assume the Telegram bot can detect or respond to new messages. When the bot is running, operational status and a Colab restart link/button may be provided where appropriate.

## Validation and safety
- Verify Drive upload completion before reporting success.
- Basic validation must detect obvious invalid downloads such as an HTML error page returned instead of the requested file.
- Archive extraction must include reasonable path/traversal and unsafe-archive protections without unnecessary complexity.
- Advanced cryptographic/integrity validation is deferred to a later phase, except for the SHA-256/content-identity mechanisms needed for duplicate prevention.
- Network/transient failures should use bounded retries with appropriate backoff.
- Temporary files must be cleaned only after the durable result has been verified.

## Settings and logs
Settings should eventually cover only useful operational controls, such as destination folder, retry behavior, archive handling, video-quality behavior, cleanup, notifications, logs, and the Colab notebook link.

Persistent history should record useful operation information such as date, type, source, filename, status, result/size when available, and failure reason when relevant.

## Daily startup requirement
- First-time setup may contain multiple setup steps, including credential configuration and initial Google Drive authorization.
- After first-time setup, the normal daily/runtime workflow must use exactly one user-run Colab cell to start the system.
- That startup cell should obtain the latest stable project state from GitHub, prepare the runtime, load secure secrets, initialize the Telegram Local Bot API Server if that approach is in use, initialize persistent state/recovery, and start the bot.
- The daily startup workflow must not normally ask the user to re-enter the Telegram token or repeat Google Drive authorization.
- If a credential has genuinely expired, been revoked, or become invalid, the system should report the specific setup action required rather than silently pretending the service is available.

## Explicitly deferred
- Gemini/AI integration.
- Advanced integrity verification beyond what is required for duplicate prevention and safe transfer.
- Drive file management/search/delete/move/rename features beyond what the uploader requires.
- CI/CD and GitHub Actions unless later justified.
- ChatGPT↔Colab/MCP direct integration.
- Any unnecessary multi-user architecture.
- Supabase or another external database service unless the recovery requirements later demonstrate a concrete need for one.

## Implementation principle
Start with the smallest reliable system that satisfies the above behavior. Prefer simple, testable components over premature abstraction. Do not introduce multiple competing Telegram transport mechanisms or external persistence services unless testing demonstrates a concrete need. Every implementation step must preserve the requirements in this document and update the documentation when a decision changes.

## Pre-implementation validation gate
Before substantial application implementation begins, validate the following in the real Colab environment:
1. Local Telegram Bot API Server can be installed/started reliably within the intended startup workflow.
2. The bot can receive and access a large Telegram file through the local server.
3. Google Drive API authentication can operate non-interactively after first-time setup using secure stored credentials.
4. Persistent recovery state can survive a Colab runtime interruption and restore safely.
5. Upload verification and duplicate prevention behave correctly for representative files and URLs.

Only after these validation points pass should the project move into full implementation.
