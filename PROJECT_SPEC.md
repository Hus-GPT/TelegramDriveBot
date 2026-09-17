# TelegramDriveBot — Project Specification

## Purpose
A private, single-owner Telegram bot for moving files into one Google Drive destination folder. The bot is a control/interface layer: it accepts files, forwarded Telegram messages, or URLs, determines the safest and most efficient transfer path, and stores the final result in Google Drive.

The core principle is **cloud-to-cloud transfer whenever technically practical**. Google Colab is an execution environment, not permanent file storage. If direct transfer from a source to Google Drive is not possible or reliable, Colab may be used as temporary working storage for that operation only; the temporary data must be removed after verified Drive completion.

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
- Google Colab: execution/runtime environment; ephemeral and never the permanent storage location for user files.
- Google Drive: the single persistent destination for transferred files and durable recovery state/snapshots as defined by the recovery design.
- GitHub: source code, documentation, configuration templates, and version history.
- The normal startup flow should restore the project from persistent state and recover unfinished work safely.

## Access and credentials
- Only the owner's Telegram account may use the bot.
- Other Telegram users must be rejected.
- The Telegram bot token is entered only during first-time setup and then stored securely outside GitHub.
- Google Drive authorization is performed only during first-time setup. Normal daily/runtime startup must not ask for Google login or Drive consent again.
- Google Drive access should use the Drive API rather than interactive Drive mounting as the application's primary storage mechanism.
- OAuth credentials, including any refresh token required by the chosen implementation, must be stored securely in Colab Secrets or an equivalent secure secret mechanism and must never be committed to GitHub or stored as plaintext in project files.
- The credentials required for `@File2url_rbot` integration, if any, must also be stored securely outside GitHub.

## Telegram input and large-file handling
- The normal user experience must not require structured commands for ordinary transfers.
- The user may send a URL directly.
- The user may send/forward a Telegram file or message from a channel, group, or private chat when Telegram makes that message available to the bot.
- The bot must not expose Telegram's public Bot API download limit as a user-facing failure such as "file larger than 20 MB".
- When a forwarded Telegram file is too large to be handled directly by the bot's normal file-download path, the bot should automatically forward/copy the message to `@File2url_rbot`, wait for the resulting URL, and continue the same transfer workflow using that URL as the source.
- The large-file handoff to `@File2url_rbot` should be automatic; the user should not need to manually send the file to that bot.
- The integration must verify that the returned response contains a usable source URL before continuing.
- Telegram file handling must not require a Telegram Local Bot API Server, MTProto/Telethon, `api_id`, or `api_hash` merely to support this large-file workflow. Those technologies are not part of the current architecture.
- The application must not impose an arbitrary file-size limit beyond the actual limits of Telegram, the source service, the selected transfer method, Colab when temporary storage is required, network conditions, and Google Drive.

## Input and source analysis
- Accept direct URLs and supported Telegram-originating files/messages.
- Analyze URLs automatically, including redirects and shortened links where technically possible.
- Identify the source type and choose the most appropriate transfer method automatically.
- The user should not need to know whether a transfer is direct or temporarily staged through Colab.
- If a source requires authentication and no supported authenticated method is available, report that clearly.
- For video sources with selectable qualities, show available quality buttons before the transfer; do not silently choose a quality.
- Avoid unnecessary technical metadata in the user interface.

## Transfer engine
The transfer engine must use the following priority:

1. **Direct/cloud-to-cloud transfer** — preferred whenever the source and Google Drive can be connected reliably without storing the complete file in Colab.
2. **Temporary Colab transfer** — fallback when direct transfer is unavailable, unsupported, or unreliable for that source.

For the temporary path:
- Download the source to Colab only for the active operation.
- Upload the result to the single Google Drive destination folder.
- Verify the Drive result before declaring success.
- Delete the temporary local file/data immediately after verified successful storage.
- Never intentionally retain completed source files in Colab.
- If an operation fails, clean up temporary data according to safe recovery rules without deleting data that is still required for a resumable operation.

The implementation must keep the transfer method behind a clear transfer layer so additional source-specific methods can be added without changing the Telegram user experience.

## Google Drive behavior
- Exactly one main destination folder is used.
- The destination folder can be changed through settings, but there is only one active destination at a time.
- Original file formats are preserved whenever possible.
- Archives such as ZIP/RAR are extracted into Drive while preserving their internal hierarchy; the archive itself is not retained as the primary result unless explicitly required later.
- Temporary Colab files are deleted only after successful and verified Drive storage.
- Drive upload completion must be verified before an operation is recorded as successful.

## Duplicate handling and content identity
- Avoid duplicate uploads using content identity rather than filename alone whenever practical.
- For Telegram-originating content, use available Telegram identifiers and source metadata as early identity signals where reliable.
- For external URLs, use inexpensive preliminary indicators such as normalized URL, available size, ETag, and other reliable source metadata when available.
- Use stronger verification when preliminary indicators are insufficient or ambiguous.
- Prefer streaming SHA-256 calculation during a temporary transfer where practical instead of performing a separate full read solely for hashing.
- For direct/cloud-to-cloud transfers, use source and destination metadata available from the transfer path and perform stronger verification when required by the duplicate decision.
- The duplicate system must balance correctness, bandwidth, local disk usage, and runtime duration; it must not download a complete file solely to calculate a hash when a safe earlier decision is possible.
- If the same content already exists, reply simply: `الملف موجود مسبقاً`.
- Do not expose the existing file link in the duplicate response.

## Queue and progress
- Multiple files/URLs can be submitted and processed through a queue.
- Show the current operation and waiting operations when useful.
- Provide a clean, professional progress display.
- Long operations should provide meaningful progress and a completion notification.
- The user can cancel the current operation.
- Transient failures should be retried automatically with bounded retries and appropriate backoff.
- Interrupted work should be recoverable where technically possible.
- Progress should distinguish meaningful stages such as source preparation, transfer, verification, and cleanup, without exposing unnecessary implementation details.

## Recovery and persistent state
- Colab's local runtime filesystem must not be treated as durable storage.
- Operational state such as queued jobs, current operation state, completion status, retry information, source information, transfer method, and recovery information must be represented in a persistent design independent of the Colab process lifetime.
- The planned local state engine is SQLite on the fast local Colab filesystem, not SQLite directly on a Google Drive-mounted filesystem.
- Persistent recovery snapshots/state records should be stored on Google Drive through the Drive API.
- Recovery persistence must not rely only on an infrequent periodic backup. Important state transitions should be persisted/snapshotted according to the final recovery design so that an unexpected runtime loss minimizes lost state.
- On startup, restore the latest valid persistent state, inspect unfinished operations, and recover/retry them safely without creating duplicate uploads.
- If Colab stops during an operation, that operation must not be marked complete.
- The system must never pretend that an operation succeeded when verification did not occur.
- If temporary data remains after an interrupted operation, startup recovery must decide whether it is safe to resume, verify, or remove it before continuing.
- If the runtime is offline, the bot cannot process new requests. The user-facing behavior should clearly indicate that the bot is currently offline when such a status can be presented, and may provide a Colab restart link/button.

## Validation and safety
- Verify Drive storage completion before reporting success.
- Basic validation must detect obvious invalid source responses such as an HTML error page returned instead of the requested file.
- Archive extraction must include reasonable path/traversal and unsafe-archive protections without unnecessary complexity.
- Advanced cryptographic/integrity validation is deferred to a later phase, except for the content-identity mechanisms needed for duplicate prevention and safe transfer.
- Temporary files must be cleaned only after the durable result has been verified.
- Direct-transfer operations must also have an explicit verification state; direct transfer must never be treated as successful merely because a transfer request was accepted.

## Settings and logs
Settings should eventually cover only useful operational controls, such as destination folder, retry behavior, archive handling, video-quality behavior, cleanup, notifications, logs, and the Colab notebook link.

Persistent history should record useful operation information such as date, type, source, filename, transfer method, status, result/size when available, and failure reason when relevant.

## Daily startup requirement
- First-time setup may contain multiple setup steps, including credential configuration and initial Google Drive authorization.
- After first-time setup, the normal daily/runtime workflow must use exactly one user-run Colab cell to start the system.
- That startup cell should obtain the latest stable project state from GitHub, prepare the runtime, load secure secrets, initialize persistent state/recovery, and start the bot.
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
- A Telegram Web App; the current workflow does not require one.
- MultCloud or another third-party cloud-management service as a mandatory transfer layer; the project should implement/select transfer methods directly rather than depend on such a service.

## Implementation principle
Start with the smallest reliable system that satisfies the above behavior. Prefer simple, testable components over premature abstraction. Do not introduce competing Telegram transport mechanisms or external persistence services unless testing demonstrates a concrete need. The system should not force every transfer through Colab when a reliable direct path exists. Every implementation step must preserve the requirements in this document and update the documentation when a decision changes.

## Pre-implementation validation gate
Before substantial application implementation begins, validate the following in the real Colab environment:
1. A representative direct/cloud-to-cloud transfer path can move a supported source into the single Google Drive destination without storing the complete file in Colab.
2. The Telegram bot can receive/recognize a forwarded file from a channel, group, or private chat when Telegram makes it available to the bot.
3. For a representative large Telegram file, the bot can automatically hand the message to `@File2url_rbot`, receive a usable URL, and continue the transfer workflow without requiring manual user intervention.
4. Google Drive API authentication can operate non-interactively after first-time setup using secure stored credentials.
5. Persistent recovery state can survive a Colab runtime interruption and restore safely.
6. Temporary-file cleanup removes staged files only after verified Drive completion.
7. Upload/transfer verification and duplicate prevention behave correctly for representative files and URLs.

Only after these validation points pass should the project move into full implementation.
