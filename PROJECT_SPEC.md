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
- Google Drive: persistent file storage and persistent application state where appropriate.
- GitHub: source code, documentation, configuration templates, and version history.
- The normal startup flow should restore the project from persistent state and recover unfinished work safely.

## Access
- Only the owner's Telegram account may use the bot.
- Other Telegram users must be rejected.
- The Telegram bot token is entered only during first-time setup and then stored securely.
- Google Drive authorization is performed only during first-time setup. Normal daily/runtime startup must not ask for Google login or Drive consent again.

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
- Temporary Colab files are deleted after successful and verified Drive storage.

## Duplicate handling
- Avoid duplicate uploads using content identity rather than filename alone whenever practical.
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

## Recovery
- If Colab stops during an operation, that operation must not be marked complete.
- On the next startup, inspect persistent operation state and recover/retry unfinished work without creating duplicates.
- If the runtime is offline, the bot should clearly indicate that it is stopped and provide the Colab notebook link/button where appropriate.
- The system must never pretend that an operation succeeded when verification did not occur.

## Validation and safety
- Verify Drive upload completion before reporting success.
- Basic validation must detect obvious invalid downloads such as an HTML error page returned instead of the requested file.
- Archive extraction must include reasonable path/traversal and unsafe-archive protections without unnecessary complexity.
- Advanced cryptographic/integrity validation is deferred to a later phase.
- No artificial file-size limit should be imposed by the application; practical limits come from Telegram, the source, Colab, network, and Google Drive.

## Settings and logs
Settings should eventually cover only useful operational controls, such as destination folder, retry behavior, archive handling, video-quality behavior, cleanup, notifications, logs, and the Colab notebook link.

Persistent history should record useful operation information such as date, type, source, filename, status, result/size when available, and failure reason when relevant.

## Explicitly deferred
- Gemini/AI integration.
- Advanced integrity verification.
- Drive file management/search/delete/move/rename features beyond what the uploader requires.
- CI/CD and GitHub Actions unless later justified.
- ChatGPT↔Colab/MCP direct integration.
- Any unnecessary multi-user architecture.

## Implementation principle
Start with the smallest reliable system that satisfies the above behavior. Prefer simple, testable components over premature abstraction. Every implementation step must preserve the requirements in this document and update the documentation when a decision changes.
