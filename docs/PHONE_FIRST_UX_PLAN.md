# Phone-first Colab experience

## User-facing contract

The owner must not need to read or edit source code during normal use. The daily workflow should be:
1. Open the official Colab notebook.
2. Run the single visible startup cell.
3. Use the private Telegram bot UI from Android.

The notebook is a launcher, not the application UI. It should display concise Arabic status messages and actionable errors only. It must not print source files, tokens, environment variables, or raw stack traces to normal output.

## Architecture

- Colab: temporary runtime, one-cell startup, Drive mount, dependency preparation, health checks, and bot process.
- Telegram: primary mobile UI for adding URLs/files, progress, history, retry, and cancellation.
- Google Drive: durable destination and persistent recovery state.
- GitHub: reviewed source of truth. Runtime must use a pinned, reviewed release/commit; never blindly execute an unreviewed moving branch.
- Python modules: transfer engine, state store, Telegram UI, configuration, and startup remain separate and testable.

## Security decisions

- Never write Telegram tokens or passwords to tracked files or logs.
- Read secrets through Colab Secrets/environment where possible. First-time setup must clearly explain the minimum required secret setup in Arabic.
- The current engine requires OWNER_ID. The user's stated preference is password-only access, so do not silently ship the current authorization model as if it met that requirement. Decide and test the access policy before production. Prefer owner allow-list security unless the user explicitly accepts the weaker password-only threat model.
- Validate all submitted URLs and redirects. Never log URL query strings or credentials.
- Pin runtime code to a reviewed commit/release and verify the expected revision before starting.

## Reliability acceptance criteria

- Persist a job before reporting that it was accepted.
- On startup, reconcile persisted jobs with files already present in Drive before retrying work.
- A job becomes completed only after the destination file is verified by size and SHA-256 where practical.
- A crash between upload and state update must not create a duplicate final file.
- Interrupted local staging files are treated as ephemeral; retry from the source if unavailable.
- One failed job must not stop the queue.
- Handle disk pressure, Drive unmount, revoked authorization, and rate limits with Arabic actionable messages.
- Never promise that the bot is available while the Colab runtime is stopped.

## UI acceptance criteria (Android)

- Arabic-first, right-to-left Telegram labels and short messages.
- Main actions: add link/file, status, recent history, retry failed job, cancel active job, help.
- Progress updates are throttled and readable on a narrow phone screen.
- Destructive actions require confirmation.
- Do not expose internal paths, tracebacks, or unnecessary technical metadata.
- A web dashboard is deferred unless Telegram proves insufficient; avoid adding a second UI before the primary flow is reliable.

## Validation gate

Before merging a release:
1. Run the repository's automated tests in a reproducible environment.
2. Add tests for access policy, persistence/recovery, URL deduplication, secret/log redaction, and interrupted finalization.
3. Test a small public file end-to-end into a dedicated Drive folder.
4. Test restart during download and after Drive copy but before state commit.
5. Test Android Telegram flows and a Colab restart manually.
6. Report only checks that were actually run; do not infer production readiness from static inspection.

## Scope for the first implementation PR

- Add the official single-cell Colab notebook only after its startup path is compatible with the approved security model.
- Keep the existing main branch untouched until CI and the required checks pass.
- Do not bundle a new web frontend, cloud server, database, or unverified large-file relay into the first change.
