# Security policy

## Reporting a vulnerability

Please **don't open a public issue.** Report it privately through GitHub: the repository's
**Security** tab → **Report a vulnerability**. You'll get an answer within a week, and credit in
the fix if you'd like it.

## Supported versions

Only the latest commit on `main`. This is a reference implementation, not a hosted service.

## What's in place

- **Authentication:** JWT access tokens from an identity provider, verified with its public
  keys (RS256 only, JWKS with rotation). See [Authentication](README.md#authentication).
- **No production defaults for secrets:** outside `ENVIRONMENT=dev`, the API refuses to start
  without a `DATABASE_URL` and a way to verify tokens. The local passwords in
  `docker-compose.yml` are for development only, and its ports are bound to `127.0.0.1`.
- **Scanning in CI** ([`security.yml`](.github/workflows/security.yml)), on every push and
  weekly: Python dependencies (pip-audit), the container image (Trivy: fails on HIGH/CRITICAL
  vulnerabilities that have a fix), committed secrets (Trivy), and risky code patterns (ruff's
  Bandit rules, in the lint job).
- **Dependencies** are locked (`uv.lock`), and Dependabot proposes updates weekly for Python
  packages, GitHub Actions and container images. Actions are pinned to commit SHAs.
- **The image** runs as an unprivileged user and carries no package installer.
- **Backups** (`docker-compose.backup.yml`) are encrypted at rest (AES-256), with the passphrase
  kept apart from them; restoring them is tested on every push
  ([runbook](docs/runbooks/restore.md)).
- **Injection and XSS:** every query goes through SQLAlchemy with bound parameters (ruff's S608
  flags SQL built from strings). The API only serves JSON, with `X-Content-Type-Options: nosniff`;
  post text is returned exactly as written, and escaping it is the rendering client's job.
  Tokens travel in the `Authorization` header, not cookies, so there's no CSRF.
