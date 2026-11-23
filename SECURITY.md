# Security Policy

## Supported Versions

Only the latest release receives security updates.

| Version | Supported |
|---------|-----------|
| 0.14.x  | :white_check_mark: |
| < 0.14  | :x: |

## Scope

By design, these are not vulnerabilities ("End-to-End Encryption" in [SecurityArchitecture.md](./SecurityArchitecture.md#end-to-end-encryption)):

- the metadata the hub sees: who talks to whom, when, and how much;
- without a pre-shared key, a hub that rewrites the streams;
- with one pre-shared key per trust domain, a node of that domain that also holds the hub;
- availability: the hub can cut any transfer.

## Reporting a Vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Use GitHub's [Private Vulnerability Reporting](https://github.com/nicopon/TransportR/security/advisories/new) to submit reports confidentially:

1. Go to the repository's **Security** tab.
2. Under **Reporting**, click **Report a vulnerability**.
3. Provide details on the affected component, reproduction steps, and potential impact.

Reports are acknowledged within 48 hours.

