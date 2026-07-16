# Security

Please report vulnerabilities through GitHub's private security-advisory
feature rather than opening a public issue.

## Scope

The supported security boundary is the default single-user deployment:

- the API listens on loopback unless configured otherwise;
- private and loopback delivery targets are rejected unless explicitly allowed;
- the journal is created with owner-only file permissions;
- outbound proxy environment variables are ignored.

`spoold` v0.1 has no authentication or tenant isolation. Exposing its API to an
untrusted network is unsupported.

