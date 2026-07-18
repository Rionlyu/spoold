## What changed

Describe the user-visible behavior or failure mode this addresses.

## Verification

List the checks and failure scenarios you exercised.

## Durability checklist

- [ ] I ran `make verify`.
- [ ] Persistence or state-transition changes include replay coverage.
- [ ] Crash-sensitive changes include a process-level test where practical.
- [ ] Documentation describes any changed delivery or retention semantics.
- [ ] This change does not introduce secrets or sensitive payloads into tests,
      logs, or fixtures.
