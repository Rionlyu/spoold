# Contributing

Bug reports, failure reproductions, portability fixes, and real deployment
notes are especially valuable while `spoold` is in alpha.

Before opening a pull request:

```sh
make verify
```

Changes to persistence or delivery state must include a replay or transition
test. Changes to crash behavior should include a process-level test when
possible. Keep the project focused on one-machine durable HTTP delivery; a
dashboard, hosted control plane, and distributed coordination are deliberate
non-goals.

Use GitHub's private security-advisory feature for vulnerabilities rather than
a public issue. By participating, you agree to follow the
[Code of Conduct](CODE_OF_CONDUCT.md).
