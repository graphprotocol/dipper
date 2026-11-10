# Working on dipper

## Fixing failures

When something fails, whether in production or caught in CI or review, fix its cause and not only this instance. Work out why it could happen and change the code so the same kind of failure doesn't happen again. Before the fix, write a test that reproduces the failure and watch it fail; where that test needs the database or the integration suite, CI's run of it is the proof. A manual workaround such as a restart is fine to stop the damage, but a follow-up PR still removes the cause.

Then look elsewhere in dipper for the same weakness and fix it there too, in a PR of its own when that keeps each change small. A Kafka client that can't recover from a broker change, for example, means checking every other place dipper connects to a broker.
