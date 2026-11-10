# Working on dipper

## Fixing failures

When something fails, whether in production or caught in CI or review, fix its cause first rather than reaching for a workaround such as a restart. A workaround hides the failure, and with it the chance to check the fix against the real thing, so leave the failure in place while the fix is built unless it is hurting users in a way that can't wait. Work out why it could happen and change the code so the same kind of failure doesn't happen again. Before the fix, write a test that reproduces the failure and watch it fail; where that test needs the database or the integration suite, CI's run of it is the proof.

Then look elsewhere in dipper for the same weakness and fix it there too, in a PR of its own when that keeps each change small. A Kafka client that can't recover from a broker change, for example, means checking every other place dipper connects to a broker.
