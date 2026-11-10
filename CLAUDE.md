# Working on dipper

## Fixing failures

When something fails in production, fix the class of failure, not only this instance. Work out why it could happen and change the code so the same kind of failure can't recur, with a test that reproduces it written first. A manual workaround such as a restart is fine to stop the damage, but the follow-up PR removes the cause.

Check nearby code for the same weakness and fix it there too. When one Kafka client fails to recover from a broker change, for example, every other Kafka client dipper runs needs the same fix.
