# Owner credential rotation

For a new observer that connects but stays invisible, first check the
[MQTT onboarding guide](mqtt-onboarding.md). Firmware topic configuration and
verified owner grants are separate requirements; credential rotation does not
repair either. The [generic linking design](designs/generic-owner-linking.md)
records the current discovery/authorization boundaries and follow-up hooks.

Owner cookies include the Redis credential generation that was current at login.
The owner session route and protected owner routes compare that generation on
each request. The encrypted v4 cookie also carries a server-side session ID;
protected reads require an active, unrevoked, unexpired row in the owner-auth
database. Cookies and their server-side sessions expire after seven days.
Revoking one session, all sessions or the credential generation closes access;
a valid cookie alone cannot authorize a revoked server-side session.

After changing a Mosquitto password, revoke existing owner sessions immediately
by running the backend tool with the affected MQTT username:

```sh
docker exec meshcore-analytics-backend-1 \
  node dist/tools/revokeOwnerSessions.js <mqtt-username>
```

The command fails if Redis cannot record the new generation. Confirm success
before treating password rotation as complete. Do not pass the password to this
command or record it in logs.
