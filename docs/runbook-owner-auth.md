# Owner credential rotation

Owner cookies include the Redis credential generation that was current at login.
The owner session route and protected owner routes compare that generation on
each request. Cookies expire after seven days.

After changing a Mosquitto password, revoke existing owner sessions immediately
by running the backend tool with the affected MQTT username:

```sh
docker exec meshcore-analytics-backend-1 \
  node dist/tools/revokeOwnerSessions.js <mqtt-username>
```

The command fails if Redis cannot record the new generation. Confirm success
before treating password rotation as complete. Do not pass the password to this
command or record it in logs.
