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

## Rotating a broker password

Perform these steps in order. Do not skip step 2: changing the broker password
alone does not invalidate owner cookies.

1. Change the password in the Mosquitto password file or secret store, then
   reload the broker so the old password stops authenticating.
2. From `backend/`, revoke every owner session for the affected MQTT username:

   ```sh
   npm run owner-auth:revoke -- <mqtt-username>
   ```

   The command increments the username's credential generation and revokes its
   `owner_sessions` rows. It fails if Redis cannot record the new generation;
   confirm it exited successfully before treating rotation as complete. Do not
   pass the password to this command or record it in logs.

   In a container deployment, the same tool can be run directly:

   ```sh
   docker exec meshcore-analytics-backend-1 \
     node dist/tools/revokeOwnerSessions.js <mqtt-username>
   ```

3. Verify the rotation: a request carrying a pre-rotation owner cookie must
   return `401` with `Credentials have been rotated — please log in again`.
   Log in again to mint a cookie under the new generation.
