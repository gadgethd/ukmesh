import {
  bumpOwnerCredentialGeneration,
  closeOwnerCredentialGenerationClient,
} from '../owner/ownerAccess.js';

async function main(): Promise<void> {
  const mqttUsername = process.argv[2]?.trim() ?? '';
  if (process.argv.length !== 3 || !/^[A-Za-z0-9_.@-]{1,128}$/.test(mqttUsername)) {
    console.error('Usage: node dist/tools/revokeOwnerSessions.js <mqtt-username>');
    process.exitCode = 2;
    return;
  }

  try {
    await bumpOwnerCredentialGeneration(mqttUsername);
    console.log(`Revoked owner sessions for ${mqttUsername}`);
  } catch (error) {
    console.error(
      'Could not revoke owner sessions:',
      error instanceof Error ? error.message : String(error),
    );
    process.exitCode = 1;
  } finally {
    try {
      await closeOwnerCredentialGenerationClient();
    } catch (error) {
      console.error(
        'Could not close owner credential generation connection:',
        error instanceof Error ? error.message : String(error),
      );
      process.exitCode = 1;
    }
  }
}

void main();
