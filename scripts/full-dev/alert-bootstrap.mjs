const headers = {authorization: `Bearer ${process.env.ALERT_RECEIVER_TOKEN}`, 'content-type': 'application/json'};
for (let attempt = 0; attempt < 120; attempt++) {
  try {
    const ready = await fetch('http://alert-receiver:8080/readyz', {signal: AbortSignal.timeout(3000)});
    if (ready.ok) process.exit(0);
    await fetch('http://alert-receiver:8080/alerts', {
      method: 'POST', headers, signal: AbortSignal.timeout(3000),
      body: JSON.stringify({source: 'local-dev', status: 'resolved', check: 'local forwarding bootstrap'}),
    });
  } catch (error) {
    if (attempt === 119) throw error;
  }
  await new Promise(resolve => setTimeout(resolve, 500));
}
throw new Error('Local alert forwarding did not become ready');
