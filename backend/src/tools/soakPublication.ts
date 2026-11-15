import { randomUUID } from 'node:crypto';

/** Old standalone invocations work; only a caller's own nonce can be trusted. */
export function soakInvocationToken(value: string | undefined): string {
  const token=value??randomUUID();
  if(!/^(?:[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$/i.test(token)) {
    throw new Error('--invocation-token must be a UUID');
  }
  return token;
}

/** A zero ACK count is not proof of no publication. Latch before either call. */
export class SoakPublication {
  publicationStarted=false;
  ackPublished:number|null=0;
  auxiliaryPublished:number|null=0;
  stage='input-validation';

  begin(): void {
    this.publicationStarted=true;this.ackPublished=null;this.auxiliaryPublished=null;this.stage='publication';
  }

  failure(context:Record<string,unknown>,error:unknown) {
    return {...context,version:1,passed:false,phase:this.publicationStarted?'publication':'pre-publication',
      publicationStarted:this.publicationStarted,ackPublished:this.ackPublished,
      auxiliaryPublished:this.auxiliaryPublished,stage:this.stage,
      error:{name:error instanceof Error?error.name:'Error',message:error instanceof Error?error.message:String(error),
        ...(error&&typeof error==='object'&&'code' in error?{code:String(error.code)}:{})}};
  }
}
