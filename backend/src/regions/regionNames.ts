// Canonical probe list for MeshCore region-scope decoding on the UK mesh.
//
// Public hashtag regions derive their transport key from the region name alone
// (firmware TransportKeyStore::getAutoKeyFor() = SHA256("#name")[0:16]), so a
// packet's transport code can be attributed by testing candidate names — there
// is no key material involved and no reverse mapping. Names that never match
// simply stay unattributed; the rebuild tool logs the unknown share so the list
// can be extended as new regions appear.
//
// The list covers every region name observed on the ukmesh corpus plus the
// wider vocabulary used by neighbouring networks (CoreScope, ScotMesh). Order
// is normalised to sorted() at module load so probing is deterministic: if two
// names ever produced the same transport code the earlier name wins.

const OBSERVED_AND_CANDIDATE_NAMES = `
yorkshire northwest eng-ne eng-yh eng-yorkshire gb uk ren durham eng-tyneside-admin
wales-admin northumbria-admin eng-cum eng-oxf-admin sta-admin ioi eng-cam northwest-admin
north-west eng-nw-admin nw bristol bristol-admin eng-bristol bath avon wessex west-country
westcountry south-west southwest south-coast bournemouth southampton portsmouth brighton
plymouth exeter dorset-admin cornwall-admin devon-admin somerset-admin norwich norfolk-admin
suffolk-admin ipswich east-anglia cambridge peterborough lincoln lincolnshire-admin hull
grimsby scunthorpe doncaster sheffield leeds york bradford halifax huddersfield wakefield
barnsley rotherham chesterfield manchester merseyside liverpool lancashire-admin bolton
wigan preston blackpool carlisle chester crewe warrington stockport oldham rochdale burnley
blackburn teesside middlesbrough darlington sunderland gateshead newcastle tyneside
northumberland eng-tyne eng-tees eng-ncl eng-dur eng-nwk eng-nwest eng-northwest north-east
northeast northeast-admin eng-ne-admin yorkshire-admin selkirkshire cleveland west-yorkshire
south-yorkshire north-yorkshire east-yorkshire ceredigion south-east southeast
southeast-admin east-midlands west-midlands midlands the-midlands mids nw-england
eng-north-west north-west-england norwest wirral cheshire-admin stoke staffordshire-admin
shropshire-admin derbyshire-admin nottinghamshire-admin leicestershire-admin
worcestershire-admin warwickshire-admin hertfordshire-admin bedfordshire-admin
buckinghamshire-admin oxfordshire-admin berkshire-admin hampshire-admin surrey-admin
kent-admin sussex-admin essex-admin sw-admin
sco wls iom fif hig tay edi noc cen gla fal dun per inv sti ioi-admin europe global
`;

export const REGION_NAMES: readonly string[] = [
  ...new Set(OBSERVED_AND_CANDIDATE_NAMES.split(/\s+/).filter(Boolean)),
].sort();

/**
 * Build the effective probe list: canonical names plus any extras from a
 * comma-separated env value (same convention as the ingest-side
 * MESHCORE_REGION_NAMES probe). Extra names keep their case — region keys are
 * case-sensitive — and the leading '#' is tolerated for convenience.
 */
export function regionProbeNamesWithEnv(envValue: string | undefined): string[] {
  const extra = (envValue ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)
    .map((name) => (name.startsWith('#') ? name.slice(1) : name));
  return [...new Set([...REGION_NAMES, ...extra])].sort();
}
