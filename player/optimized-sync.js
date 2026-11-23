/** V2.9 unified TV sync policy. No UI dependency: also used by load tests. */
export const SYNC_INTERVAL_MS=60_000;
export const SYNC_JITTER_MS=10_000;
export const DEVICE_DB_WRITE_INTERVAL_MS=180_000;
export const PROGRESS_LEDGER_MS=300_000;
export function nextSyncDelay(random=Math.random){
  return SYNC_INTERVAL_MS+Math.floor(random()*SYNC_JITTER_MS);
}
export function selectSnapshot(state, video, content, manifest){
  if(state!=='playing'||!video||video.paused||!content?.id||!manifest?.revision)return null;
  const positionSec=Math.max(0,Math.floor(Number(video.currentTime)||0));
  return {contentId:content.id,manifestRevision:manifest.revision,positionSec};
}
/** Analytical model: not an actual 12-hour wall-clock soak test. */
export function simulateFleet({devices=100,hours=12,secondsPerSync=60,secondsPerDbWrite=180}={}){
 const duration=hours*3600,requestsPerTv=Math.ceil(duration/secondsPerSync),statusWritesPerTv=Math.ceil(duration/secondsPerDbWrite);
 return {devices,hours,syncRequests:devices*requestsPerTv,deviceStatusWrites:devices*statusWritesPerTv,requestsPerTv,statusWritesPerTv};
}
