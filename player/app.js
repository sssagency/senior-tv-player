import {DEFAULT_API_URL} from './config.js';
import {SYNC_INTERVAL_MS, PROGRESS_LEDGER_MS, selectSnapshot, nextSyncDelay} from './optimized-sync.js';
const $=s=>document.querySelector(s),esc=s=>String(s??'').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x]));
const KEY='senior-tv-device-v2',MANIFEST='senior-tv-manifest-v2',SOUNDKEY='senior-tv-sound-muted-v24';
const PENDING_KEY='senior-tv-pending-v25',SCHEDULE_KEY='senior-tv-weekly-v28';
const categoryNames=['건강관리','시니어 안전','건강운동','키오스크','스마트폰','두뇌활동','생활정보','복지정보'];
const demoLists=[{id:'demo-1',title:'매일 건강운동 (시범)',category:'건강운동'},{id:'demo-2',title:'키오스크 기초 (시범)',category:'키오스크'},{id:'demo-3',title:'시니어 안전 (시범)',category:'시니어 안전'}];
const demoVideos=[{id:'demo-video',title:'시범 교육영상 · 전문가 검수 영상 아님',category:'건강운동'},{id:'demo-video-2',title:'생활 안전 시범 영상',category:'시니어 안전'}];
const demoManifest=id=>({revision:id,playlist:demoLists.find(x=>x.id===id),items:[{id:'demo-video',title:'시니어교육TV 시범 화면 · 전문가 검수 영상 아님',category:'시범 영상',url:'./sample.mp4',assetKey:'demo-sample',durationSec:12}],expiresAt:new Date(Date.now()+86400000).toISOString()});
function read(k){try{return JSON.parse(localStorage.getItem(k))}catch{return null}};
const s={api:DEFAULT_API_URL,pair:read(KEY),manifest:read(MANIFEST),screen:'boot',demo:new URLSearchParams(location.search).has('demo'),code:null,session:null,started:0,pairTimer:null,syncTimer:null,manifestTimer:null,catalog:[],videos:[],category:'',mode:'playlist',home:true,cursor:0,index:0,inSync:false,syncAgain:false,video:null,blobUrl:null,playback:'starting',mute:localStorage.getItem(SOUNDKEY)==='1',online:navigator.onLine,institution:'시니어교육TV',message:'',cached:{done:0,total:0},reported:'',info:false,loadSeq:0,watchdog:null,playStartAt:0,lastAdvanceAt:0,lastPosition:-1,recoveries:0,recovering:false,cacheStates:{},cacheLoading:false,cacheProgress:0,pendingConnection:null,mediaErrors:0,consecutiveFailures:0,rightsCheckedAt:read(MANIFEST)?.rightsCheckedAt||null,baseManifest:read(MANIFEST),schedule:read(SCHEDULE_KEY),scheduleActiveKey:null,activeSlotId:null,activeScheduleSlot:null,scheduleOverrideKey:null,scheduleFinishedKey:null,clockOffsetMs:0,scheduleTimer:null,scheduleCaching:false};

// V2.7: reliable playback ledger independent of TV heartbeat/status.
// Each event has a stable ID. IndexedDB queue survives lost internet and page reloads.
const telemetry={session:null,lastVideoTime:-1,lastTickAt:0,playedSec:0,lastQueuedAt:0,seq:0,flushing:false,selectionSource:'unknown',sessionSource:'unknown',content:null};
function telemetryId(){return crypto.randomUUID?crypto.randomUUID():String(Date.now())+'-'+Array.from(crypto.getRandomValues(new Uint8Array(10))).map(n=>n.toString(16).padStart(2,'0')).join('')}
async function telemetryStoreOpen(){return new Promise((resolve,reject)=>{if(!('indexedDB' in window))return reject(new Error('IndexedDB unavailable'));const q=indexedDB.open('senior-tv-playback-v27',2);q.onupgradeneeded=()=>{const store=q.result.objectStoreNames.contains('queue')?q.transaction.objectStore('queue'):q.result.createObjectStore('queue',{keyPath:'eventId'});if(!store.indexNames.contains('by_device'))store.createIndex('by_device','deviceId',{unique:false})};q.onsuccess=()=>resolve(q.result);q.onerror=()=>reject(q.error)})}
async function telemetrySave(e){const db=await telemetryStoreOpen();return new Promise((resolve,reject)=>{const tx=db.transaction('queue','readwrite');tx.objectStore('queue').put(e);tx.oncomplete=()=>{db.close();resolve()};tx.onerror=()=>{db.close();reject(tx.error)}})}
async function telemetryRead(deviceId){const db=await telemetryStoreOpen();return new Promise((resolve,reject)=>{const tx=db.transaction('queue','readonly'),req=tx.objectStore('queue').index('by_device').getAll(deviceId);tx.oncomplete=()=>{db.close();resolve((req.result||[]).sort((a,b)=>a.occurredAt.localeCompare(b.occurredAt)||a.seq-b.seq).slice(0,50))};tx.onerror=()=>{db.close();reject(tx.error)}})}
async function telemetryRemove(ids){if(!ids.length)return;const db=await telemetryStoreOpen();return new Promise((resolve,reject)=>{const tx=db.transaction('queue','readwrite');for(const id of ids)tx.objectStore('queue').delete(id);tx.oncomplete=()=>{db.close();resolve()};tx.onerror=()=>{db.close();reject(tx.error)}})}
function telemetryTrackTime(){if(!telemetry.session||!s.video||s.video.paused||s.video.seeking||s.playback!=='playing')return;const nowTick=Date.now(),pos=s.video.currentTime;if(!Number.isFinite(pos))return;
 if(telemetry.lastVideoTime>=0){const delta=pos-telemetry.lastVideoTime,wall=(nowTick-telemetry.lastTickAt)/1000;if(delta>0&&delta<=5&&wall>0&&wall<=8)telemetry.playedSec+=Math.min(delta,wall*1.35)}telemetry.lastVideoTime=pos;telemetry.lastTickAt=nowTick;
}
function telemetryQueue(type,detail=''){
 if(s.demo||!s.pair||!telemetry.session)return;
 const c=telemetry.content;if(!c)return;
 const e={eventId:telemetryId(),sessionId:telemetry.session,deviceId:s.pair.deviceId,contentId:c.id,playlistId:telemetry.content.playlistId?.startsWith('single:')?null:(telemetry.content.playlistId||null),durationSec:Math.max(0,Math.floor(c.durationSec||s.video?.duration||0)),manifestRevision:s.manifest?.revision||'',source:telemetry.sessionSource,seq:++telemetry.seq,type,occurredAt:new Date().toISOString(),positionSec:Math.max(0,Math.floor(s.video?.currentTime||0)),playedSec:Math.max(0,Math.floor(telemetry.playedSec)),online:s.online,errorCode:detail.slice(0,60),detail:detail.slice(0,300),...(telemetry.sessionSource==='scheduled'&&s.activeScheduleSlot?{scheduleId:s.schedule?.id,scheduleVersion:s.schedule?.version,scheduleSlotId:s.activeScheduleSlot.id,scheduledStart:s.activeScheduleSlot.scheduledStart}: {})};
 // Queue write is durable before attempting network transmission.
 telemetrySave(e).then(()=>{/* queued; transmitted with the next unified sync */}).catch(x=>console.warn('재생 로그 보관 실패',x));
}
function telemetryStart(){if(telemetry.session||s.demo||!s.pair||!current())return;telemetry.session=telemetryId();telemetry.sessionSource=s.activeSlotId?'scheduled':telemetry.selectionSource;telemetry.content={...current(),playlistId:s.manifest?.playlist?.id||null};telemetry.playedSec=0;telemetry.seq=0;telemetry.lastVideoTime=-1;telemetry.lastTickAt=0;telemetry.lastQueuedAt=Date.now();telemetryQueue('start')}
function telemetryFinish(type='stop',reason=''){if(!telemetry.session)return;telemetryTrackTime();telemetryQueue(type,reason);telemetry.session=null;telemetry.content=null;telemetry.lastVideoTime=-1;telemetry.lastTickAt=0}
function telemetryProgress(){if(!telemetry.session||!s.video||s.video.paused)return;telemetryTrackTime();if(Date.now()-telemetry.lastQueuedAt>=PROGRESS_LEDGER_MS){telemetry.lastQueuedAt=Date.now();telemetryQueue('progress')}}

const roots=()=>s.api.trim().replace(/\/+$/,'');
async function api(path,method='GET',body,auth=true){
 if(!/^https?:\/\//.test(roots()))throw new Error('서비스 서버가 설정되지 않았습니다. 운영 담당자에게 문의해주세요.');
 const h={'Content-Type':'application/json'};
 if(auth&&s.pair){h['X-Device-ID']=s.pair.deviceId;h['X-Device-Key']=s.pair.deviceKey;}
 const res=await fetch(roots()+'/api'+path,{method,headers:h,body:body===undefined?undefined:JSON.stringify(body)});
 const j=await res.json().catch(()=>({error:'서비스 응답을 읽지 못했습니다.'}));
 if(!res.ok||!j.ok)throw new Error(j.error||`서버 오류 ${res.status}`);return j;
}
const TV_ART={
 '플레이리스트':'<rect x="17" y="18" width="44" height="48" rx="8" fill="#358AC6"/><path d="M28 32h22 M28 43h22 M28 54h16" stroke="white" stroke-width="4" stroke-linecap="round"/><circle cx="58" cy="58" r="13" fill="#F4BF50"/><path d="M55 51l11 7-11 7z" fill="#183655"/>',
 '건강관리':'<path d="M40 70L17 48C-3 27 25 11 40 33c15-22 43-6 23 15z" fill="#F28C99"/><path d="M14 47h15l6-10 8 19 7-9h16" stroke="white" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" fill="none"/>',
 '시니어 안전':'<path d="M40 12l26 11v20c0 18-15 28-26 33-11-5-26-15-26-33V23z" fill="#F4C86E"/><path d="M26 43l9 9 19-22" stroke="#193652" stroke-width="6" fill="none" stroke-linecap="round" stroke-linejoin="round"/>',
 '건강운동':'<circle cx="43" cy="17" r="9" fill="#90D5AB"/><path d="M42 29l-10 18 19 5 11 20 M42 29l12 14 14 1 M32 47l-14 17" stroke="#90D5AB" stroke-width="9" fill="none" stroke-linecap="round" stroke-linejoin="round"/>',
 '키오스크':'<rect x="14" y="12" width="52" height="54" rx="7" fill="#9DA5F7"/><rect x="21" y="20" width="38" height="30" rx="4" fill="#273A65"/><rect x="29" y="56" width="22" height="4" rx="2" fill="#FFFFFF"/><path d="M34 35h13 M41 28v14" stroke="#F7C65C" stroke-width="4" stroke-linecap="round"/>',
 '스마트폰':'<rect x="24" y="9" width="33" height="62" rx="8" fill="#83D5E9"/><rect x="29" y="17" width="23" height="40" rx="3" fill="#21446A"/><circle cx="40" cy="64" r="3" fill="#FFFFFF"/><path d="M33 35h14 M40 28v14" stroke="#F4BE51" stroke-width="4" stroke-linecap="round"/>',
 '두뇌활동':'<path d="M42 17c-9-13-28-4-25 10-9 6-8 19 2 22-2 14 17 20 23 8 6 12 24 6 23-8 11-5 10-18 1-23 2-14-18-22-24-9z" fill="#C6A4EF"/><path d="M42 18v40 M27 28l11 7-11 9 M57 28l-12 7 11 10" stroke="#684A99" stroke-width="3" fill="none"/>',
 '생활정보':'<path d="M13 17c11-5 20-5 27 0 7-5 16-5 27 0v47c-11-5-20-5-27 0-7-5-16-5-27 0z" fill="#F5B58F"/><path d="M40 17v47 M20 30h13 M20 40h12 M47 30h13 M47 40h13" stroke="#B86A44" stroke-width="3" fill="none"/>',
 '복지정보':'<rect x="13" y="26" width="54" height="45" rx="6" fill="#9FD8BC"/><path d="M25 25v-9h30v9 M40 34v25 M28 46h24" stroke="#225A59" stroke-width="5" stroke-linecap="round"/><circle cx="62" cy="20" r="12" fill="#F4C86E"/>'
};
function art(name){const content=TV_ART[name]||TV_ART['플레이리스트'];return `<svg class="tile-art" viewBox="0 0 80 80" aria-hidden="true" focusable="false">${content}</svg>`;}
function tvBrand(light=false){return `<div class="tv-brand"><span class="tv-brand-mark"><img src="./brand-symbol.png" alt="시니어스포츠산업진흥원"/></span><span><strong>시니어교육TV</strong><small>시니어스포츠산업진흥원</small></span></div>`;}
function logo(){return `<div class="logo"><span class="setup-mark"><img src="./brand-symbol.png" alt="시니어스포츠산업진흥원"/></span><span>시니어교육TV</span></div>`}
function setMessage(x){s.message=x||'';const el=$('#message');if(el)el.textContent=s.message;}
function showTrial(){
 s.demo=true;s.catalog=demoLists;s.videos=demoVideos;s.manifest=demoManifest('demo-1');
 s.institution='시범 교육TV';s.home=true;startPlayback();
}
function connectionStatus(text,failed=false){
 const el=$('#server-status');if(!el)return;
 el.textContent=text;el.classList.toggle('error',failed);
}
async function checkServiceConnection(){
 connectionStatus('서비스 연결 확인 중…');
 try{
  // 6초 후에도 응답이 없으면 오류 안내. 현장 직원은 주소를 직접 입력하지 않습니다.
  const controller=new AbortController();const id=setTimeout(()=>controller.abort(),6000);
  let res;
  try{res=await fetch(roots()+'/api/health',{signal:controller.signal,cache:'no-store'});}finally{clearTimeout(id)}
  if(!res.ok)throw new Error('연결 실패');
  connectionStatus('● 서비스 연결 정상');
 }catch(e){connectionStatus('서비스 연결에 실패했습니다. 인터넷을 확인한 뒤 다시 시도해주세요.',true);}
}
function beginPair(){
 clearInterval(s.approvalTimer);s.screen='pairing';s.pendingConnection=null;
 $('#player').innerHTML=`<div class="screen setup-screen"><div class="setup-box">${logo()}<div class="setup-eyebrow">TV 연결 등록</div><h1>TV 번호를 입력해 주세요.</h1><p>CMS에서 발급된 번호를 입력하고, 기관 정보를 확인합니다.<br/>최종 연결은 CMS 관리자 승인 후 완료됩니다.</p><label for="tv-number-input">CMS TV 연결번호</label><input id="tv-number-input" autocomplete="off" spellcheck="false" maxlength="12" placeholder="SSS01" style="text-transform:uppercase"/><div id="confirm-target"></div><div class="setup-actions"><button class="btn wide" id="claim-device">기관 정보 확인하기</button><button class="btn alt wide" id="check-server">연결 상태 다시 확인</button></div><div class="setup-service-status" id="server-status" role="status">서비스 연결 확인 중…</div><div class="statusline" id="message" role="alert"></div></div></div>`;
 $('#check-server').onclick=()=>checkServiceConnection();
 checkServiceConnection();
 $('#claim-device').onclick=async()=>{
  const number=$('#tv-number-input').value.trim().toUpperCase().replace(/\s/g,''),button=$('#claim-device');
  if(!/^SSS[0-9]{2,}$/.test(number))return setMessage('SSS01 형태의 연결번호를 입력해 주세요.');
  button.disabled=true;setMessage('연결 가능한 TV를 찾고 있습니다.');
  try{const r=await api('/device/activation/preview','POST',{tvNumber:number},false);
   s.pendingConnection={tvNumber:number,...r};setMessage('');
   $('#confirm-target').innerHTML=`<div class="confirm-card"><b>이 TV가 맞나요?</b><div class="pair-summary"><div><small>소속 기관</small><strong>${esc(r.institutionName)}</strong></div><div><small>TV 번호</small><strong>${esc(r.tvNumber)}</strong></div><div><small>기기 이름</small><strong>${esc(r.deviceName)}</strong></div></div><button class="btn wide" id="confirm-device">맞습니다 · 연결 승인 요청</button><button class="btn alt wide" id="cancel-device">다른 번호 입력</button></div>`;
   $('#cancel-device').onclick=()=>{$('#confirm-target').innerHTML='';s.pendingConnection=null};
   $('#confirm-device').onclick=async()=>{const b=$('#confirm-device');b.disabled=true;
    try{const result=await api('/device/activation/request','POST',{tvNumber:number},false);
     const pending={requestId:result.requestId,requestToken:result.requestToken,tvNumber:number,institutionName:result.institutionName,deviceName:result.deviceName};
     localStorage.setItem(PENDING_KEY,JSON.stringify(pending));showApprovalWait(pending);
    }catch(ex){setMessage('요청 실패: '+ex.message);b.disabled=false}
   };
  }catch(e){setMessage('번호 확인 실패: '+e.message)}finally{button.disabled=false}
 };
 const pending=read(PENDING_KEY);if(pending?.requestId&&pending?.requestToken)showApprovalWait(pending);
}
function showApprovalWait(pending){
 clearInterval(s.approvalTimer);
 const host=$('#confirm-target');if(!host)return;
 $('#claim-device').style.display='none';$('#tv-number-input').value=pending.tvNumber;$('#tv-number-input').disabled=true;
 host.innerHTML=`<div class="confirm-card approval-wait"><span class="wait-spinner" aria-hidden="true"></span><b>관리자 승인을 기다리고 있습니다.</b><p><strong>${esc(pending.institutionName||'')}</strong> · ${esc(pending.tvNumber||'')}<br/>CMS → TV 기기 관리 → 연결 승인 요청에서 승인해 주세요.</p><div id="approval-status" role="status">서버에서 승인 상태를 확인하고 있습니다.</div><button class="btn alt wide" id="cancel-approval">연결 요청 취소하고 다시 입력</button></div>`;
 $('#cancel-approval').onclick=()=>{clearInterval(s.approvalTimer);localStorage.removeItem(PENDING_KEY);beginPair()};
 let busy=false;
 const poll=async()=>{
  if(busy||s.screen!=='pairing')return;busy=true;
  try{const r=await api('/device/activation/status','POST',{requestId:pending.requestId,requestToken:pending.requestToken},false);
   if(r.status==='approved'){
    clearInterval(s.approvalTimer);localStorage.removeItem(PENDING_KEY);
    s.pair={deviceId:r.deviceId,deviceKey:r.deviceKey,tvNumber:r.tvNumber};localStorage.setItem(KEY,JSON.stringify(s.pair));s.manifest=null;localStorage.removeItem(MANIFEST);s.home=true;
    startPlayback();await sync(true);return;
   }
   if(r.status==='rejected'||r.status==='expired'){
    clearInterval(s.approvalTimer);localStorage.removeItem(PENDING_KEY);
    host.innerHTML=`<div class="confirm-card"><b>${r.status==='expired'?'연결 요청 시간이 지났습니다.':'연결 요청이 거절되었습니다.'}</b><p>다시 요청하려면 아래 버튼을 눌러주세요.</p><button class="btn wide" id="pair-retry">다시 연결하기</button></div>`;
    $('#pair-retry').onclick=beginPair;return;
   }
   const el=$('#approval-status');if(el)el.textContent='CMS 승인 대기 중 · 자동 확인 중';
  }catch(e){const el=$('#approval-status');if(el)el.textContent='서버 연결을 확인하고 있습니다: '+e.message}
  finally{busy=false}
 };
 poll();s.approvalTimer=setInterval(poll,3500);
}

function current(){return s.manifest?.items?.[s.index]||null}
function categories(){return ['플레이리스트',...categoryNames]}
function startPlayback(){
 s.screen='playing';
 $('#player').innerHTML=`<div class="screen playback"><video class="mainvideo" id="mainvideo" playsinline autoplay preload="auto"></video><div class="gradient"></div><div class="topline"><div class="brand">${tvBrand()}</div><div class="inst" id="inst">${esc(s.institution)}</div></div><button type="button" class="clickarea" id="open-overlay" aria-label="교육 메뉴"></button><div class="bottomline"><div><div class="course" id="course">교육을 준비하고 있습니다</div><div class="title" id="title"></div></div><div class="right"><span class="indicator" id="connection">연결 확인 중</span><button class="toolbutton" id="toggle-sound">🔇 소리 켜기</button><button class="toolbutton" id="show-cats">확인 버튼 · 교육 선택</button></div></div><div id="message-box"></div><div id="preparation" class="preparation" hidden></div><div id="overlay-container"></div><div id="diag-container"></div></div>`;
 s.video=$('#mainvideo');s.video.muted=s.mute;s.reported='';s.lastAdvanceAt=Date.now();s.playStartAt=Date.now();
  clearInterval(s.watchdog);s.watchdog=setInterval(checkPlaybackHealth,2000);clearInterval(s.scheduleTimer);s.scheduleTimer=setInterval(scheduleTick,5000);
  s.video.addEventListener('timeupdate',()=>{if(s.video.currentTime>s.lastPosition+0.2){s.lastAdvanceAt=Date.now();s.lastPosition=s.video.currentTime;}telemetryTrackTime();});
  s.video.addEventListener('loadedmetadata',()=>{s.playStartAt=Date.now();});
 s.video.onended=()=>nextVideo('complete');s.video.onerror=()=>{if(s.recovering)return;recoverPlayback('decoder error')};
 s.video.onwaiting=()=>{s.playback='buffering';report('buffering','network buffering');telemetryQueue('buffering','network buffering')};
  s.video.oncanplay=()=>{if(s.playback==='buffering'){s.playback='starting';}};
 s.video.onplaying=()=>{s.playback='playing';s.consecutiveFailures=0;s.lastAdvanceAt=Date.now();s.recoveries=0;s.recovering=false;clearPreparation();const c=current();if(!c||s.demo)return;
   const capturedRevision=s.manifest?.revision;const k=capturedRevision+':'+c.id;
   if(s.reported===k)return;
   const confirm=()=>{if(s.reported===k)return;s.reported=k;telemetryStart();report('play_started','first frame displayed',{contentId:c.id,revision:Number(String(capturedRevision).split('.')[0])||0,manifestRevision:capturedRevision,positionSec:Math.floor(s.video?.currentTime||0),...(s.activeSlotId?{scheduleId:s.schedule.id,scheduleVersion:s.schedule.version,scheduleSlotId:s.activeSlotId}:{})})};
   if(typeof s.video.requestVideoFrameCallback==='function')s.video.requestVideoFrameCallback(()=>confirm());else setTimeout(()=>{if(s.video?.readyState>=2)confirm();},450);
 };
 $('#open-overlay').onclick=showCategories;$('#show-cats').onclick=showCategories;$('#toggle-sound').onclick=()=>toggleSound();
 updateSoundButton();renderCaption();if(s.home)showCategories();else playCurrent();
 if(!s.demo){report('audio','startup',{muted:s.video.muted});refreshCatalog();clearTimeout(s.syncTimer);clearTimeout(s.manifestTimer);s.manifestTimer=null;
  // A single polling stream replaces the former 45s manifest + 20s progress/report streams.
  const loop=async()=>{await sync(false);if(s.pair&&!s.demo)s.syncTimer=setTimeout(loop,nextSyncDelay());};
  s.syncTimer=setTimeout(loop,nextSyncDelay());}
}
function renderCaption(){if(!$('#course'))return;const item=current();$('#course').textContent=s.manifest?.playlist?.title||'방송 대기';$('#title').textContent=item?.title||'편성된 영상이 없습니다.';$('#inst').textContent=s.institution;$('#connection').textContent=s.demo?'체험 모드':s.online?'● 서버 연결':'● 저장된 방송';}
// Local-time scheduling works during short internet outages using the last authorized weekly manifest.
function scheduleLocalParts(t=Date.now()+s.clockOffsetMs){const date=new Date(t+9*3600000);return {weekday:(date.getUTCDay()+6)%7,minute:date.getUTCHours()*60+date.getUTCMinutes(),ymd:date.toISOString().slice(0,10)};}
function scheduleCurrent(){if(!s.schedule?.id||!s.schedule?.slots)return null;const date=scheduleLocalParts();const slot=s.schedule.slots.find(x=>x.weekday===date.weekday&&x.startMinute<=date.minute&&date.minute<x.endMinute);return slot?{slot,key:`${s.schedule.revision}.${slot.id}`,day:date}:null;}
function scheduleTick(){if(s.demo||!s.pair||s.screen==='pairing'||s.screen==='setup')return;
 const active=scheduleCurrent();const key=active?.key||null;
 if(s.scheduleActiveKey&&s.scheduleActiveKey!==key){
  if(s.activeSlotId){telemetryFinish('stop','예약된 방송 종료');s.video?.pause();s.activeSlotId=null;s.activeScheduleSlot=null;s.home=true;showCategories();}
  s.scheduleActiveKey=null;s.scheduleOverrideKey=null;s.scheduleFinishedKey=null;
 }
 if(!active||s.scheduleOverrideKey===key||s.scheduleFinishedKey===key||s.scheduleActiveKey===key)return;
 if(!active.slot.items?.length){s.scheduleActiveKey=key;setNotice('예약된 교육은 현재 준비되어 있지 않습니다. 다른 교육을 선택할 수 있습니다.');return;}
 if(!s.online&&!offlineRightsValidSchedule()){setNotice('예약 교육 권한 확인 기간이 지났습니다. 인터넷을 연결해주세요.');return;}
 const slot=active.slot;
 s.scheduleActiveKey=key;s.activeSlotId=slot.id;s.activeScheduleSlot={...slot,scheduledStart:`${active.day.ymd}T${String(Math.floor(slot.startMinute/60)).padStart(2,'0')}:${String(slot.startMinute%60).padStart(2,'0')}:00+09:00`};
 const policy=s.schedule.revision.split('.p').pop();
 s.manifest={revision:`sch:${s.schedule.id}:${s.schedule.version}:${slot.id}:p${policy}`,playlist:{id:slot.playlistId,title:slot.playlistTitle,category:'예약 교육'},items:slot.items,rightsCheckedAt:s.schedule.rightsCheckedAt,offlineRightsMaxHours:24,expiresAt:s.schedule.expiresAt};
 s.index=0;s.home=false;hideOverlay();renderCaption();setNotice('예약된 교육이 시작됩니다.');playCurrent();
}
function offlineRightsValidSchedule(){const v=Date.parse(s.schedule?.rightsCheckedAt||'');return Number.isFinite(v)&&Date.now()-v<=24*3600e3;}
async function cacheWeeklySchedule(schedule){if(!schedule?.id||s.scheduleCaching)return;s.scheduleCaching=true;
 try{const slots=(schedule.slots||[]),map=new Map();for(const slot of slots)for(const v of slot.items||[])if(v.assetKey&&!map.has(v.assetKey))map.set(v.assetKey,v);
  const values=[...map.values()].slice(0,50);let total=0;
  for(const item of values){if(s.schedule?.revision!==schedule.revision)break;if(item.size>MAX_VIDEO_BYTES||total+Number(item.size||0)>550*1024*1024)continue;total+=Number(item.size||0);
   try{await ensureVideoCached(item,values.map(x=>x.assetKey));}catch(e){console.warn('예약 영상 저장 실패',e.message)}
  }
 }finally{s.scheduleCaching=false;}
}
function setNotice(msg){s.message=msg;const holder=$('#message-box');if(holder)holder.innerHTML=msg?`<div class="notice">${esc(msg)}</div>`:'';if(msg)setTimeout(()=>{if(s.message===msg){s.message='';const node=$('#message-box');if(node)node.innerHTML=''}},8500)}
async function report(eventType,detail='',extra={}){if(s.demo||!s.pair)return;try{await api('/device/event','POST',{eventType,detail,...extra})}catch(e){console.warn('device report',e.message)}}
function revExpired(){return !s.manifest?.expiresAt||new Date(s.manifest.expiresAt).getTime()-Date.now()<40*60000}
async function sync(force=false){if(s.demo||!s.pair)return;if(s.inSync){s.syncAgain=true;return;}s.inSync=true;
 try{
  // Snapshot and durable offline events share one authenticated Workers request.
  if(s.video&&!s.video.paused&&s.playback==='playing')telemetryProgress();
  let pending=[];try{pending=await telemetryRead(s.pair.deviceId)}catch(e){console.warn('재생 기록 임시 저장소',e.message)}
  const snapshot=selectSnapshot(s.playback,s.video,current(),s.manifest);
  const r=await api('/device/sync','POST',{knownRevision:s.baseManifest?.revision||'',refreshUrls:force||revExpired(),knownScheduleRevision:s.schedule?.revision||'',refreshScheduleUrls:force||!s.schedule||Date.parse(s.schedule.expiresAt||0)-Date.now()<40*60000,playbackEvents:pending.map(({deviceId,...rest})=>rest),playbackSnapshot:snapshot});
  const ack=r.playbackAck||{};
  const discarded=[...(ack.accepted||[]),...(ack.rejected||[]).filter(x=>!x.retryable).map(x=>x.eventId)];
  if(discarded.length)await telemetryRemove(discarded);
  if((ack.rejected||[]).length)console.warn('서버에서 재생 이벤트 일부 거부',ack.rejected);
  s.institution=r.institutionName||'기관';telemetry.selectionSource=r.selectionSource||telemetry.selectionSource;if(s.pair&&r.tvNumber&&s.pair.tvNumber!==r.tvNumber){s.pair.tvNumber=r.tvNumber;localStorage.setItem(KEY,JSON.stringify(s.pair));}s.online=true;if(r.serverTime){const diff=Date.parse(r.serverTime)-Date.now();if(Number.isFinite(diff))s.clockOffsetMs=diff;}
  if(r.schedule&&!r.schedule.unchanged){s.schedule=r.schedule;s.scheduleFinishedKey=null;localStorage.setItem(SCHEDULE_KEY,JSON.stringify(r.schedule));if(r.schedule.id){api('/device/schedule/ack','POST',{scheduleId:r.schedule.id,version:r.schedule.version,revision:r.schedule.revision}).catch(()=>{});cacheWeeklySchedule(r.schedule).catch(e=>console.warn('schedule prefetch',e.message));}}
  if(r.manifest&&!r.manifest.unchanged){
   const old=s.manifest,oldItem=current(),different=s.baseManifest?.revision!==r.manifest.revision;
   s.baseManifest=r.manifest;localStorage.setItem(MANIFEST,JSON.stringify(r.manifest));
   if(s.activeSlotId&&!s.scheduleOverrideKey){ // Keep locally scheduled playback active; still refresh the background CMS playlist.
     if(r.selectionSource==='cms'&&s.baseManifest&&String(s.baseManifest.revision||'').split('.')[0]!==String(r.manifest.revision||'').split('.')[0]){s.scheduleOverrideKey=s.scheduleActiveKey;s.activeSlotId=null;s.activeScheduleSlot=null;}
     else {scheduleTick();return;}
   }
   s.manifest=r.manifest;s.rightsCheckedAt=r.manifest.rightsCheckedAt||new Date().toISOString();
   if(different)s.index=0;
   for(const removed of (old?.items||[]).filter(x=>!r.manifest.items?.some(y=>y.assetKey===x.assetKey)))if(removed.assetKey)cacheDelete(removed.assetKey).catch(()=>{});
   if(different)refreshCatalog();
   renderCaption();
   const blockedOrRemoved=!!oldItem&&!r.manifest.items?.some(x=>x.id===oldItem.id);
   if(blockedOrRemoved){telemetryFinish('stop','content removed');s.video?.pause();s.video?.removeAttribute('src');s.video?.load();setNotice('이 교육은 더 이상 송출할 수 없습니다. 다른 교육을 선택해주세요.');s.home=true;showCategories();}
   else if(r.selectionSource==='cms'&&different&&r.manifest.items?.length){s.home=false;hideOverlay();}
   if(!s.home&&(different||!s.video?.src))playCurrent();
   cacheManifest(r.manifest).catch(e=>console.warn('cache',e));
  }else if(s.baseManifest){s.rightsCheckedAt=new Date().toISOString();s.baseManifest.rightsCheckedAt=s.rightsCheckedAt;localStorage.setItem(MANIFEST,JSON.stringify(s.baseManifest));}
  scheduleTick();renderCaption();
 }catch(e){if(/인증|접근정보|연결번호|등록되지 않은/.test(e.message)){localStorage.removeItem(KEY);localStorage.removeItem(MANIFEST);localStorage.removeItem(PENDING_KEY);s.pair=null;s.manifest=null;s.video?.pause();clearTimeout(s.syncTimer);clearTimeout(s.manifestTimer);beginPair();setMessage('기기 연결이 해제되었습니다. 새 TV 연결번호를 입력해주세요.');return;}
  s.online=false;renderCaption();setNotice('서버에 연결되지 않았습니다. 유효한 저장 영상이 있다면 계속 재생합니다.');if(!offlineRightsValid()){s.video?.pause();setNotice('오프라인 이용 권한 확인 기한이 지나 교육을 일시 중지합니다. 인터넷을 연결해주세요.');s.home=true;showCategories();}}
 finally{s.inSync=false;if(s.syncAgain){s.syncAgain=false;sync(true)}}}
async function refreshCatalog(){if(s.demo)return;s.catalog=s.catalog||[];try{const r=await api('/device/catalog');s.catalog=r.playlists||[];s.videos=r.videos||[];if(['categories','catalog'].includes(s.screen))renderOverlay();}catch(e){console.warn('catalog',e.message)}}
function playlistSelect(p){if(s.scheduleActiveKey){s.scheduleOverrideKey=s.scheduleActiveKey;s.activeSlotId=null;s.activeScheduleSlot=null;}s.home=false;if(s.demo){s.manifest=demoManifest(p.id);s.index=0;hideOverlay();playCurrent();return;}
 setNotice('방송을 변경하고 있습니다...');api('/device/select','POST',{playlistId:p.id}).then(()=>sync(true)).then(()=>{hideOverlay();setNotice('교육을 변경했습니다.')}).catch(e=>{setNotice('방송 변경 실패: '+e.message);});}
function videoSelect(c){if(s.scheduleActiveKey){s.scheduleOverrideKey=s.scheduleActiveKey;s.activeSlotId=null;s.activeScheduleSlot=null;}s.home=false;if(s.demo){s.manifest=demoManifest('demo-1');s.index=0;hideOverlay();playCurrent();return;}
 setNotice('선택한 교육을 준비하고 있습니다...');api('/device/select-content','POST',{contentId:c.id}).then(()=>sync(true)).then(()=>{hideOverlay();setNotice('선택한 교육을 재생합니다.')}).catch(e=>setNotice('영상 변경 실패: '+e.message));}
function showCategories(){s.screen='categories';s.cursor=0;report('menu','교육 선택 화면');refreshCatalog();renderOverlay()}
function currentOptions(){return s.screen==='categories'?categories():s.mode==='playlist'?s.catalog:s.videos.filter(x=>x.category===s.category)}
function renderOverlay(){const host=$('#overlay-container');if(!host)return;if(s.screen==='playing'){host.innerHTML='';return;}
 const isHome=s.screen==='categories',opts=currentOptions();s.cursor=Math.min(s.cursor,Math.max(0,opts.length-1));
 const title=isHome?'어떤 교육을 보시겠어요?':s.category==='플레이리스트'?'우리 기관 플레이리스트':`${s.category} 교육`;
 const hint=isHome?'원하시는 교육을 리모컨으로 선택해 주세요.':s.mode==='playlist'?'기관에서 시청할 수 있는 교육 묶음입니다.':'교수 검수를 마친 교육 영상만 표시됩니다.';
 host.innerHTML=`<div class="overlay ${isHome?'home-menu':'catalog-menu'}"><div class="tv-bg-circle circle-a"></div><div class="tv-bg-circle circle-b"></div><header class="tv-menu-header">${tvBrand()}<div class="tv-menu-right"><div class="tv-site"><span class="tv-site-dot"></span><span>${esc(s.institution)}</span><small>${esc(s.pair?.tvNumber||'시범 TV')}</small></div><button class="tv-close" id="overlay-close" aria-label="교육 목록 닫기">×</button></div></header><main class="tv-menu-content"><div class="tv-eyebrow">${isHome?'교육 분야 선택':'교육 목록'}</div><h1 class="headline">${esc(title)}</h1><p class="hint">${esc(hint)}</p><div role="list" class="${isHome?'cats':'catalog'}">${opts.map((c,i)=>{
  const label=typeof c==='string'?c:c.title;const cat=typeof c==='string'?c:c.category||'플레이리스트';
  const artName=isHome?cat:(s.mode==='playlist'?cat:s.category);
  return `<button type="button" role="listitem" aria-label="${esc(label)}" aria-current="${i===s.cursor?'true':'false'}" data-choice="${i}" class="${isHome?'cat':'playlist-card'} ${i===s.cursor?'selected':''}">${isHome?`<span class="cat-illustration">${art(artName)}</span><span class="cat-label">${esc(label)}</span>`:`<span class="catalog-art">${art(artName)}</span><span class="catalog-info"><b>${esc(label)}</b><small>${s.mode==='playlist'?'교육 플레이리스트':esc(cat)}</small></span><span class="catalog-arrow">›</span>`}</button>`;
 }).join('')||'<div class="empty">현재 이용 가능한 교육이 없습니다.<small>CMS에서 교육영상을 등록한 후 다시 확인해 주세요.</small></div>'}</div></main><footer class="tv-menu-footer"><div class="remote-help"><span><span class="key">↑ ↓ ← →</span> 이동</span><span><span class="key">OK</span> 선택</span><span><span class="key">↩</span> 뒤로</span></div><span class="tv-footer-status"><i></i>${s.demo?'체험 모드':s.online?'서버 연결 정상':'오프라인'}</span></footer></div>`;
 host.querySelectorAll('[data-choice]').forEach(b=>{b.onclick=()=>choose(Number(b.dataset.choice));b.onmouseenter=()=>{s.cursor=Number(b.dataset.choice);syncFocus()};b.onfocus=()=>{s.cursor=Number(b.dataset.choice);syncFocus()}});
 $('#overlay-close')?.addEventListener('click',()=>isHome?(!s.home?hideOverlay():null):back());
}
function syncFocus(){document.querySelectorAll('#overlay-container [data-choice]').forEach((e,i)=>{e.classList.toggle('selected',i===s.cursor);e.setAttribute('aria-current',i===s.cursor?'true':'false')});}

function choose(n){const opts=currentOptions(),c=opts[n];if(!c)return;if(s.screen==='categories'){s.category=c;s.mode=c==='플레이리스트'?'playlist':'video';s.screen='catalog';s.cursor=0;renderOverlay();}else if(s.mode==='playlist')playlistSelect(c);else videoSelect(c);}
function back(){if(s.screen==='catalog'){s.screen='categories';s.cursor=0;renderOverlay()}else if(s.screen==='categories'){if(!s.home)hideOverlay()}else showCategories()}
function hideOverlay(){s.screen='playing';renderOverlay()}
function nextVideo(why='skip'){
 telemetryFinish(why);const items=s.manifest?.items||[];if(!items.length)return;
 if(s.activeSlotId&&s.index>=items.length-1&&s.activeScheduleSlot?.finishAction!=='repeat'){
   s.scheduleFinishedKey=s.scheduleActiveKey;s.video?.pause();s.activeSlotId=null;s.activeScheduleSlot=null;s.home=true;showCategories();setNotice('예약된 교육을 모두 마쳤습니다. 다음 교육 시간까지 기다립니다.');return;
 }
 s.index=(s.index+1)%items.length;s.reported='';playCurrent();}

const MAX_VIDEO_BYTES=280*1024*1024,MAX_CACHE_BYTES=650*1024*1024;
function offlineRightsValid(){const checked=Date.parse(s.rightsCheckedAt||s.manifest?.rightsCheckedAt||'');return Number.isFinite(checked)&&Date.now()-checked<=24*3600e3;}
function cacheOpen(){return new Promise((resolve,reject)=>{if(!('indexedDB' in window))return reject(new Error('브라우저 저장 기능 미지원'));const req=indexedDB.open('senior-tv-media-v2',2);req.onupgradeneeded=()=>{if(!req.result.objectStoreNames.contains('files'))req.result.createObjectStore('files');if(!req.result.objectStoreNames.contains('meta'))req.result.createObjectStore('meta')};req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error)})}
async function cacheQuery(key){const db=await cacheOpen();return new Promise((res,rej)=>{const t=db.transaction(['files','meta'],'readonly'),a=t.objectStore('files').get(key),b=t.objectStore('meta').get(key);t.oncomplete=()=>{db.close();res({blob:a.result||null,meta:b.result||null})};t.onerror=()=>{db.close();rej(t.error)}})}
async function cached(key){return (await cacheQuery(key)).blob}
async function cachePut(key,blob,meta){const db=await cacheOpen();return new Promise((res,rej)=>{const t=db.transaction(['files','meta'],'readwrite');t.objectStore('files').put(blob,key);t.objectStore('meta').put({...meta,bytes:blob.size,usedAt:Date.now()},key);t.oncomplete=()=>{db.close();res(true)};t.onerror=()=>{db.close();rej(t.error)}})}
async function cacheDelete(key){const db=await cacheOpen();return new Promise((res,rej)=>{const tx=db.transaction(['files','meta'],'readwrite');tx.objectStore('files').delete(key);tx.objectStore('meta').delete(key);tx.oncomplete=()=>{db.close();res(true)};tx.onerror=()=>{db.close();rej(tx.error)}})}
async function cacheEntries(){const db=await cacheOpen();return new Promise((res,rej)=>{const t=db.transaction('meta','readonly'),r=t.objectStore('meta').getAllKeys(),vals=t.objectStore('meta').getAll();t.oncomplete=()=>{db.close();res((r.result||[]).map((key,i)=>({key,...(vals.result||[])[i]})))};t.onerror=()=>{db.close();rej(t.error)}})}
async function verifyBlob(blob,item){if(!blob)return false;if(item.size&&blob.size!==item.size)return false;
 if(item.sha256){const ab=await blob.arrayBuffer();const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',ab))].map(v=>v.toString(16).padStart(2,'0')).join('');return hash===item.sha256;}
 return true; // >64MiB uploads without server SHA are checked by exact size only.
}
async function cacheGetValidated(item){if(!item?.assetKey)return null;const row=await cacheQuery(item.assetKey);if(!row.blob)return null;
 if(row.meta?.sha256===item.sha256&&row.meta?.verified&&(!item.size||row.blob.size===item.size))return row.blob;
 const valid=await verifyBlob(row.blob,item);if(!valid){await cacheDelete(item.assetKey);s.cacheStates[item.id]='failed';return null;}
 await cachePut(item.assetKey,row.blob,{sha256:item.sha256||'',verified:true});s.cacheStates[item.id]='ready';return row.blob;
}
async function trimCache(needed,protectedKeys=[]){let entries=await cacheEntries();let total=entries.reduce((sum,e)=>sum+Number(e.bytes||0),0);const protect=new Set(protectedKeys);
 for(const e of entries.sort((a,b)=>(a.usedAt||0)-(b.usedAt||0))){if(total+needed<=MAX_CACHE_BYTES)break;if(protect.has(e.key)||e.key===current()?.assetKey)continue;await cacheDelete(e.key);total-=Number(e.bytes||0);}
 return total+needed<=MAX_CACHE_BYTES;
}
function setPreparation(text,progress=null){const el=$('#preparation');if(!el)return;el.hidden=false;el.innerHTML=`<strong>${esc(text)}</strong>${progress!==null?`<div class="preparation-track"><span style="width:${Math.round(progress)}%"></span></div><small>${Math.round(progress)}% · 다운로드 중</small>`:''}`;}
function clearPreparation(){const el=$('#preparation');if(el){el.hidden=true;el.innerHTML='';}}
let lastCacheStatusAt=0;function reportCacheState(force=false){if(!force&&Date.now()-lastCacheStatusAt<180000&&s.cached.done<s.cached.total)return;lastCacheStatusAt=Date.now();report('cache_state','cache updates',{ready:s.cached.done,total:s.cached.total});}
// One in-flight R2 download per asset, even when schedule prefetch and normal playlist
// prefetch happen at the same time. Successful verified assets are reused on every loop.
const mediaCacheInFlight=new Map();
async function ensureVideoCached(item,protectedKeys=[]){
 if(!item?.assetKey)return false;
 if(mediaCacheInFlight.has(item.assetKey))return mediaCacheInFlight.get(item.assetKey);
 const action=(async()=>{
  if(await cacheGetValidated(item))return true;
  if(item.size&&item.size>MAX_VIDEO_BYTES)throw new Error('로컬 저장 허용 크기 초과');
  if(!await trimCache(item.size||MAX_VIDEO_BYTES,protectedKeys))throw new Error('로컬 저장공간 부족');
  const response=await fetch(item.url,{cache:'no-store'});if(!response.ok)throw new Error('영상 다운로드 HTTP '+response.status);
  const max=Number(response.headers.get('content-length')||item.size||0);let loaded=0,chunks=[];
  if(response.body){const reader=response.body.getReader();while(true){const {done,value}=await reader.read();if(done)break;loaded+=value.length;if(loaded>MAX_VIDEO_BYTES)throw new Error('허용 용량 초과');chunks.push(value);if(current()?.id===item.id&&max)setPreparation('선택한 교육영상을 다운로드하고 있습니다.',Math.min(99,loaded/max*100));}}
  else{const buffer=await response.arrayBuffer();loaded=buffer.byteLength;chunks=[new Uint8Array(buffer)];}
  const blob=new Blob(chunks,{type:'video/mp4'});chunks=[];
  if(current()?.id===item.id)setPreparation('영상 파일을 확인하고 있습니다.');
  if(!await verifyBlob(blob,item))throw new Error('영상 크기 또는 SHA-256 검증 실패');
  await cachePut(item.assetKey,blob,{sha256:item.sha256||'',verified:true});
  if(current()?.id===item.id)clearPreparation();
  return true;
 })();
 mediaCacheInFlight.set(item.assetKey,action);
 try{return await action;}finally{mediaCacheInFlight.delete(item.assetKey)}
}
async function cacheManifest(m){if(s.demo||s.cacheLoading)return;s.cacheLoading=true;const items=(m.items||[]).filter(x=>x.assetKey);s.cached={total:items.length,done:0};
 try{for(const x of items){if(s.baseManifest?.revision!==m.revision)break;
  try{s.cacheStates[x.id]='downloading';if(await ensureVideoCached(x,items.map(v=>v.assetKey))){s.cacheStates[x.id]='ready';s.cached.done++;reportCacheState();}}
  catch(e){s.cacheStates[x.id]='failed';console.warn('video cache',e.message);report('cache_error',x.id+' '+e.message);}}
 }finally{s.cacheLoading=false;reportCacheState(true);if(s.baseManifest?.revision!==m.revision&&s.baseManifest)queueMicrotask(()=>cacheManifest(s.baseManifest));}
}

function updateSoundButton(){const b=$('#toggle-sound');if(b)b.textContent=s.mute?'🔇 소리 켜기':'🔊 소리 끄기';}
function toggleSound(force){s.mute=typeof force==='boolean'?force:!s.mute;localStorage.setItem(SOUNDKEY,s.mute?'1':'0');if(s.video){s.video.muted=s.mute;if(!s.mute&&s.video.paused)s.video.play().catch(()=>setNotice('TV 확인 버튼을 눌러 음성 재생을 허용해주세요.'));}updateSoundButton();report('audio','sound preference',{muted:s.mute});}
function checkPlaybackHealth(){if(s.demo||s.screen!=='playing'||!current()||!s.video||s.recovering)return;
 if(s.video.ended||s.video.seeking)return;
 const elapsed=Date.now()-s.playStartAt,stalled=Date.now()-s.lastAdvanceAt;
 if(s.playback==='starting'&&elapsed>12000)return recoverPlayback('first frame timeout');
 if(s.playback==='buffering'&&stalled>15000)return recoverPlayback('buffering timeout');
 if(s.playback==='playing'&&!s.video.paused&&stalled>20000)return recoverPlayback('playback progress stalled');
}
async function recoverPlayback(reason){if(s.recovering||!current())return;s.recovering=true;s.playback='recovering';telemetryQueue('recovering',reason);s.recoveries++;setNotice(`재생을 복구하고 있습니다. (${s.recoveries}/2)`);report('recovering',reason,{contentId:current()?.id});
 const token=s.loadSeq;try{if(s.recoveries<=2){const at=Math.max(0,s.video?.currentTime||0);s.video?.pause();s.video?.load();if(s.video){s.video.currentTime=at;await s.video.play();}s.playStartAt=Date.now();s.lastAdvanceAt=Date.now();s.playback='starting';report('recovered','retry started',{contentId:current()?.id});telemetryQueue('recovered','retry started');}
 else{report('play_error',reason,{contentId:current()?.id});telemetryQueue('error',reason);s.recoveries=0;s.consecutiveFailures++;if(s.consecutiveFailures>=(s.manifest?.items?.length||1)){s.video?.pause();s.home=true;showCategories();setNotice('재생할 수 있는 영상이 없습니다. 본부에서 장비 상태를 확인해주세요.');}else nextVideo();}}
 catch(e){if(token===s.loadSeq){s.playback='error';report('play_error','recovery error '+e.message);setTimeout(()=>{s.recovering=false;recoverPlayback('recovery retry')},1800)}}finally{if(token===s.loadSeq)s.recovering=false;}
}
async function playCurrent(){if(!s.video)return;telemetryFinish('stop','changed video');const c=current();const seq=++s.loadSeq;renderCaption();clearPreparation();if(s.blobUrl){URL.revokeObjectURL(s.blobUrl);s.blobUrl=null;}
 s.video.pause();s.video.removeAttribute('src');s.video.load();if(!c)return;
 if(!s.demo&&!s.online&&!(s.activeSlotId?offlineRightsValidSchedule():offlineRightsValid())){s.home=true;showCategories();setNotice('오프라인 이용 권한 확인 기한이 지나 재생을 중지했습니다.');return;}
 let url=c.url;
 if(!s.demo&&c.assetKey){try{
   let blob=await cacheGetValidated(c);
   if(!blob&&s.online&&(!c.size||c.size<=MAX_VIDEO_BYTES)){
     setPreparation('교육 영상을 먼저 저장하고 있습니다.');
     // Share the same in-flight download as weekly/playlist prefetch.
     // This prevents the first video streaming from R2 while another fetch stores it.
     try{await ensureVideoCached(c,(s.manifest?.items||[]).map(v=>v.assetKey));blob=await cacheGetValidated(c);}catch(e){console.warn('영상 저장 실패, 온라인 재생을 시도합니다',e.message)}
   }
   if(blob){s.blobUrl=URL.createObjectURL(blob);url=s.blobUrl;}
   else if(!s.online){setNotice('인터넷이 없고 검증된 저장 영상도 없습니다.');return;}
   else setPreparation('온라인 영상을 불러오고 있습니다.');
  }catch(e){console.warn('영상 캐시 읽기 실패',e.message)}}
 if(seq!==s.loadSeq||c!==current())return;s.video.src=url;s.video.muted=s.mute;s.playback='starting';s.reported='';s.playStartAt=Date.now();s.lastAdvanceAt=Date.now();s.lastPosition=-1;s.recoveries=0;
 try{await s.video.play();}catch(e){if(!s.mute){s.mute=true;s.video.muted=true;updateSoundButton();report('audio','autoplay unmuted blocked; needs user action',{muted:true});setNotice('TV가 소리 있는 자동재생을 제한했습니다. 소리 켜기를 눌러주세요.');try{await s.video.play()}catch{setNotice('리모컨 확인 버튼을 눌러 교육을 재생해주세요.');}}
 else setNotice('재생이 차단되었습니다. 리모컨 확인 버튼을 눌러주세요.');}
}
document.addEventListener('keydown',e=>{if(s.screen==='setup'||s.screen==='pairing')return;
 if(['INPUT','TEXTAREA'].includes(document.activeElement?.tagName))return;
 const isOk=['Enter','NumpadEnter',' ','OK'].includes(e.key),arrows=['ArrowUp','ArrowDown','ArrowLeft','ArrowRight'];
 if(isOk){e.preventDefault();if(s.screen==='playing')showCategories();else choose(s.cursor);return;}
 if(arrows.includes(e.key)&&s.screen!=='playing'){
  e.preventDefault();const len=currentOptions().length;if(!len)return;
  const cols=s.screen==='categories'?(matchMedia('(max-width:760px)').matches?2:matchMedia('(max-width:1100px)').matches?4:5):2,step=e.key==='ArrowUp'?-cols:e.key==='ArrowDown'?cols:e.key==='ArrowLeft'?-1:1;
  s.cursor=Math.max(0,Math.min(len-1,s.cursor+step));renderOverlay();return;
 }
 if(['Escape','Backspace','BrowserBack'].includes(e.key)){e.preventDefault();back();return;}
 if(e.key.toLowerCase()==='m'&&s.video)toggleSound();
 if(e.key.toLowerCase()==='i'){s.info=!s.info;showDiagnostics()}
});
function showDiagnostics(){const host=$('#diag-container');if(!host)return;if(!s.info){host.innerHTML='';return;}
 host.innerHTML=`<div class="diagnostic"><b>TV 진단 정보</b><div>기관: ${esc(s.institution)}</div><div>TV 번호: ${esc(s.pair?.tvNumber||'시범')}</div><div>기기: ${esc(s.pair?.deviceId||'시범')}</div><div>API: ${esc(roots())}</div><div>연결: ${s.online?'정상':'오프라인'}</div><div>재생: ${esc(s.playback)}</div><div>편성 버전: ${esc(s.manifest?.revision||'-')}</div><div>파일 저장: ${s.cached.done}/${s.cached.total}</div><div>음성: ${s.video?.muted?'음소거':'재생'}</div><div>이번 영상 재생 시간: ${Math.floor(telemetry.playedSec)}초</div><div>오프라인 권한: ${offlineRightsValid()?'유효':'만료/확인 안 됨'}</div><button class="toolbutton" id="do-sync">지금 동기화</button> <button class="toolbutton" id="close-diag">닫기</button><div style="margin-top:8px"><button class="toolbutton" id="disconnect">기기 연결 해제</button></div></div>`;
 $('#do-sync').onclick=()=>sync(true);$('#close-diag').onclick=()=>{s.info=false;showDiagnostics()};$('#disconnect').onclick=()=>{if(confirm('이 TV를 새 기관으로 연결할까요?')){localStorage.removeItem(KEY);localStorage.removeItem(MANIFEST);localStorage.removeItem(PENDING_KEY);s.pair=null;s.manifest=null;s.info=false;clearTimeout(s.syncTimer);clearTimeout(s.manifestTimer);beginPair()}};
}
window.addEventListener('online',()=>{s.online=true;renderCaption();sync(true);scheduleTick()});
window.addEventListener('offline',async()=>{
 s.online=false;renderCaption();const c=current();if(s.demo||!c?.assetKey||!s.video)return;if(!offlineRightsValid()){s.video.pause();setNotice('오프라인 이용 허용시간이 지났습니다. 인터넷에 연결해주세요.');return;}
 try{const blob=await cacheGetValidated(c);if(!blob||c!==current())return;
  const previousTime=s.video.currentTime||0;s.video.pause();if(s.blobUrl)URL.revokeObjectURL(s.blobUrl);
  s.blobUrl=URL.createObjectURL(blob);s.video.src=s.blobUrl;s.video.currentTime=previousTime;await s.video.play();
  setNotice('인터넷이 끊겨 저장된 영상으로 재생을 이어갑니다.');
 }catch(e){console.warn('offline switch',e.message)}
});
if('serviceWorker' in navigator)navigator.serviceWorker.register('./sw.js').catch(()=>{});
if(s.demo){s.catalog=demoLists;s.videos=demoVideos;s.manifest=demoManifest('demo-1');s.institution='시범 교육TV';s.home=true;startPlayback();}
else if(s.pair){startPlayback();scheduleTick();sync(true);}
else beginPair();
