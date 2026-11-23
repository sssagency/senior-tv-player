/** 시니어교육TV V2.6: 사용자에게 API 주소를 입력받지 않습니다.
 * 이 파일은 배포 시 운영 담당자가 한 번만 설정합니다.
 * Cloudflare Pages > Player 저장소의 player/config.js 에서 아래 값 수정.
 * 로컬 실행 (localhost/127.0.0.1)은 자동으로 로컬 Worker 8787 연결.
 */
const PRODUCTION_WORKERS_URL = 'https://YOUR-SENIOR-TV-API.workers.dev';
const IS_LOCAL = ['localhost', '127.0.0.1'].includes(location.hostname);
export const DEFAULT_API_URL = IS_LOCAL
  ? 'http://127.0.0.1:8787'
  : PRODUCTION_WORKERS_URL.replace(/\/+$/, '');
