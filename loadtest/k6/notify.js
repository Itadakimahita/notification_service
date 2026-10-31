import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8081';
const KEY = __ENV.API_KEY || 'demo-key';
const RATE = Number(__ENV.RATE || 2300);
const DURATION = __ENV.DURATION || '30m';
const PEAK = Number(__ENV.PEAK || 23000);

export const options = {
  scenarios: {
    sustained: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 500, maxVUs: 5000,
    },
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: RATE, timeUnit: '1s', startTime: __ENV.BURST_AT || '10m',
      preAllocatedVUs: 2000, maxVUs: 20000,
      stages: [
        { target: PEAK, duration: '1m' },
        { target: PEAK, duration: '5m' },
        { target: RATE, duration: '1m' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    'http_req_duration{expected_response:true}': ['p(95)<100'],
  },
};

export default function () {
  const marketing = Math.random() < Number(__ENV.MARKETING_PCT || 0) / 100;
  const body = JSON.stringify({
    user_id: `lt-${Math.floor(Math.random() * 100000)}`,
    template_id: 'loadtest',
    category: marketing ? 'marketing' : 'transactional',
    channels: ['push', 'inapp'],
    data: { n: String(__ITER) },
  });
  const res = http.post(`${BASE}/v1/notifications`, body, {
    headers: { 'Content-Type': 'application/json', Authorization: `ApiKey ${KEY}` },
  });
  check(res, { accepted: (r) => r.status === 202 });
}
