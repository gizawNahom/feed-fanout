// One load step: a fixed number of requests per second, mixed like a social app's traffic.
//
//   k6 run -e RATE=200 -e DURATION=3m /loadtest/feed.js   (loadtest/run.sh runs one per step)
//
// Open model: requests arrive at RATE per second whether or not earlier ones have finished,
// the way real users do. (A closed model, where each virtual user waits for its response
// before sending the next, quietly sends less when the system slows down, and hides it.)
// If k6 runs out of virtual users to keep the rate, it counts `dropped_iterations`: that
// means the system couldn't keep up, not that the test is broken.

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const API = __ENV.API || 'http://api:8000';
const RATE = Number(__ENV.RATE || 50);
const DURATION = __ENV.DURATION || '3m';
const USERS = Number(__ENV.USERS || 100000); // everyone in the seeded graph
const SKEW = Number(__ENV.SKEW || 2.13); // same as the seed: popular accounts get more follows

// Tokens for the active users (every 10th user of the graph), made by loadtest/setup.sh.
const tokens = new SharedArray('tokens', () => JSON.parse(open('/loadtest/tokens.json')));

const rateLimited = new Counter('posts_rate_limited');

// 429 is the posting limit doing its job, not a failure: counted on its own.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 399 }, 429));

const ENDPOINTS = ['GET /me/feed', 'POST /posts', 'PUT /me/following', 'DELETE /me/following'];

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(20, RATE),
      maxVUs: Math.max(100, RATE * 5), // enough for 5 s responses at this rate
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  // Thresholds that can't fail, one per endpoint: they make k6 report each endpoint's
  // latency separately in the summary. Judging the results is the report's job.
  thresholds: Object.fromEntries(ENDPOINTS.map((name) => [`http_req_duration{name:${name}}`, ['max>=0']])),
  discardResponseBodies: true,
};

function userId(i) {
  return 'u' + String(i).padStart(6, '0');
}

export default function () {
  const me = tokens[Math.floor(Math.random() * tokens.length)];
  const auth = { Authorization: `Bearer ${me.token}` };
  const roll = Math.random();

  if (roll < 0.9) {
    // 90%: read your feed (first page)
    const res = http.get(`${API}/me/feed?limit=20`, { headers: auth, tags: { name: 'GET /me/feed' } });
    check(res, { 'feed read': (r) => r.status === 200 });
  } else if (roll < 0.98) {
    // 8%: post
    const res = http.post(`${API}/posts`, JSON.stringify({ text: `load test post by ${me.user}` }), {
      headers: { ...auth, 'Content-Type': 'application/json' },
      tags: { name: 'POST /posts' },
    });
    if (res.status === 429) rateLimited.add(1);
    check(res, { 'post created': (r) => r.status === 201 || r.status === 429 });
  } else {
    // 2%: follow or unfollow someone, chosen like the seed (popular accounts more often)
    const target = userId(Math.floor(USERS * Math.pow(Math.random(), SKEW)));
    if (target === me.user) return;
    const follow = Math.random() < 0.5;
    const res = follow
      ? http.put(`${API}/me/following/${target}`, null, { headers: auth, tags: { name: 'PUT /me/following' } })
      : http.del(`${API}/me/following/${target}`, null, { headers: auth, tags: { name: 'DELETE /me/following' } });
    check(res, { 'follow changed': (r) => r.status === 204 });
  }
}
