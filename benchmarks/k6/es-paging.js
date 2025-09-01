import http   from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;
const PAGE_SIZE   = 100;
const TOTAL_PAGES = 50; 

export let options = {
  vus:        20, 
  duration:   '10s',  
};

export default function () {
  const page = __ITER % TOTAL_PAGES;
  const from = page * PAGE_SIZE;

  const query = JSON.stringify({
    track_total_hits: false,
    query: { match_all: {} },  
    size:  PAGE_SIZE,
    from:  from
  });

  const res = http.post(
    `${BASE_URL}/logs/_search?request_cache=false`, 
    query,
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, { "200-ok": (res) => res.status == 200});

  sleep(0.2);
}
