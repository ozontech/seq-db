import http from 'k6/http';
import { sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;

export let options = {
  vus: 2,
  iterations: 10,
};

export default function () {
  const query = JSON.stringify({
    size: 0,
    aggs: {
      name: {
        terms: {
          field: "status"
        }
      }
    }
  });

  const res = http.post(
    `${BASE_URL}/logs/_search?request_cache=false`,  
    query,
    { headers: { 'Content-Type': 'application/json' } }
  );

  sleep(0.2);
}