import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;

export let options = {
  vus: 1,
  iterations: 5,
};

export default function () {
  const query = JSON.stringify({
    size: 0,
    aggs: {
      "by_status": {
        "terms": {
          "field": "status",
          "size": 1000
        },
        "aggs": {
          "min_request_time": {
            "min": {
              "field": "size"
            }
          }
        }
      }
    }
  });

  const res = http.post(
    `${BASE_URL}/logs/_search?request_cache=false`,
    query,
    { headers: { 'Content-Type': 'application/json' } }
  );

  console.log(res.status);
  check(res, { "200-ok": (res) => res.status == 200});

  sleep(0.2);
}
