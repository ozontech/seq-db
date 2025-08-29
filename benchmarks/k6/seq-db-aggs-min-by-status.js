import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;


export let options = {
  vus: 1,
  iterations: 5,
};

export default function () {
  const query = JSON.stringify({
    "query": {
      "query": "",
      "from": "2000-01-01T00:00:00Z",
      "to": "2050-01-01T00:00:00Z",
      "explain": false
    },
    "aggs": [{
      "field": "size",
      "group_by": "status",
      "func": "AGG_FUNC_MIN"
    }],
    "size": 0,
    "offset": 0,
    "with_total": false

  });

  const res = http.post(
    `${BASE_URL}/complex-search`,
    query,
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, { "200-ok": (res) => res.status == 200});

  sleep(0.2);
}
