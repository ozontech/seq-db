import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;

export let options = {
  vus: 1,
  iterations: 5,
};

export default function () {
  const res = http.post(
    `${BASE_URL}/select/logsql/stats_query`,
    'query=* | stats by (status) min(size)',
    {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    }
  );

  console.log(res)

  check(res, { '200-ok': (r) => r.status === 200 });

  sleep(0.2);
}
