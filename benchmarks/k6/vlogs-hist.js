import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;

export let options = {
  vus: 2,
  iterations: 10,
};

export default function () {
  const res = http.post(
    `${BASE_URL}/select/logsql/hits`,
    'query=*&start=7d&end=now&step=1m',
    {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    }
  );

  check(res, { '200-ok': (r) => r.status === 200 });

  sleep(0.2);
}
