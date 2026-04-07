import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL;
const PAGE_SIZE = 100;
const TOTAL_PAGES = 50;

export let options = {
  vus: 20,
  duration: '10s',
};

export default function () {
  const page = __ITER % TOTAL_PAGES;
  const offset = page * PAGE_SIZE;
  const payload = `query=size :> 2020645 | sort by (_time) asc&limit=${PAGE_SIZE}&offset=${offset}`;

  const res = http.post(
    `${BASE_URL}/select/logsql/query`,
    payload,
    {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    }
  );

  check(res, { '200-ok': (r) => r.status === 200 });

  sleep(0.2);
}
