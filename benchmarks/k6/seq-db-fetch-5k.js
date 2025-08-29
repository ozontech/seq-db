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
  const from = page * PAGE_SIZE;

  const query = JSON.stringify({
    query: {
      query: "size:[2020645 to *]",
      from: "2000-01-01T00:00:00Z",
      to: "2050-01-01T00:00:00Z",
      explain: false,
    },
    order: "ORDER_ASC",
    size: PAGE_SIZE,
    offset: from
  });

  const res = http.post(
    `${BASE_URL}/complex-search`,
    query,
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(res, { "200-ok": (res) => res.status == 200});

  sleep(0.2);
}
