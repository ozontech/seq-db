import http   from 'k6/http';
import { check, sleep } from 'k6';

/*
* Elasticsearch has search_after field which can be used same way as seq-db's offset-id field. In order to use
* the field, one must create a PIT (point in time) and then specify _shard_doc as sort field while querying.
* Elasticsearch will provide sort field in response hits which is a number that can be used for
* efficient scrolling on the next query.
*/

const BASE_URL = __ENV.BASE_URL;
const PAGE_SIZE   = 100;
const TOTAL_PAGES = 50;

export let options = {
    vus:        20,
    duration:   '10s',
};

let pitId = null;

export function setup() {
    const pitRes = http.post(
        `${BASE_URL}/logs-index/_pit?keep_alive=1m`,
        null,
        { headers: { 'Content-Type': 'application/json' } }
    );

    if (pitRes.status === 200) {
        try {
            const pitBody = JSON.parse(pitRes.body);
            pitId = pitBody.id;
            console.log(`PIT created successfully: ${pitId}`);
            return { pitId: pitBody.id };
        } catch (e) {
            console.error('Failed to parse PIT response:', e);
            return { pitId: null };
        }
    } else {
        console.error(`Failed to create PIT: ${pitRes.status} - ${pitRes.body}`);
        return { pitId: null };
    }
}

const vuState = {};

export default function (data) {
    if (!vuState[__VU]) {
        vuState[__VU] = {
            searchAfter: null,
            pageCount: 0
        };
    }

    const state = vuState[__VU];
    const pitId = data?.pitId || pitId;

    if (!pitId) {
        console.error('PIT ID not available, skipping iteration');
        return;
    }

    if (state.pageCount >= TOTAL_PAGES) {
        state.searchAfter = null;
        state.pageCount = 0;
    }

    const queryObj = {
        track_total_hits: false,
        query: { match_all: {} },
        pit: {
            id: pitId,
            keep_alive: '2m'
        },
        size: PAGE_SIZE,
        sort: [
            { _shard_doc: 'asc' }
        ]
    };

    if (state.searchAfter !== null) {
        queryObj.search_after = state.searchAfter;
    }

    const res = http.post(
        `${BASE_URL}/_search?request_cache=false`,
        JSON.stringify(queryObj),
        { headers: { 'Content-Type': 'application/json' } }
    );

    check(res, {
        "200-ok": (res) => res.status == 200,
        "has-hits": (res) => {
            if (res.status === 200) {
                try {
                    const body = JSON.parse(res.body);
                    return body.hits && body.hits.hits && body.hits.hits.length > 0;
                } catch (e) {
                    return false;
                }
            }
            return false;
        }
    });

    if (res.status === 200) {
        try {
            const body = JSON.parse(res.body);
            const hits = body.hits?.hits || [];
            if (hits.length > 0) {
                const lastHit = hits[hits.length - 1];
                state.searchAfter = lastHit.sort || null;
                state.pageCount++;
            } else {
                state.searchAfter = null;
                state.pageCount = 0;
            }
        } catch (e) {
            console.error(e)
            return;
        }
    }

    sleep(0.2);
}
