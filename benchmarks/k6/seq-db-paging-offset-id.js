import http from 'k6/http';
import { check, sleep } from 'k6';

/*
* seq-db has offset-id field which is similar to Elasticsearch's search_after field.
* To use offset-id, you query with offset id: null for the first page,
* then extract the document ID from the last document in the response and use it
* as offset_id in subsequent queries for efficient pagination.
*/

const BASE_URL = __ENV.BASE_URL;
const PAGE_SIZE = 100;
const TOTAL_PAGES = 50;

export let options = {
    vus: 20,
    duration: '10s',
};

const vuState = {};

export default function () {
    if (!vuState[__VU]) {
        vuState[__VU] = {
            offsetId: null,
            page: 0
        };
    }

    const state = vuState[__VU];

    if (state.page >= TOTAL_PAGES) {
        state.offsetId = null;
        state.page = 0;
    }

    const queryObj = {
        query: {
            query: "",
            from: "2000-01-01T00:00:00Z",
            to: "2050-01-01T00:00:00Z",
            explain: false,
        },
        order: "ORDER_ASC",
        size: PAGE_SIZE
    };

    if (state.offsetId !== null) {
        queryObj.offset_id = state.offsetId;
    }

    const query = JSON.stringify(queryObj);

    const res = http.post(
        `${BASE_URL}/complex-search`,
        query,
        { headers: { 'Content-Type': 'application/json' } }
    );

    check(res, {
        "200-ok": (res) => res.status == 200,
        "has-docs": (res) => {
            if (res.status === 200) {
                try {
                    const body = JSON.parse(res.body);
                    return body.docs && body.docs.length > 0;
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
            const docs = body.docs || [];
            if (docs.length > 0) {
                // Set offset id as the last doc id - move to the next page
                const lastDoc = docs[docs.length - 1];
                state.offsetId = lastDoc.id || null;
                state.page++;
            } else {
                // No more docs, reset page
                state.offsetId = null;
                state.page = 0;
            }
        } catch (e) {
            console.error('Failed to parse response:', e);
            return;
        }
    }

    sleep(0.2);
}

