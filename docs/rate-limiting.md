# Rate limiting requests

Obviously there is a need to rate limit some requests from users or other
services. Right now we use simple internal implementation of RateLimiter,
see [`network/ratelimiter.go`](../network/ratelimiter.go), it is enough
for current tasks. Following sections describe the use cases for
rate limiter.

## Rate limiting search queries

Because of bugs in UI or script automation there is a possibility of
repeating the same search query multiple times. Search query may create
a significant load on stores, and to evade useless work, search queries
are rate limited by stores. Two queries are considered identical if they
have same query string, aggregation and interval. This is implemented in
[`search_store.go`](../network/search_store.go).

## Rate limiting document fetching

There are 2 cases of document fetching, first is made after search query
found IDs and fetching is needed to return results to user. Second is
when document is directly requested from API on ingestor. Second way
is vulnerable to DDOS kind of attack, because fetching by ID is not
simple operation for now. So rate limiter is implemented to throttle
such requests by message ID. This is implemented in 
[`search_proxy.go`](../network/search_proxy.go).

## How to enable the rate limiter
The rate limiter can be enabled on launch using the `query-rate-limit` flag 
followed by a number -- the maximum number of queries allowed per second. 
The default value for this flag is `2.0`.