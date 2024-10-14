package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestTokenizeMany(t *testing.T) {
	tokenizer := NewTokenizer(6, false, TestMapping)
	root, _ := insaneJSON.DecodeString(`{"message":"/T1.T2_T3,t4.looooong_t5/readyz error* 5555-r2"}`)
	tokens := tokenizer.Do(root)

	assert.Equal(t, T("message:t1"), tokens[0])
	assert.Equal(t, T("message:t2_t3"), tokens[1])
	assert.Equal(t, T("message:t4"), tokens[2])
	assert.Equal(t, T("message:readyz"), tokens[3])
	assert.Equal(t, T("message:error*"), tokens[4])
	assert.Equal(t, T("message:5555"), tokens[5])
	assert.Equal(t, T("message:r2"), tokens[6])
	assert.Equal(t, T("_exists_:message"), tokens[7])
	assert.Equal(t, AllToken, tokens[8])
}

func check(t *testing.T, token string, cnt *int, tokens []Token) {
	assert.Equal(t, T(token), tokens[*cnt])
	*cnt++
}

func TestTokenizeTags(t *testing.T) {
	tokenizer := NewTokenizer(10, false, TestMapping)
	root, _ := insaneJSON.DecodeString(`{"tags":[{"key":"key1","value":"one"},{"key":"key2","value":"some string"}]}`)
	tokens := tokenizer.Do(root)

	cnt := 0
	check(t, "tags.key1:one", &cnt, tokens)
	check(t, "_exists_:tags.key1", &cnt, tokens)
	check(t, "tags.key2:some string", &cnt, tokens)
	check(t, "_exists_:tags.key2", &cnt, tokens)
	check(t, "_exists_:tags", &cnt, tokens)
	check(t, AllToken.HR(), &cnt, tokens)
}

func TestTokenizeProcess(t *testing.T) {
	tokenizer := NewTokenizer(10, false, TestMapping)
	root, _ := insaneJSON.DecodeString(`{"process": {"serviceName": "simple", "tags":[{"key":"key1","value":"one"},{"key":"key2","value":"some string"}]}}`)
	tokens := tokenizer.Do(root)

	cnt := 0
	check(t, "process.serviceName:simple", &cnt, tokens)
	check(t, "_exists_:process.serviceName", &cnt, tokens)
	check(t, "process.tags.key1:one", &cnt, tokens)
	check(t, "_exists_:process.tags.key1", &cnt, tokens)
	check(t, "process.tags.key2:some string", &cnt, tokens)
	check(t, "_exists_:process.tags.key2", &cnt, tokens)
	check(t, "_exists_:process.tags", &cnt, tokens)
	check(t, "_exists_:process", &cnt, tokens)

	check(t, AllToken.HR(), &cnt, tokens)
}

func TestTokenizeMixed(t *testing.T) {
	tokenizer := NewTokenizer(10, false, TestMapping)
	root, _ := insaneJSON.DecodeString(`{"text":"some kind of text","traceID":"some_id","tags":[{"key":"internal_key1","value":"one"},{"key":"internal_key2","value":"two three four"}]}`)
	tokens := tokenizer.Do(root)

	cnt := 0
	check(t, "text:some", &cnt, tokens)
	check(t, "text:kind", &cnt, tokens)
	check(t, "text:of", &cnt, tokens)
	check(t, "text:text", &cnt, tokens)
	check(t, "_exists_:text", &cnt, tokens)
	check(t, "traceID:some_id", &cnt, tokens)
	check(t, "_exists_:traceID", &cnt, tokens)
	check(t, "tags.internal_key1:one", &cnt, tokens)
	check(t, "_exists_:tags.internal_key1", &cnt, tokens)
	check(t, "tags.internal_key2:two three four", &cnt, tokens)
	check(t, "_exists_:tags.internal_key2", &cnt, tokens)
	check(t, "_exists_:tags", &cnt, tokens)
	check(t, AllToken.HR(), &cnt, tokens)
}

func TestTokenizeSpan(t *testing.T) {
	tokenizer := NewTokenizer(100, false, TestMapping)
	// real object from jaeger
	// we're using test mapping, so not all keys are processed
	root, _ := insaneJSON.DecodeString(`{"traceID":"410abbcfacacd815","spanID":"410abbcfacacd815","flags":1,"operationName":"/api/traces","references":[],"startTime":1624919754896566,"startTimeMillis":1624919754896,"duration":45582,"tags":[{"key":"sampler.type","type":"string","value":"const"},{"key":"sampler.param","type":"bool","value":"true"},{"key":"span.kind","type":"string","value":"server"},{"key":"http.method","type":"string","value":"GET"},{"key":"http.url","type":"string","value":"/api/traces?end=1624919754891000\u0026limit=20\u0026lookback=2d\u0026maxDuration=1s\u0026minDuration=1us\u0026service=cartservice\u0026start=1624746954891000\u0026tags=%7B%22http.method%22%3A%22GET%22%7D"},{"key":"component","type":"string","value":"net/http"},{"key":"http.status_code","type":"int64","value":"200"},{"key":"internal.span.format","type":"string","value":"proto"}],"logs":[],"process":{"serviceName":"jaeger-query","tags":[{"key":"jaeger.version","type":"string","value":"Go-2.29.1"},{"key":"hostname","type":"string","value":"leviska-ThinkBook-13s-G2-ITL"},{"key":"ip","type":"string","value":"192.168.1.138"},{"key":"client-uuid","type":"string","value":"4f9435f65a6142ce"}]}}`)
	tokens := tokenizer.Do(root)

	cnt := 0
	check(t, "traceID:410abbcfacacd815", &cnt, tokens)
	check(t, "_exists_:traceID", &cnt, tokens)
	check(t, "tags.sampler.type:const", &cnt, tokens)
	check(t, "_exists_:tags.sampler.type", &cnt, tokens)
	check(t, "tags.sampler.param:true", &cnt, tokens)
	check(t, "_exists_:tags.sampler.param", &cnt, tokens)
	check(t, "tags.span.kind:server", &cnt, tokens)
	check(t, "_exists_:tags.span.kind", &cnt, tokens)
	check(t, "tags.http.method:get", &cnt, tokens)
	check(t, "_exists_:tags.http.method", &cnt, tokens)
	check(t, "tags.http.url:/api/traces?end=1624919754891000&limit=20&lookback=2d&maxduration=1s&minduration=1us&service=cartservice&start=1624746954891000&tags=%7b%22http.method%22%3a%22get%22%7d", &cnt, tokens)
	check(t, "_exists_:tags.http.url", &cnt, tokens)
	check(t, "tags.component:net/http", &cnt, tokens)
	check(t, "_exists_:tags.component", &cnt, tokens)
	check(t, "tags.http.status_code:200", &cnt, tokens)
	check(t, "_exists_:tags.http.status_code", &cnt, tokens)
	check(t, "tags.internal.span.format:proto", &cnt, tokens)
	check(t, "_exists_:tags.internal.span.format", &cnt, tokens)
	check(t, "_exists_:tags", &cnt, tokens)
	check(t, "process.serviceName:jaeger-query", &cnt, tokens)
	check(t, "_exists_:process.serviceName", &cnt, tokens)
	check(t, "process.tags.jaeger.version:go-2.29.1", &cnt, tokens)
	check(t, "_exists_:process.tags.jaeger.version", &cnt, tokens)
	check(t, "process.tags.hostname:leviska-thinkbook-13s-g2-itl", &cnt, tokens)
	check(t, "_exists_:process.tags.hostname", &cnt, tokens)
	check(t, "process.tags.ip:192.168.1.138", &cnt, tokens)
	check(t, "_exists_:process.tags.ip", &cnt, tokens)
	check(t, "process.tags.client-uuid:4f9435f65a6142ce", &cnt, tokens)
	check(t, "_exists_:process.tags.client-uuid", &cnt, tokens)
	check(t, "_exists_:process.tags", &cnt, tokens)
	check(t, "_exists_:process", &cnt, tokens)
	check(t, AllToken.HR(), &cnt, tokens)
}

func TestTokenizeKeywordList(t *testing.T) {
	tokenizer := NewTokenizer(30, false, TestMapping)
	root, _ := insaneJSON.DecodeString(`{"traceID":"410abbcfacacd815", "operation_name": "operation1 operation2 operation1__service-name-2 operation4-service1 one-two-three"}`)
	tokens := tokenizer.Do(root)
	cnt := 0
	check(t, "traceID:410abbcfacacd815", &cnt, tokens)
	check(t, "_exists_:traceID", &cnt, tokens)
	check(t, "operation_name:operation1", &cnt, tokens)
	check(t, "operation_name:operation2", &cnt, tokens)
	check(t, "operation_name:operation1__service-name-2", &cnt, tokens)
	check(t, "operation_name:operation4-service1", &cnt, tokens)
	check(t, "operation_name:one-two-three", &cnt, tokens)
	check(t, "_exists_:operation_name", &cnt, tokens)
	check(t, "_all_:", &cnt, tokens)
}

func TestEmpty(t *testing.T) {
	tokenizer := NewTokenizer(6, false, TestMapping)
	root, _ := insaneJSON.DecodeString(`{"message":""}`)
	tokens := tokenizer.Do(root)

	assert.Equal(t, T("_exists_:message"), tokens[0])
	assert.Equal(t, T("_all_:"), tokens[1])
}
