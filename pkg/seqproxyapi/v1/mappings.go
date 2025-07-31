package seqproxyapi

import (
	"fmt"

	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/seq"
)

var funcMappings = []AggFunc{
	seq.AggFuncCount:    AggFunc_AGG_FUNC_COUNT,
	seq.AggFuncSum:      AggFunc_AGG_FUNC_SUM,
	seq.AggFuncMin:      AggFunc_AGG_FUNC_MIN,
	seq.AggFuncMax:      AggFunc_AGG_FUNC_MAX,
	seq.AggFuncAvg:      AggFunc_AGG_FUNC_AVG,
	seq.AggFuncQuantile: AggFunc_AGG_FUNC_QUANTILE,
	seq.AggFuncUnique:   AggFunc_AGG_FUNC_UNIQUE,
}

var funcMappingsPb = func() []seq.AggFunc {
	mappings := make([]seq.AggFunc, len(funcMappings))
	for from, to := range funcMappings {
		mappings[to] = seq.AggFunc(from)
	}
	return mappings
}()

func (f AggFunc) ToAggFunc() (seq.AggFunc, error) {
	if int(f) >= len(funcMappingsPb) || f < 0 {
		return 0, fmt.Errorf("unknown function")
	}
	return funcMappingsPb[f], nil
}

func (f AggFunc) MustAggFunc() seq.AggFunc {
	aggFunc, err := f.ToAggFunc()
	if err != nil {
		panic(err)
	}
	return aggFunc
}

var orderMappings = []Order{
	seq.DocsOrderAsc:  Order_ORDER_ASC,
	seq.DocsOrderDesc: Order_ORDER_DESC,
}

var orderMappingsPb = func() []seq.DocsOrder {
	mappings := make([]seq.DocsOrder, len(orderMappings))
	for from, to := range orderMappings {
		mappings[to] = seq.DocsOrder(from)
	}
	return mappings
}()

func (o Order) ToDocsOrder() (seq.DocsOrder, error) {
	if int(o) >= len(orderMappingsPb) {
		return 0, fmt.Errorf("unknown order")
	}
	return orderMappingsPb[o], nil
}

func (o Order) MustDocsOrder() seq.DocsOrder {
	order, err := o.ToDocsOrder()
	if err != nil {
		panic(err)
	}
	return order
}

var statusMappings = []AsyncSearchStatus{
	fracmanager.AsyncSearchStatusDone:       AsyncSearchStatus_AsyncSearchStatusDone,
	fracmanager.AsyncSearchStatusInProgress: AsyncSearchStatus_AsyncSearchStatusInProgress,
	fracmanager.AsyncSearchStatusError:      AsyncSearchStatus_AsyncSearchStatusError,
	fracmanager.AsyncSearchStatusCanceled:   AsyncSearchStatus_AsyncSearchStatusCanceled,
}

var statusMappingsPb = func() []fracmanager.AsyncSearchStatus {
	mappings := make([]fracmanager.AsyncSearchStatus, len(statusMappings))
	for from, to := range statusMappings {
		mappings[to] = fracmanager.AsyncSearchStatus(from)
	}
	return mappings
}()

func (s AsyncSearchStatus) ToAsyncSearchStatus() (fracmanager.AsyncSearchStatus, error) {
	if int(s) >= len(statusMappingsPb) {
		return 0, fmt.Errorf("unknown status")
	}
	return statusMappingsPb[s], nil
}

func (s AsyncSearchStatus) MustAsyncSearchStatus() fracmanager.AsyncSearchStatus {
	v, err := s.ToAsyncSearchStatus()
	if err != nil {
		panic(err)
	}
	return v
}

func ToProtoAsyncSearchStatus(s fracmanager.AsyncSearchStatus) (AsyncSearchStatus, error) {
	if int(s) >= len(statusMappings) {
		return 0, fmt.Errorf("unknown status")
	}
	return statusMappings[s], nil
}

func MustProtoAsyncSearchStatus(s fracmanager.AsyncSearchStatus) AsyncSearchStatus {
	v, err := ToProtoAsyncSearchStatus(s)
	if err != nil {
		panic(err)
	}
	return v
}

var asyncSearchStatusFromString = map[string]AsyncSearchStatus{
	"AsyncSearchStatusDone":       AsyncSearchStatus_AsyncSearchStatusDone,
	"AsyncSearchStatusInProgress": AsyncSearchStatus_AsyncSearchStatusInProgress,
	"AsyncSearchStatusError":      AsyncSearchStatus_AsyncSearchStatusError,
	"AsyncSearchStatusCanceled":   AsyncSearchStatus_AsyncSearchStatusCanceled,
}

func AsyncSearchStatusFromString(s string) (AsyncSearchStatus, error) {
	if res, ok := asyncSearchStatusFromString[s]; ok {
		return res, nil
	}

	return 0, fmt.Errorf("unknown status")
}
