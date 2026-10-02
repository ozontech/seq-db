package storeapi

import (
	"fmt"

	"github.com/ozontech/seq-db/asyncsearcher"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

var funcMappings = []AggFunc{
	seq.AggFuncCount:       AggFunc_AGG_FUNC_COUNT,
	seq.AggFuncSum:         AggFunc_AGG_FUNC_SUM,
	seq.AggFuncMin:         AggFunc_AGG_FUNC_MIN,
	seq.AggFuncMax:         AggFunc_AGG_FUNC_MAX,
	seq.AggFuncAvg:         AggFunc_AGG_FUNC_AVG,
	seq.AggFuncQuantile:    AggFunc_AGG_FUNC_QUANTILE,
	seq.AggFuncUnique:      AggFunc_AGG_FUNC_UNIQUE,
	seq.AggFuncUniqueCount: AggFunc_AGG_FUNC_UNIQUE_COUNT,
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
	if int(f) >= len(funcMappingsPb) || f < 0 {
		panic("unknown function")
	}
	return funcMappingsPb[f]
}

func ToProtoAggFunc(f seq.AggFunc) (AggFunc, error) {
	if int(f) >= len(funcMappings) {
		return 0, fmt.Errorf("unknown order")
	}
	return funcMappings[f], nil
}

func MustProtoAggFunc(f seq.AggFunc) AggFunc {
	fu, err := ToProtoAggFunc(f)
	if err != nil {
		panic(err)
	}
	return fu
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
		return seq.DocsOrderDesc, fmt.Errorf("unknown order")
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

func ToProtoOrder(o seq.DocsOrder) (Order, error) {
	if int(o) >= len(orderMappingsPb) {
		return Order_ORDER_DESC, fmt.Errorf("unknown order")
	}
	return orderMappings[o], nil
}

func MustProtoOrder(o seq.DocsOrder) Order {
	order, err := ToProtoOrder(o)
	if err != nil {
		panic(err)
	}
	return order
}

var statusMappings = []AsyncSearchStatus{
	asyncsearcher.AsyncSearchStatusDone:       AsyncSearchStatus_AsyncSearchStatusDone,
	asyncsearcher.AsyncSearchStatusInProgress: AsyncSearchStatus_AsyncSearchStatusInProgress,
	asyncsearcher.AsyncSearchStatusError:      AsyncSearchStatus_AsyncSearchStatusError,
	asyncsearcher.AsyncSearchStatusCanceled:   AsyncSearchStatus_AsyncSearchStatusCanceled,
}

var statusMappingsPb = func() []asyncsearcher.AsyncSearchStatus {
	mappings := make([]asyncsearcher.AsyncSearchStatus, len(statusMappings))
	for from, to := range statusMappings {
		mappings[to] = asyncsearcher.AsyncSearchStatus(from)
	}
	return mappings
}()

func (s AsyncSearchStatus) ToAsyncSearchStatus() (asyncsearcher.AsyncSearchStatus, error) {
	if int(s) >= len(statusMappingsPb) {
		return 0, fmt.Errorf("unknown status: %d", s)
	}
	return statusMappingsPb[s], nil
}

func (s AsyncSearchStatus) MustAsyncSearchStatus() asyncsearcher.AsyncSearchStatus {
	v, err := s.ToAsyncSearchStatus()
	if err != nil {
		panic(err)
	}
	return v
}

func ToProtoAsyncSearchStatus(s asyncsearcher.AsyncSearchStatus) (AsyncSearchStatus, error) {
	if int(s) >= len(statusMappings) {
		return 0, fmt.Errorf("unknown status")
	}
	return statusMappings[s], nil
}

func MustProtoAsyncSearchStatus(s asyncsearcher.AsyncSearchStatus) AsyncSearchStatus {
	v, err := ToProtoAsyncSearchStatus(s)
	if err != nil {
		panic(err)
	}
	return v
}

var typeMappings = []DataType{
	query.DataTypeBytes:        DataType_BYTES,
	query.DataTypeSeqID:        DataType_SEQ_ID,
	query.DataTypeDocument:     DataType_RAW_DOCUMENT,
	query.DataTypeString:       DataType_STRING,
	query.DataTypeUint32:       DataType_UINT32,
	query.DataTypeUint64:       DataType_UINT64,
	query.DataTypeInt32:        DataType_INT32,
	query.DataTypeInt64:        DataType_INT64,
	query.DataTypeFloat64:      DataType_FLOAT64,
	query.DataTypeFloat64Array: DataType_FLOAT64_ARRAY,
	query.DataTypeStringArray:  DataType_STRING_ARRAY,
}

var typeMappingsPb = func() []query.DataType {
	mappings := make([]query.DataType, len(typeMappings))
	for from, to := range typeMappings {
		mappings[to] = query.DataType(from)
	}
	return mappings
}()

func (t DataType) ToQueryDataType() (query.DataType, error) {
	if int(t) >= len(typeMappingsPb) || t < 0 {
		return 0, fmt.Errorf("unknown data type: %d", t)
	}
	return typeMappingsPb[t], nil
}

func (t DataType) MustQueryDataType() query.DataType {
	v, err := t.ToQueryDataType()
	if err != nil {
		panic(err)
	}
	return v
}

func ToProtoDataType(t query.DataType) (DataType, error) {
	if int(t) >= len(typeMappings) {
		return 0, fmt.Errorf("unknown data type: %d", t)
	}
	return typeMappings[t], nil
}

func MustProtoDataType(t query.DataType) DataType {
	v, err := ToProtoDataType(t)
	if err != nil {
		panic(err)
	}
	return v
}
