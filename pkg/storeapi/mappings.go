package storeapi

import (
	"fmt"

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
