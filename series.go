package pivot

import "fmt"

type Operation int

const (
	none Operation = iota
	Count
	Sum
)

type DataRef struct {
	index     int
	operation Operation
}

func DataRefs(indexes []int, operation Operation) []DataRef {
	dataRefs := make([]DataRef, len(indexes))
	for i := 0; i < len(indexes); i++ {
		dataRefs[i].index = indexes[i]
		dataRefs[i].operation = operation
	}
	return dataRefs
}

type seriesType interface{ string | float64 }

type series[T seriesType] struct {
	dataRefs []DataRef
	name     string
	filter   Filter
	compute  Compute[T]
	sort     Sort
	format   string
}

func newRCSeries(dataIndexes []int, filter Filter, compute Compute[string], sort Sort) *series[string] {
	dataRefs := make([]DataRef, len(dataIndexes))
	for i := 0; i < len(dataIndexes); i++ {
		dataRefs[i] = DataRef{index: dataIndexes[i], operation: none}
	}
	return &series[string]{
		dataRefs: dataRefs,
		filter:   filter,
		compute:  compute,
		sort:     sort,
	}
}

func newVSeries(name SeriesName, dataRefs []DataRef, compute Compute[float64], format ValueFormat) *series[float64] {
	return &series[float64]{
		dataRefs: dataRefs,
		name:     string(name),
		compute:  compute,
		format:   string(format),
	}
}

func toIndexes(dataRefs []DataRef) []int {
	result := make([]int, len(dataRefs))
	for i := 0; i < len(dataRefs); i++ {
		result[i] = dataRefs[i].index
	}
	return result
}

func (s *series[T]) NameFromHeaders(headers []interface{}) {
	if s.compute != nil {
		if len(s.dataRefs) > 0 && len(s.name) == 0 {
			s.name = fmt.Sprintf("Computed%v", toIndexes(s.dataRefs))
		}
	} else {
		if len(s.dataRefs) > 0 {
			if headers != nil {
				var ok bool
				s.name, ok = headers[s.dataRefs[0].index].(string)
				if !ok {
					s.name = fmt.Sprintf("Unnamed%v", toIndexes(s.dataRefs))
				}
			} else {
				s.name = fmt.Sprintf("Unnamed%v", toIndexes(s.dataRefs))
			}
		}
	}
}
