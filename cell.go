package pivot

import (
	"fmt"
	"strings"
)

type cell[T valueType] interface {
	fmt.Stringer
	Set(index int, compute Compute[T], keys []DataRef) error
	Get() []T
	Record(key DataRef, value T)
}

type pivotCell[T valueType] struct {
	finalValues    []T
	recordedValues map[DataRef]T
	formats        []string
}

func newPivotCell(formats []ValueFormat) cell[float64] {
	valueFormats := make([]string, len(formats))
	for i := 0; i < len(formats); i++ {
		valueFormats[i] = string(formats[i])
	}
	return &pivotCell[float64]{
		finalValues:    make([]float64, len(formats)),
		recordedValues: make(map[DataRef]float64),
		formats:        valueFormats,
	}
}

func (p *pivotCell[T]) String() string {
	var sb strings.Builder
	if len(p.finalValues) > 1 {
		sb.WriteString("[ ")
		for i := 0; i < len(p.finalValues); i++ {
			sb.WriteString(fmt.Sprintf(p.formats[i], p.finalValues[i]))
			if i < len(p.finalValues)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" ]")
		return sb.String()
	} else {
		return fmt.Sprintf(p.formats[0], p.finalValues[0])
	}
}

func (p *pivotCell[T]) Set(index int, compute Compute[T], keys []DataRef) error {
	if compute != nil {
		var elements []RawValue
		var err error
		for _, key := range keys {
			elements = append(elements, p.recordedValues[key])
		}
		p.finalValues[index], err = compute(elements)
		if err != nil {
			return fmt.Errorf("while computing for %v: %w", elements, err)
		}
	} else {
		p.finalValues[index] = p.recordedValues[keys[0]]
	}
	return nil
}

func (p *pivotCell[T]) Get() []T {
	return p.finalValues
}

func (p *pivotCell[T]) Record(key DataRef, value T) {
	if key.operation == Sum {
		p.recordedValues[key] += value
	} else if key.operation == Count {
		p.recordedValues[key]++
	}
}
