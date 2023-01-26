package pivot

import (
	"fmt"
	"strings"
)

type RawValue interface{}

type Header string

type ValueFormat string

type SeriesName string

type Filter func(RawValue) bool

type Sort func([]Header) []Header

type Compute[T seriesType] func([]RawValue) (T, error)

type valueType interface{ float64 }

type converter[T valueType] func(RawValue) (T, error)

type vSeriesFactory[T valueType] func(SeriesName, []DataRef, Compute[T], ValueFormat) *series[T]

type cellFactory[T valueType] func([]ValueFormat) cell[T]

// Table
// usedIndexes to avoid declaring same index as row & column
type Table[T valueType] struct {
	data                [][]interface{}
	dataHeaders         bool
	registeredRCIndexes map[int]bool
	registeredVIndexes  map[DataRef]bool
	cells               map[string]map[string]cell[T]
	filters             map[int]Filter
	rowHeaders          *headers
	columnHeaders       *headers
	valueHeaders        *headers
	rowSeries           []*series[string]
	columnSeries        []*series[string]
	valueSeries         []*series[T]
	series              map[int]*series[T]
	newVSeries          vSeriesFactory[T]
	newCell             cellFactory[T]
	cellValue           converter[T]
	err                 error
}

func NewTable(data [][]interface{}, dataHeaders bool) *Table[float64] {
	var err error
	if data == nil || (len(data) == 0 && !dataHeaders) || (len(data) <= 1 && dataHeaders) {
		err = fmt.Errorf("no input data")
	} else if len(data[0]) == 0 {
		err = fmt.Errorf("no input data")
	} else {
		length := -1
		for _, record := range data {
			if length < 0 {
				length = len(record)
			} else if len(record) != length {
				err = fmt.Errorf("input data has variable records size")
			}
		}
	}
	return &Table[float64]{
		data:                data,
		dataHeaders:         dataHeaders,
		registeredRCIndexes: make(map[int]bool),
		registeredVIndexes:  make(map[DataRef]bool),
		cells:               make(map[string]map[string]cell[float64]),
		filters:             make(map[int]Filter),
		rowHeaders:          newRootHeaders(nil),
		columnHeaders:       newRootHeaders(nil),
		// TODO populate this variable when several values are requested and use it for Generate
		valueHeaders: nil,
		rowSeries:    make([]*series[string], 0),
		columnSeries: make([]*series[string], 0),
		valueSeries:  make([]*series[float64], 0),
		newVSeries:   newVSeries,
		newCell:      newPivotCell,
		cellValue:    toFloat,
		err:          err,
	}
}

func (t *Table[T]) updateCell(rowLabel string, columnLabel string, record []interface{}) error {
	rr, ok := t.cells[rowLabel]
	if !ok {
		rr = make(map[string]cell[T])
		t.cells[rowLabel] = rr
	}
	rc, ok := rr[columnLabel]
	if !ok {
		displays := make([]ValueFormat, len(t.valueSeries))
		for i, s := range t.valueSeries {
			displays[i] = ValueFormat(s.format)
		}
		rc = t.newCell(displays)
		rr[columnLabel] = rc
	}
	for k := range t.registeredVIndexes {
		value, err := t.cellValue(record[k.index])
		if err != nil {
			return fmt.Errorf("while updating cell: %w", err)
		}
		rc.Record(k, value)
	}
	for is, serie := range t.valueSeries {
		err := rc.Set(is, serie.compute, serie.dataRefs)
		if err != nil {
			return fmt.Errorf("while updating cell: %w", err)
		}
	}
	return nil
}

func (t *Table[T]) updateCrossCells(rowLabel string, columnLabel string, record []interface{}) error {
	sumColumnLabel := columnLabel
	for i := 0; i < len(t.columnSeries)+1; i++ {
		sumRowLabel := rowLabel
		for j := 0; j < len(t.rowSeries)+1; j++ {
			if i != 0 || j != 0 {
				err := t.updateCell(sumRowLabel, sumColumnLabel, record)
				if err != nil {
					return err
				}
			}
			sumRowLabel = parentHeaderLabel(sumRowLabel)
		}
		sumColumnLabel = parentHeaderLabel(sumColumnLabel)
	}
	return nil
}

func (t *Table[T]) registerRow(indexes []int, filter Filter, compute Compute[string], sort Sort) error {
	if len(indexes) == 0 {
		return fmt.Errorf("invalid row definition, no indexes given")
	}
	if compute == nil && len(indexes) != 1 {
		return fmt.Errorf("invalid row definition, several indexes with no compute given")
	}
	if compute == nil {
		_, ok := t.registeredRCIndexes[indexes[0]]
		if ok {
			return fmt.Errorf("invalid row definition, index already used")
		}
		t.registeredRCIndexes[indexes[0]] = true
	}
	t.rowSeries = append(t.rowSeries, newRCSeries(indexes, filter, compute, sort))
	return nil
}

func (t *Table[T]) registerColumn(indexes []int, filter Filter, compute Compute[string], sort Sort) error {
	if len(indexes) == 0 {
		return fmt.Errorf("invalid column definition, no indexes given")
	}
	if compute == nil && len(indexes) != 1 {
		return fmt.Errorf("invalid column definition, several indexes with no compute given")
	}
	if compute == nil {
		_, ok := t.registeredRCIndexes[indexes[0]]
		if ok {
			return fmt.Errorf("invalid column definition, index already used")
		}
		t.registeredRCIndexes[indexes[0]] = true
	}
	t.columnSeries = append(t.columnSeries, newRCSeries(indexes, filter, compute, sort))
	return nil
}

func (t *Table[T]) registerValue(name string, dataRefs []DataRef, compute Compute[T], format string) error {
	if len(dataRefs) == 0 {
		return fmt.Errorf("invalid value definition, no indexes given")
	}
	if compute == nil && len(dataRefs) != 1 {
		return fmt.Errorf("invalid value definition, several indexes with no compute given")
	}
	for i := 0; i < len(dataRefs); i++ {
		t.registeredVIndexes[dataRefs[i]] = true
	}
	t.valueSeries = append(t.valueSeries, t.newVSeries(SeriesName(name), dataRefs, compute, ValueFormat(format)))
	return nil
}

func (t *Table[T]) Generate() error {
	if t.err != nil {
		return t.err
	}
	if len(t.rowSeries) == 0 {
		return fmt.Errorf("no rows defined")
	}
	if len(t.columnSeries) == 0 {
		return fmt.Errorf("no columns defined")
	}
	if len(t.valueSeries) == 0 {
		return fmt.Errorf("no values defined")
	}
	var headerSeries []*series[string]
	var headerLabels []interface{}
	if t.dataHeaders {
		headerLabels = t.data[0]
	}
	headerSeries = append(headerSeries, t.rowSeries...)
	headerSeries = append(headerSeries, t.columnSeries...)
	for _, serie := range headerSeries {
		serie.NameFromHeaders(headerLabels)
	}
	filteredData, err := filter(t.filters, headerSeries, t.data, t.dataHeaders)
	if err != nil {
		return err
	}
	for _, serie := range t.valueSeries {
		serie.NameFromHeaders(headerLabels)
	}
	for _, record := range filteredData {
		var rowLabel string
		var columnLabel string
		rowLabel, err = walk(t.rowHeaders, t.rowSeries, record)
		if err != nil {
			return err
		}
		if len(rowLabel) == 0 {
			return fmt.Errorf("empty row labels are not supported")
		}
		columnLabel, err = walk(t.columnHeaders, t.columnSeries, record)
		if err != nil {
			return err
		}
		if len(columnLabel) == 0 {
			return fmt.Errorf("empty column labels are not supported")
		}
		err = t.updateCell(rowLabel, columnLabel, record)
		if err != nil {
			return err
		}
		err = t.updateCrossCells(rowLabel, columnLabel, record)
		if err != nil {
			return err
		}
	}
	return nil
}

// ToCSV
// TODO manage multi-values through virtual column
func (t *Table[T]) ToCSV() string {
	columnLabels := t.columnHeaders.labels(true, true)
	rowLabels := t.rowHeaders.labels(true, true)
	var sb strings.Builder
	for _, columnLabel := range columnLabels {
		if columnLabel == "" {
			_, _ = fmt.Fprint(&sb, ";Total")
		} else {
			_, _ = fmt.Fprint(&sb, ";"+columnLabel)
		}
	}
	_, _ = fmt.Fprintln(&sb)
	for _, rowLabel := range rowLabels {
		if rowLabel == "" {
			_, _ = fmt.Fprint(&sb, "Total;")
		} else {
			_, _ = fmt.Fprint(&sb, rowLabel+";")
		}
		for i, columnLabel := range columnLabels {
			v, ok := t.cells[rowLabel][columnLabel]
			if ok {
				_, _ = fmt.Fprintf(&sb, v.String())
			} else {
				_, _ = fmt.Fprintf(&sb, "")
			}
			if i < len(columnLabels)-1 {
				_, _ = fmt.Fprintf(&sb, ";")
			}
		}
		_, _ = fmt.Fprintln(&sb)
	}
	return sb.String()
}

func (t *Table[T]) Filter(index int, filter Filter) *Table[T] {
	t.filters[index] = filter
	return t
}

func (t *Table[T]) Row(index int) *Table[T] {
	return t.ComputedRow([]int{index}, nil, nil, nil)
}

func (t *Table[T]) ComputedRow(indexes []int, filter Filter, compute Compute[string], sort Sort) *Table[T] {
	err := t.registerRow(indexes, filter, compute, sort)
	if t.err == nil {
		t.err = err
	}
	return t
}

func (t *Table[T]) Column(index int) *Table[T] {
	return t.ComputedColumn([]int{index}, nil, nil, nil)
}

func (t *Table[T]) ComputedColumn(indexes []int, filter Filter, compute Compute[string], sort Sort) *Table[T] {
	err := t.registerColumn(indexes, filter, compute, sort)
	if t.err == nil {
		t.err = err
	}
	return t
}

func (t *Table[T]) Values(index int, operation Operation, format string) *Table[T] {
	dataRef := DataRef{index: index, operation: operation}
	err := t.registerValue("", []DataRef{dataRef}, nil, format)
	if t.err == nil {
		t.err = err
	}
	return t
}

func (t *Table[T]) ComputedValues(name string, dataRefs []DataRef, compute Compute[T], format string) *Table[T] {
	err := t.registerValue(name, dataRefs, compute, format)
	if t.err == nil {
		t.err = err
	}
	return t
}
