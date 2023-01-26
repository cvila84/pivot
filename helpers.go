package pivot

import (
	"fmt"
	"strconv"
	"strings"
)

func toFloat(element RawValue) (float64, error) {
	switch element.(type) {
	case int:
		return float64(element.(int)), nil
	case float64:
		return element.(float64), nil
	case string:
	default:
		return 0, InvalidType(element)
	}
	es, _ := element.(string)
	es = strings.Replace(es, ",", ".", 1)
	result, err := strconv.ParseFloat(es, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric format for element %q", element)
	}
	return result, nil
}

func computeString(serie series[string], record []interface{}) (string, error) {
	var value string
	if serie.compute != nil {
		var elements []RawValue
		for _, dataRef := range serie.dataRefs {
			elements = append(elements, record[dataRef.index])
		}
		var err error
		value, err = serie.compute(elements)
		if err != nil {
			return "", fmt.Errorf("while computing for %v: %w", elements, err)
		}
	} else {
		var ok bool
		value, ok = record[serie.dataRefs[0].index].(string)
		if !ok {
			value = fmt.Sprintf("%v", record[0])
		}
	}
	return value, nil
}

func filter(filters map[int]Filter, series []*series[string], records [][]interface{}, headers bool) ([][]interface{}, error) {
	filteredRecords := make([][]interface{}, 0)
	for i, record := range records {
		if i != 0 || !headers {
			keep := true
			for j, f := range filters {
				if !f(record[j]) {
					keep = false
				}
			}
			for _, serie := range series {
				value, err := computeString(*serie, record)
				if err != nil {
					return nil, fmt.Errorf("while filtering in serie %q for record %v: %w", serie.name, record, err)
				}
				if serie.filter != nil && !serie.filter(value) {
					keep = false
				}
			}
			if keep {
				filteredRecords = append(filteredRecords, record)
			}
		}
	}
	return filteredRecords, nil
}

func walk(headers *headers, series []*series[string], record []interface{}) (string, error) {
	h := headers
	for _, serie := range series {
		value, err := computeString(*serie, record)
		if err != nil {
			return "", fmt.Errorf("while walking in serie %q for record %v: %w", serie.name, record, err)
		}
		h = h.sort(serie.sort).walk(value)
	}
	return h.label, nil
}
