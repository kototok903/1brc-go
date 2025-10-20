// Processed 13156.7MB in 2m33.881545042s

package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type measurementAggregatorR1 struct {
	min   float64
	max   float64
	sum   float64
	count int
}

func (ma *measurementAggregatorR1) add(measurement float64) {
	ma.sum += measurement
	ma.count++
	ma.min = min(ma.min, measurement)
	ma.max = max(ma.max, measurement)
}

func newMeasurementAggregatorR1() *measurementAggregatorR1 {
	return &measurementAggregatorR1{
		min:   math.MaxFloat64,
		max:   -math.MaxFloat64,
		sum:   0,
		count: 0,
	}
}

func r1(inputPath string, output io.Writer) error {
	measurementAggregators := make(map[string]*measurementAggregatorR1)
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")
		name := fields[0]
		measurement, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return err
		}
		aggregator, ok := measurementAggregators[name]
		if !ok {
			aggregator = newMeasurementAggregatorR1()
			measurementAggregators[name] = aggregator
		}
		aggregator.add(measurement)
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	names := make([]string, 0, len(measurementAggregators))
	for name := range measurementAggregators {
		names = append(names, name)
	}
	sort.Strings(names)
	aggregator := measurementAggregators[names[0]]
	fmt.Fprintf(output, "{%s=%.1f/%.1f/%.1f", names[0], aggregator.min, aggregator.sum/float64(aggregator.count), aggregator.max)
	for _, name := range names[1:] {
		aggregator = measurementAggregators[name]
		fmt.Fprintf(output, ", %s=%.1f/%.1f/%.1f", name, aggregator.min, aggregator.sum/float64(aggregator.count), aggregator.max)
	}
	fmt.Fprint(output, "}\n")
	return nil
}
