// Processed 13156.7MB in 1m22.937343083s

package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

const (
	maxNameLenR2 = 30
	bufferSizeR2 = 500000
)

func divRoundClosest(n int64, d int64) int64 {
	if n >= 0 {
		return (n + d/2) / d
	}
	return (n - d/2) / d
}

type measurementAggregatorR2 struct {
	min   int16
	max   int16
	sum   int64
	count int
}

func (ma *measurementAggregatorR2) add(measurement int16) {
	ma.sum += int64(measurement)
	ma.count++
	ma.min = min(ma.min, measurement)
	ma.max = max(ma.max, measurement)
}

func newMeasurementAggregatorR2() *measurementAggregatorR2 {
	return &measurementAggregatorR2{
		min:   math.MaxInt16,
		max:   -math.MaxInt16,
		sum:   0,
		count: 0,
	}
}

func formatTempR2(temp int16) string {
	if temp < 0 {
		temp = -temp
		return fmt.Sprintf("-%d.%d", temp/10, temp%10)
	}
	return fmt.Sprintf("%d.%d", temp/10, temp%10)
}

func arrayToStringR2(nameArr [maxNameLenR2]byte) string {
	end := bytes.IndexByte(nameArr[:], 0)
	if end == -1 {
		end = maxNameLenR2
	}
	return string(nameArr[:end])
}

func stringToArrayR2(name string) [maxNameLenR2]byte {
	nameBuf := [maxNameLenR2]byte{}
	copy(nameBuf[:], name)
	return nameBuf
}

func r2(inputPath string, output io.Writer) error {
	measurementAggregators := make(map[[maxNameLenR2]byte]*measurementAggregatorR2)
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := make([]byte, bufferSizeR2)
	nameBuf := [maxNameLenR2]byte{}
	name := nameBuf[:0]
	measurement := int16(0)
	isNegative := false
	isReadingName := true

	for {
		n, err := file.Read(buf)
		// fmt.Println(n, err)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		for _, b := range buf[:n] {
			switch b {
			case ';':
				isReadingName = false
			case '.':
				if isReadingName {
					name = append(name, b)
				}
			case '\n':
				isReadingName = true
				if isNegative {
					measurement = -measurement
					isNegative = false
				}
				for i := len(name); i < len(nameBuf); i++ {
					nameBuf[i] = 0
				}
				aggregator, ok := measurementAggregators[nameBuf]
				if !ok {
					aggregator = newMeasurementAggregatorR2()
					measurementAggregators[nameBuf] = aggregator
				}
				aggregator.add(measurement)
				name = name[:0]
				measurement = int16(0)
			case '-':
				if isReadingName {
					name = append(name, b)
				} else {
					isNegative = true
				}
			default:
				if isReadingName {
					name = append(name, b)
				} else {
					measurement = measurement*10 + int16(b-'0')
				}
			}
		}
	}

	names := make([][maxNameLenR2]byte, 0, len(measurementAggregators))
	for name := range measurementAggregators {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return bytes.Compare(names[i][:], names[j][:]) < 0
	})

	aggregator := measurementAggregators[names[0]]
	mean := int16(divRoundClosest(aggregator.sum, int64(aggregator.count)))
	fmt.Fprintf(output, "{%s=%s/%s/%s",
		arrayToStringR2(names[0]),
		formatTempR2(aggregator.min),
		formatTempR2(mean),
		formatTempR2(aggregator.max))
	for _, name := range names[1:] {
		aggregator = measurementAggregators[name]
		mean = int16(divRoundClosest(aggregator.sum, int64(aggregator.count)))
		fmt.Fprintf(output, ", %s=%s/%s/%s",
			arrayToStringR2(name),
			formatTempR2(aggregator.min),
			formatTempR2(mean),
			formatTempR2(aggregator.max))
	}
	fmt.Fprint(output, "}\n")

	return nil
}
