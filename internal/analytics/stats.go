package analytics

import (
	"math"
	"sort"

	"github.com/silasms/stream-data-pipeline/internal/domain"
)

func CalculateWindowStats(metric string, values []float64, start, end domain.Event) *domain.WindowResult {
	n := len(values)
	if n == 0 {
		return nil
	}

	sum := 0.0
	minVal := values[0]
	maxVal := values[0]

	sorted := make([]float64, n)
	copy(sorted, values)
	sort.Float64s(sorted)

	for _, v := range sorted {
		sum += v
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	mean := sum / float64(n)

	var variance float64
	if n > 1 {
		sqDiffSum := 0.0
		for _, v := range sorted {
			diff := v - mean
			sqDiffSum += diff * diff
		}
		variance = sqDiffSum / float64(n-1)
	}
	stdDev := math.Sqrt(variance)

	return &domain.WindowResult{
		Metric:      metric,
		WindowStart: start.Timestamp,
		WindowEnd:   end.Timestamp,
		Count:       int64(n),
		Sum:         sum,
		Min:         minVal,
		Max:         maxVal,
		Mean:        mean,
		Variance:    variance,
		StdDev:      stdDev,
	}
}
