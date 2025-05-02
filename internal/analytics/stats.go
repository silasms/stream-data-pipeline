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
		P50:         PercentileSorted(sorted, 50.0),
		P90:         PercentileSorted(sorted, 90.0),
		P99:         PercentileSorted(sorted, 99.0),
	}
}

func PercentileSorted(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}

	rank := (p / 100.0) * float64(n-1)
	low := int(math.Floor(rank))
	high := int(math.Ceil(rank))
	weight := rank - float64(low)

	return sorted[low]*(1.0-weight) + sorted[high]*weight
}
