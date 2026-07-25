package snuffle

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

const sampleTimestampBucketMillis = int64(10 * 60 * 1000)

func sampleBaseFilters(cfg Config, matchers []*labels.Matcher, mint, maxt int64) []string {
	filters := []string{teamFilter(cfg)}
	filters = append(filters, sampleTimeFilters(cfg, mint, maxt)...)
	filters = append(filters, metricNameConstraints(matchers)...)
	filters = append(filters, postHogServiceNameFilters(cfg, matchers)...)
	return filters
}

func sampleTimeFilters(cfg Config, mint, maxt int64) []string {
	filters := []string{
		"timestamp >= " + chTimeMillis(mint),
		"timestamp <= " + chTimeMillis(maxt),
	}
	if cfg.postHogSchemaLayout() {
		filters = append(filters,
			"time_bucket >= toStartOfDay("+chTimeMillis(mint)+")",
			"time_bucket <= toStartOfDay("+chTimeMillis(maxt)+")",
		)
	} else {
		filters = append(filters,
			"toStartOfTenMinutes(timestamp) >= toStartOfTenMinutes("+chTimeMillis(mint)+")",
			"toStartOfTenMinutes(timestamp) <= toStartOfTenMinutes("+chTimeMillis(maxt)+")",
		)
	}
	return filters
}

func sampleStepBucketFilters(cfg Config, startMillis, stepMillis, steps int64) []string {
	if cfg.postHogSchemaLayout() || steps <= 0 || stepMillis < sampleTimestampBucketMillis {
		return nil
	}
	return []string{fmt.Sprintf(
		"toStartOfTenMinutes(timestamp) IN (SELECT toStartOfTenMinutes(fromUnixTimestamp64Milli(toInt64(%d) + toInt64(number) * %d, 'UTC')) FROM numbers(toUInt64(%d)))",
		startMillis,
		stepMillis,
		steps,
	)}
}

func postHogServiceNameFilters(cfg Config, matchers []*labels.Matcher) []string {
	if !cfg.postHogSchemaLayout() {
		return nil
	}
	for _, matcher := range matchers {
		if matcher.Name != "service.name" && matcher.Name != "service_name" {
			continue
		}
		switch matcher.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if matcher.Matches("") {
				continue
			}
			if condition, ok := stringColumnMatcherCondition("service_name", matcher); ok {
				return []string{condition}
			}
		}
	}
	return nil
}

func stringColumnMatcherCondition(column string, matcher *labels.Matcher) (string, bool) {
	switch matcher.Type {
	case labels.MatchEqual:
		return column + " = " + sqlString(matcher.Value), true
	case labels.MatchNotEqual:
		return column + " != " + sqlString(matcher.Value), true
	case labels.MatchRegexp:
		if values := matcher.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return column + " IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := matcher.Prefix(); prefix != "" {
			return "startsWith(" + column + ", " + sqlString(prefix) + ")", true
		}
		return "match(" + column + ", " + sqlString(promRegexToCH(matcher.Value)) + ")", true
	case labels.MatchNotRegexp:
		if values := matcher.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return column + " NOT IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := matcher.Prefix(); prefix != "" {
			return "NOT startsWith(" + column + ", " + sqlString(prefix) + ")", true
		}
		return "NOT match(" + column + ", " + sqlString(promRegexToCH(matcher.Value)) + ")", true
	default:
		return "", false
	}
}

func sampleSelectedSeriesFilters(cfg Config) []string {
	return sampleIDMembershipFilters(cfg, "IN", "SELECT id FROM selected_series")
}

// sampleSelectedSeriesFiltersFromMatchers prunes a sample scan to the selected
// series without naming the selected_series CTE. ClickHouse inlines CTEs rather
// than materialising them, so referencing it from the sample scan runs the whole
// series lookup a second time -- which costs more than the scan it saves. The
// label index answers the same question on its own, off a single key prefix.
func sampleSelectedSeriesFiltersFromMatchers(cfg Config, matchers []*labels.Matcher) []string {
	if metric := exactMetricName(matchers); metric != "" && cfg.LabelIndexTable != "" {
		if filters, ok := nonMetricSampleIDFiltersFromMatchers(cfg, metric, matchers); ok {
			return filters
		}
	}
	return sampleSelectedSeriesFilters(cfg)
}

func sampleExplicitIDFilters(cfg Config, ids []uint64) []string {
	return sampleIDMembershipFilters(cfg, "IN", joinUint64(ids))
}

func sampleIDMembershipFilters(cfg Config, membership, source string) []string {
	filters := []string{"id " + membership + " (" + source + ")"}
	if cfg.postHogSchemaLayout() {
		filters = append(filters, "resource_fingerprint "+membership+" ("+source+")")
	}
	return filters
}
