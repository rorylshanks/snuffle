package snuffle

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func TestSeriesExprBranchesFlattenSelectorUnion(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (job) ((up{job="api"} or up{job="worker"} or process_start_time_seconds{job="api"}))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}

	branches, ok := seriesExprBranches(aggregate.Expr)
	if !ok {
		t.Fatal("seriesExprBranches returned ok=false")
	}
	if len(branches) != 3 {
		t.Fatalf("branch count = %d, want 3", len(branches))
	}
	for _, branch := range branches {
		if branch.kind != seriesExprSelector || !branch.transform.identity() {
			t.Fatalf("plain selector branch parsed as %#v", branch)
		}
	}
}

func TestSeriesExprBranchesRejectMatchingModifiers(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	for _, query := range []string{
		`up{job="api"} or on (job) up{job="worker"}`,
		`up{job="api"} or ignoring (instance) up{job="worker"}`,
	} {
		expr, err := p.ParseExpr(query)
		if err != nil {
			t.Fatalf("ParseExpr(%q) returned error: %v", query, err)
		}
		if branches, ok := seriesExprBranches(expr); ok {
			t.Fatalf("seriesExprBranches(%q) = %#v, want rejection", query, branches)
		}
	}
}

func TestFastAggregateRangeUnionRejectsLargeUnions(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	parts := make([]string, 0, maxAggregateUnionSelectors+1)
	for i := range maxAggregateUnionSelectors + 1 {
		parts = append(parts, `up{job="`+strconv.Itoa(i)+`"}`)
	}
	expr, err := p.ParseExpr(`sum(` + strings.Join(parts, " or ") + `)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}
	branches, ok := seriesExprBranches(aggregate.Expr)
	if !ok {
		t.Fatal("seriesExprBranches returned ok=false")
	}
	if len(branches) <= maxAggregateUnionSelectors {
		t.Fatalf("branch count = %d, want more than fast-path cap", len(branches))
	}

	s := &Server{cfg: Config{LookbackDelta: 5 * time.Minute}}
	start := time.Unix(1700000000, 0).UTC()
	if _, handled, err := s.tryFastAggregateRangeUnionQuery(context.Background(), aggregate, start, start.Add(time.Hour), time.Minute, time.Minute.Milliseconds(), "sum(sample_value)"); handled || err != nil {
		t.Fatalf("tryFastAggregateRangeUnionQuery handled=%v err=%v, want unhandled", handled, err)
	}
}

func TestPostHogAggregateUnionPlanFactorsSingleVaryingLabel(t *testing.T) {
	cfg := Config{
		CHDatabase:    "default",
		SchemaLayout:  "posthog",
		SamplesTable:  "metrics1",
		LookbackDelta: 5 * time.Minute,
	}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (hostname) (increase(usage_user{hostname="host_0"}[5m]) / 5 or increase(usage_user{hostname="host_1"}[5m]) / 5)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	start := time.Unix(1700000000, 0).UTC()
	plan, ok := s.postHogAggregateUnionPlan(aggregate, start, start.Add(time.Hour), time.Minute)
	if !ok {
		t.Fatal("postHogAggregateUnionPlan returned ok=false")
	}

	sql := postHogAggregateUnionSQL(cfg, plan, aggregate.Grouping, start, time.Minute.Milliseconds(), "sum(sample_value)")
	for _, want := range []string{
		"timeSeriesRateToGrid",
		"IN ('host_0','host_1')",
		"metric_name = 'usage_user'",
		"GROUP BY series_id",
		"x / 5",
		"`default`.`metrics1`",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("union SQL does not contain %q:\n%s", want, sql)
		}
	}
	// the varying hostname expression must be factored into one IN lookup, not
	// one map lookup per branch
	_, where, found := strings.Cut(sql, " WHERE ")
	if !found {
		t.Fatalf("union SQL has no WHERE clause:\n%s", sql)
	}
	if got := strings.Count(where, "mapContains(attributes_map_str, 'hostname__str')"); got != 1 {
		t.Fatalf("hostname lookup count in WHERE = %d, want 1:\n%s", got, sql)
	}
	if got := strings.Count(sql, "FROM `default`.`metrics1`"); got != 1 {
		t.Fatalf("samples table scan count = %d, want 1:\n%s", got, sql)
	}
}

func TestPostHogAggregateUnionPlanRejectsMixedTransforms(t *testing.T) {
	s := &Server{cfg: Config{SchemaLayout: "posthog", SamplesTable: "metrics1", LookbackDelta: 5 * time.Minute}}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (hostname) (increase(usage_user{hostname="host_0"}[5m]) / 5 or increase(usage_user{hostname="host_1"}[5m]) / 6)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	start := time.Unix(1700000000, 0).UTC()
	if plan, ok := s.postHogAggregateUnionPlan(expr.(*parser.AggregateExpr), start, start.Add(time.Hour), time.Minute); ok {
		t.Fatalf("mixed scalar transforms should be rejected, got plan %#v", plan)
	}
}

func TestPostHogAggregateUnionPlanRejectsPlainSingleSelector(t *testing.T) {
	s := &Server{cfg: Config{SchemaLayout: "posthog", SamplesTable: "metrics1", LookbackDelta: 5 * time.Minute}}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (region) (usage_user{hostname="host_0"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	start := time.Unix(1700000000, 0).UTC()
	if plan, ok := s.postHogAggregateUnionPlan(expr.(*parser.AggregateExpr), start, start.Add(time.Hour), time.Minute); ok {
		t.Fatalf("plain single selector should use the dedicated path, got plan %#v", plan)
	}
}

func TestPostHogUnionMatcherConditionFallsBackToOr(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`up{job="api",env="prod"} or up{job="worker",env="dev"}`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	branches, ok := seriesExprBranches(expr)
	if !ok || len(branches) != 2 {
		t.Fatalf("seriesExprBranches ok=%v len=%d, want 2 branches", ok, len(branches))
	}
	selectors := []*parser.VectorSelector{branches[0].selector, branches[1].selector}
	condition, ok := postHogUnionMatcherCondition(selectors)
	if !ok {
		t.Fatal("postHogUnionMatcherCondition returned ok=false")
	}
	// two labels vary, so the IN factoring must not apply
	if !strings.Contains(condition, " OR ") {
		t.Fatalf("condition = %q, want OR of branch conjunctions", condition)
	}
	for _, want := range []string{"'api'", "'worker'", "'prod'", "'dev'"} {
		if !strings.Contains(condition, want) {
			t.Fatalf("condition %q does not contain %s", condition, want)
		}
	}
}

func TestNestedCountRangeSQLUsesSeriesBounds(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer, ok := expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("expr is %T, want *parser.AggregateExpr", expr)
	}
	inner, ok := outer.Expr.(*parser.AggregateExpr)
	if !ok {
		t.Fatalf("inner expr is %T, want *parser.AggregateExpr", outer.Expr)
	}
	selector, ok := inner.Expr.(*parser.VectorSelector)
	if !ok {
		t.Fatalf("selector expr is %T, want *parser.VectorSelector", inner.Expr)
	}

	start := time.Unix(1778398980, 0).UTC()
	end := time.Unix(1778402580, 0).UTC()
	step := time.Minute
	stepMillis := step.Milliseconds()
	source, mint, maxt, ok := selectorRangeGridSource(cfg, selector, start, end, step)
	if !ok {
		t.Fatal("selectorRangeGridSource returned ok=false")
	}
	selectedSeries, ok := selectedSeriesSQL(cfg, selector.LabelMatchers, mint, maxt, []string{"id", "min_time", "max_time"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	sql, ok := nestedCountRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, selectedSeries, source.start, start, stepMillis, steps, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"min(min_time) AS min_time",
		"max(max_time) AS max_time",
		"`default`.`series`",
		"`default`.`label_index`",
		"range(toUInt64(61)) AS step_idx",
		"max_time >= fromUnixTimestamp64Milli(",
		"min_time <= fromUnixTimestamp64Milli(",
		"group_intervals AS",
		"groupArray((toUnixTimestamp64Milli(min_time), toUnixTimestamp64Milli(max_time))) AS active_ranges",
		"GROUP BY `__group_0`",
		"arrayExists(x ->",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"metrics_samples", "timeSeriesLastToGrid", "sample_value", "id IN (SELECT id FROM selected_series)"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountTimestampSamplesRangeSQLUsesEvalTimestamps(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	stepMillis := time.Minute.Milliseconds()
	sql, ok := nestedCountTimestampSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 61, 5*time.Minute)
	if !ok {
		t.Fatal("nestedCountTimestampSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name = 'node_cpu_seconds_total'",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"active_samples AS (SELECT timestamp, id FROM `default`.`samples` WHERE",
		"FROM active_samples ANY LEFT JOIN group_labels USING id",
		"toFloat64(uniq(ifNull(`__group_0`, ''))) AS value",
		"modulo(toUnixTimestamp64Milli(timestamp) - 1778398980000, 60000) = 0",
		nonStaleSampleSQL("value"),
		"GROUP BY ts ORDER BY ts",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"active_ids AS", "range(toUInt64", "timeSeriesLastToGrid", "metrics_series"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountTimestampSamplesRangeSQLAddsLargeStepBucketPredicate(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398800, 0).UTC()
	stepMillis := (30 * time.Minute).Milliseconds()
	sql, ok := nestedCountTimestampSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 4, 5*time.Minute)
	if !ok {
		t.Fatal("nestedCountTimestampSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"toStartOfTenMinutes(timestamp) >= toStartOfTenMinutes(fromUnixTimestamp64Milli(1778398800000, 'UTC'))",
		"toStartOfTenMinutes(timestamp) <= toStartOfTenMinutes(fromUnixTimestamp64Milli(1778404200000, 'UTC'))",
		"toStartOfTenMinutes(timestamp) IN (SELECT toStartOfTenMinutes(fromUnixTimestamp64Milli(toInt64(1778398800000) + toInt64(number) * 1800000, 'UTC')) FROM numbers(toUInt64(4)))",
		"modulo(toUnixTimestamp64Milli(timestamp) - 1778398800000, 1800000) = 0",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestNestedCountTimestampSamplesRangeSQLBucketsUnalignedEvalTimes(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	evalStart := time.Unix(1778398987, 0).UTC()
	outputStart := time.Unix(1778398999, 0).UTC()
	sql, ok := nestedCountTimestampSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, evalStart, outputStart, time.Minute.Milliseconds(), 61, 5*time.Minute)
	if !ok {
		t.Fatal("nestedCountTimestampSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"step_map AS",
		"FROM numbers(toUInt64(61))",
		"intDiv(toInt64(1778398987000) + toInt64(number) * 60000, 15000) * 15000 AS bucket_ms",
		"active_samples ON active_samples.timestamp = fromUnixTimestamp64Milli(step_map.bucket_ms, 'UTC')",
		"SELECT toInt64(1778398999000) + toInt64(number) * 60000 AS ts",
		"label_name = 'ready'",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"modulo(toUnixTimestamp64Milli(timestamp)", "range(toUInt64"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestExactBucketRangeSelectorRequiresAlignedRemoteWriteRange(t *testing.T) {
	cfg := Config{RemoteWriteInterval: 15 * time.Second}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`usage_user{hostname="host_42"}`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	selector := expr.(*parser.VectorSelector)
	start := time.Unix(1451607900, 0).UTC()
	end := time.Unix(1451608185, 0).UTC()

	if !exactBucketRangeSelector(cfg, selector, start, end, 15*time.Second) {
		t.Fatal("aligned range should use exact bucket path")
	}
	if exactBucketRangeSelector(cfg, selector, start.Add(time.Millisecond), end, 15*time.Second) {
		t.Fatal("unaligned start should not use exact bucket path")
	}
	if exactBucketRangeSelector(cfg, selector, start, end, 30*time.Second) {
		t.Fatal("non-remote-write step should not use exact bucket path")
	}

	selector.OriginalOffset = time.Minute
	if exactBucketRangeSelector(cfg, selector, start, end, 15*time.Second) {
		t.Fatal("offset selector should not use exact bucket path")
	}
}

func TestLastGridExprTurnsStaleSamplesIntoNulls(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := time.Unix(160, 0).UTC()

	sql := lastGridExpr(start, end, 15*time.Second, 5*time.Minute)
	for _, want := range []string{
		"timeSeriesLastToGrid",
		"arrayMap(x -> if(isNull(x) OR",
		"reinterpretAsUInt64(assumeNotNull(x))",
		"NULL, x)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestMetricsQLRunningSumParses(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	if _, err := p.ParseExpr(`quantile(1, running_sum(delta(node_cpu_seconds_total{ready=~"true"}[1m])))`); err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
}

func TestAggregateRangeSourceSQLWrapsRunningSum(t *testing.T) {
	cfg := Config{
		LookbackDelta: 5 * time.Minute,
	}
	s := newServer(cfg)
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`quantile(1, running_sum(delta(node_cpu_seconds_total{ready=~"true"}[1m])))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)

	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, time.Unix(1700000000, 0).UTC(), time.Unix(1700000060, 0).UTC(), 15*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if !source.runningSumWrap {
		t.Fatal("running_sum should mark the range source for cumulative wrapping")
	}
	if selector == nil || selector.Name != "node_cpu_seconds_total" {
		t.Fatalf("selector = %#v", selector)
	}
	if mint != 1699999940000 || maxt != 1700000060000 {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}
	if !strings.Contains(source.gridExpr, "timeSeriesDeltaToGrid") {
		t.Fatalf("grid expression does not use delta grid:\n%s", source.gridExpr)
	}

	sql := rangeSourcePerSeriesSQL(source, "SELECT id, timestamp, value FROM samples")
	for _, want := range []string{
		"timeSeriesDeltaToGrid",
		"arrayCumSum",
		"assumeNotNull",
		"if(seen = 0, NULL, running_sum)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if got := strings.Count(sql, "timeSeriesDeltaToGrid"); got != 1 {
		t.Fatalf("delta grid expression count = %d, want 1:\n%s", got, sql)
	}
}

func TestAggregateRangeSourceSQLDeltaUsesPreviousSample(t *testing.T) {
	cfg := Config{
		LookbackDelta:       5 * time.Minute,
		RemoteWriteInterval: 15 * time.Second,
	}
	s := newServer(cfg)
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`quantile(1, delta(node_cpu_seconds_total{ready=~"true"}[15s]))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)

	start := time.Unix(1700000000, 0).UTC()
	end := time.Unix(1700000060, 0).UTC()
	source, _, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, start, end, 15*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if mint != start.Add(-30*time.Second).UnixMilli() || maxt != end.UnixMilli() {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}
	if !strings.Contains(source.gridExpr, ", 15, 30)(timestamp, value)") {
		t.Fatalf("delta grid should include one previous sample interval:\n%s", source.gridExpr)
	}
}

func TestAggregateRangeSourceSQLRateFunctionsUsePromQLRange(t *testing.T) {
	cfg := Config{
		LookbackDelta:       5 * time.Minute,
		RemoteWriteInterval: 15 * time.Second,
	}
	s := newServer(cfg)
	p := parser.NewParser(parser.Options{})

	for _, tc := range []struct {
		name      string
		query     string
		gridFunc  string
		windowSQL string
		extraSQL  string
		mint      int64
	}{
		{
			name:      "rate",
			query:     `quantile(1, rate(node_cpu_seconds_total{ready=~"true"}[15s]))`,
			gridFunc:  "timeSeriesRateToGrid",
			windowSQL: ", 15, 15)(timestamp, value)",
			mint:      time.Unix(1700000000, 0).UTC().Add(-15 * time.Second).UnixMilli(),
		},
		{
			name:      "irate",
			query:     `quantile(1, irate(node_cpu_seconds_total{ready=~"true"}[15s]))`,
			gridFunc:  "timeSeriesInstantRateToGrid",
			windowSQL: ", 15, 15)(timestamp, value)",
			mint:      time.Unix(1700000000, 0).UTC().Add(-15 * time.Second).UnixMilli(),
		},
		{
			name:      "increase",
			query:     `quantile(1, increase(node_cpu_seconds_total{ready=~"true"}[5m]))`,
			gridFunc:  "timeSeriesRateToGrid",
			windowSQL: ", 15, 300)(timestamp, value)",
			extraSQL:  "x * 300",
			mint:      time.Unix(1700000000, 0).UTC().Add(-300 * time.Second).UnixMilli(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := p.ParseExpr(tc.query)
			if err != nil {
				t.Fatalf("ParseExpr returned error: %v", err)
			}
			aggregate := expr.(*parser.AggregateExpr)

			start := time.Unix(1700000000, 0).UTC()
			end := time.Unix(1700000060, 0).UTC()
			source, _, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, start, end, 15*time.Second)
			if !ok {
				t.Fatal("aggregateRangeSourceSQL returned ok=false")
			}
			if mint != tc.mint || maxt != end.UnixMilli() {
				t.Fatalf("mint/maxt = %d/%d", mint, maxt)
			}
			for _, want := range []string{tc.gridFunc, tc.windowSQL, tc.extraSQL} {
				if want != "" && !strings.Contains(source.gridExpr, want) {
					t.Fatalf("grid expression does not contain %q:\n%s", want, source.gridExpr)
				}
			}
		})
	}
}

func TestAggregateRangeSourceSQLAvgOverTime(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	s := newServer(cfg)
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`quantile by (topic, consumer_group) (0.5, avg_over_time(warpstream_consumer_group_estimated_lag{topic="events",consumer_group="events_ws"}[5m]))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	start := time.Unix(1700000000, 0).UTC()
	end := time.Unix(1700000060, 0).UTC()
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, start, end, 15*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if source.avgOverTime != 5*time.Minute {
		t.Fatalf("avgOverTime = %s, want 5m", source.avgOverTime)
	}
	if mint != start.Add(-5*time.Minute).UnixMilli()+1 || maxt != end.UnixMilli() {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}

	perSeries := rangeSourcePerSeriesSQL(source, "SELECT id, timestamp, value FROM samples")
	for _, want := range []string{
		"arrayReduce('avgOrNull'",
		"arrayFilter((v, ts_ms)",
		"range(toInt64(1700000000000), toInt64(1700000075000), toInt64(15000))",
		"groupArray((toUnixTimestamp64Milli(timestamp), value))",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(perSeries, want) {
			t.Fatalf("per-series SQL does not contain %q:\n%s", want, perSeries)
		}
	}
	if strings.Contains(perSeries, "length(vals) > 1") {
		t.Fatalf("avg_over_time must preserve single-sample windows:\n%s", perSeries)
	}

	sql := fastAggregateRangeSQL(cfg, aggregate, "SELECT id FROM selected", selector.LabelMatchers, source, "SELECT id, timestamp, value FROM samples", start, 15000, "quantileExact(0.5)(sample_value)")
	for _, want := range []string{
		"'events' AS `__group_0`",
		"'events_ws' AS `__group_1`",
		"GROUP BY `__group_0`, `__group_1`, idx",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("aggregate SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"group_labels", "label_index"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("aggregate SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestRewriteImplicitMetricsQLSubquerySteps(t *testing.T) {
	query := `quantile(1, sum_over_time(delta(foo{label="[5m]"}[1m])[5m]))`
	got := rewriteImplicitMetricsQLSubquerySteps(query, 30*time.Second)
	want := `quantile(1, sum_over_time(delta(foo{label="[5m]"}[1m])[5m:30s]))`
	if got != want {
		t.Fatalf("rewrite = %q, want %q", got, want)
	}
}

func TestAggregateRangeSourceSQLWrapsSumOverTimeSubquery(t *testing.T) {
	cfg := Config{
		LookbackDelta: 5 * time.Minute,
	}
	s := newServer(cfg)
	expr, rewritten, err := s.parseFastRangeExpr(`quantile(1, sum_over_time(delta(node_cpu_seconds_total{ready=~"true"}[1m])[5m]))`, 30*time.Second)
	if err != nil {
		t.Fatalf("parseFastRangeExpr returned error: %v", err)
	}
	if !rewritten {
		t.Fatal("parseFastRangeExpr should rewrite the MetricsQL implicit subquery step")
	}
	aggregate := expr.(*parser.AggregateExpr)

	start := time.Unix(1700000000, 0).UTC()
	end := time.Unix(1700000060, 0).UTC()
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, start, end, 30*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if source.sumOverTime == nil {
		t.Fatal("sum_over_time over a subquery should mark the range source for rollup wrapping")
	}
	if !source.gridStart.Equal(start.Add(-5*time.Minute)) || source.gridStep != 30*time.Second {
		t.Fatalf("input grid = start %s step %s", source.gridStart, source.gridStep)
	}
	if selector == nil || selector.Name != "node_cpu_seconds_total" {
		t.Fatalf("selector = %#v", selector)
	}
	if mint != 1699999640000 || maxt != 1700000060000 {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}

	sql := rangeSourcePerSeriesSQL(source, "SELECT id, timestamp, value FROM samples")
	for _, want := range []string{
		"timeSeriesDeltaToGrid",
		"arraySlice",
		"arraySum",
		"range(toUInt64(3))",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestAggregateRangeSourceSQLSumOverTimeDeltaUsesPreviousSample(t *testing.T) {
	cfg := Config{
		LookbackDelta:       5 * time.Minute,
		RemoteWriteInterval: 15 * time.Second,
	}
	s := newServer(cfg)
	expr, rewritten, err := s.parseFastRangeExpr(`quantile(1, sum_over_time(delta(node_cpu_seconds_total{ready=~"true"}[15s])[5m]))`, 15*time.Second)
	if err != nil {
		t.Fatalf("parseFastRangeExpr returned error: %v", err)
	}
	if !rewritten {
		t.Fatal("parseFastRangeExpr should rewrite the MetricsQL implicit subquery step")
	}
	aggregate := expr.(*parser.AggregateExpr)

	start := time.Unix(1700000000, 0).UTC()
	end := time.Unix(1700000060, 0).UTC()
	source, _, mint, maxt, ok := s.aggregateRangeSourceSQL(aggregate.Expr, start, end, 15*time.Second)
	if !ok {
		t.Fatal("aggregateRangeSourceSQL returned ok=false")
	}
	if source.sumOverTime == nil {
		t.Fatal("sum_over_time over a subquery should mark the range source for rollup wrapping")
	}
	if mint != start.Add(-5*time.Minute-30*time.Second).UnixMilli() || maxt != end.UnixMilli() {
		t.Fatalf("mint/maxt = %d/%d", mint, maxt)
	}
	if !strings.Contains(source.gridExpr, ", 15, 30)(timestamp, value)") {
		t.Fatalf("delta subquery grid should include one previous sample interval:\n%s", source.gridExpr)
	}
}

func TestNestedCountSamplesInstantSQLUsesLookbackWindow(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		LabelIndexTable:     "label_index",
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	sql, ok := nestedCountSamplesInstantSQL(cfg, selector.LabelMatchers, inner.Grouping, 1778398680000, 1778398980000, 1778398980000)
	if !ok {
		t.Fatal("nestedCountSamplesInstantSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name = 'node_cpu_seconds_total'",
		"timestamp >= fromUnixTimestamp64Milli(1778398680000, 'UTC') AND timestamp <= fromUnixTimestamp64Milli(1778398980000, 'UTC')",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"active_ids AS",
		"argMax(value, timestamp)",
		nonStaleSampleSQL("value"),
		"active_ids ANY LEFT JOIN group_labels USING id",
		"toFloat64(uniq(ifNull(`__group_0`, ''))) AS value",
		"SELECT toInt64(1778398980000) AS ts",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"range(toUInt64", "timeSeriesLastToGrid", "metrics_series"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestLatestSamplesForSelectedSeriesSQLUsesLookbackWindow(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		RemoteWriteInterval: 15 * time.Second,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`http_requests_total{job="api"}`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	selector := expr.(*parser.VectorSelector)

	sql := latestSamplesForSelectedSeriesSQL(cfg, selector.LabelMatchers, 1000, 2000)
	for _, want := range []string{
		"argMax(value, timestamp)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name = 'http_requests_total'",
		"id IN (SELECT id FROM selected_series)",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "timestamp = fromUnixTimestamp64Milli") {
		t.Fatalf("instant latest SQL should not require one exact sample bucket:\n%s", sql)
	}
}

func TestLatestSamplesForSelectedSeriesSQLSkipsSelectedIDsForMetricOnlyMatcher(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		RemoteWriteInterval: 15 * time.Second,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`http_requests_total`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	selector := expr.(*parser.VectorSelector)

	sql := latestSamplesForSelectedSeriesSQL(cfg, selector.LabelMatchers, 1000, 2000)
	for _, want := range []string{
		"argMax(value, timestamp)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name = 'http_requests_total'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "id IN (SELECT id FROM selected_series)") {
		t.Fatalf("metric-only latest SQL should use the sample metric_name key directly:\n%s", sql)
	}
}

func TestLatestSamplesForSelectedSeriesUnionSQLFiltersSelectedIDsAndMetricNames(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		RemoteWriteInterval: 15 * time.Second,
	}

	sql := latestSamplesForSelectedSeriesUnionSQL(cfg, []string{"up", "process_start_time_seconds"}, 1000, 2000)
	for _, want := range []string{
		"argMax(value, timestamp)",
		"timestamp >= fromUnixTimestamp64Milli(1000, 'UTC')",
		"timestamp <= fromUnixTimestamp64Milli(2000, 'UTC')",
		"metric_name IN ('up','process_start_time_seconds')",
		"id IN (SELECT id FROM selected_series)",
		nonStaleSampleSQL("value"),
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestSelectedSeriesExactMatcherUnionSQLUsesSingleLabelIndexScan(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (topic, consumer_group) (
		increase(warpstream_consumer_group_max_offset{topic="a",consumer_group="a_ws"}[5m]) / 5
		or
		increase(warpstream_consumer_group_max_offset{topic="b",consumer_group="b_ws"}[5m]) / 5
	)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	s := &Server{cfg: Config{LookbackDelta: 5 * time.Minute}}
	branches, ok := s.instantSeriesExprBranches(aggregate.Expr, time.Unix(1700000000, 0).UTC())
	if !ok {
		t.Fatal("instantSeriesExprBranches returned ok=false")
	}
	selectors := []*parser.VectorSelector{branches[0].selector, branches[1].selector}

	sql, ok := selectedSeriesExactMatcherUnionSQL(cfg, selectors, []string{"id"}, 0, 0)
	if !ok {
		t.Fatal("selectedSeriesExactMatcherUnionSQL returned ok=false")
	}
	for _, want := range []string{
		"`default`.`label_index` AS li",
		"INNER JOIN values('branch UInt32, full_mask UInt64, match_bit UInt64, metric_name String, label_name String, label_value String'",
		"(0, 3, 1, 'warpstream_consumer_group_max_offset', 'topic', 'a')",
		"(0, 3, 2, 'warpstream_consumer_group_max_offset', 'consumer_group', 'a_ws')",
		"li.metric_name = mr.metric_name",
		"li.label_name = mr.label_name",
		"li.label_value = mr.label_value",
		"metric_name = 'warpstream_consumer_group_max_offset'",
		"label_name IN ('topic','consumer_group')",
		"label_value IN ('a','a_ws','b','b_ws')",
		"groupBitOr(mr.match_bit) = full_mask",
		"topic",
		"consumer_group",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "`default`.`series`") {
		t.Fatalf("id-only exact matcher union should not scan series table:\n%s", sql)
	}
}

func TestSelectedSeriesExactMatcherUnionSQLFactorsSingleLabelValues(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (hostname) (
		increase(usage_user{hostname="host_0"}[5m]) / 5
		or
		increase(usage_user{hostname="host_1"}[5m]) / 5
	)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	selectors := branchSelectors(t, aggregate.Expr)

	sql, ok := selectedSeriesExactMatcherUnionSQL(cfg, selectors, []string{"id"}, 0, 0)
	if !ok {
		t.Fatal("selectedSeriesExactMatcherUnionSQL returned ok=false")
	}
	for _, want := range []string{
		"`default`.`label_index`",
		"metric_name = 'usage_user'",
		"label_name = 'hostname'",
		"label_value IN ('host_0','host_1')",
		"GROUP BY id",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"values(", "`default`.`series`", "uniqExact"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestSelectedSeriesSQLOnlyReadsRequestedColumns(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		TeamID:          42,
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "usage_user"),
		labels.MustNewMatcher(labels.MatchEqual, "hostname", "host_42"),
	}
	sql, ok := selectedSeriesSQL(cfg, matchers, 1000, 2000, []string{"id"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	for _, want := range []string{
		"SELECT id FROM `default`.`label_index`",
		"metric_name = 'usage_user'",
		"label_name = 'hostname'",
		"label_value = 'host_42'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"`default`.`series`", "labels_json", "min_time", "max_time"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestSelectedSeriesIDSQLIntersectsLabelIndexMatchers(t *testing.T) {
	cfg := Config{CHDatabase: "default", SeriesTable: "series", LabelIndexTable: "label_index", TeamID: 42}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "usage_user"),
		labels.MustNewMatcher(labels.MatchRegexp, "type", "offline|online"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "clickhouse"),
		labels.MustNewMatcher(labels.MatchNotEqual, "shard", "test"),
	}

	sql, ok := selectedSeriesSQL(cfg, matchers, 1000, 2000, []string{"id"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	for _, want := range []string{
		"label_name = 'job' AND label_value = 'clickhouse'",
		"id IN (SELECT id FROM `default`.`label_index`",
		"label_name = 'type' AND label_value IN ('offline','online')",
		"id NOT IN (SELECT id FROM `default`.`label_index`",
		"label_name = 'shard' AND label_value = 'test'",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
}

func TestSelectedSeriesExactMatcherUnionSQLRejectsNonExactMatchers(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job=~"api|worker"} or up{job="db"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	selectors := branchSelectors(t, aggregate.Expr)
	if sql, ok := selectedSeriesExactMatcherUnionSQL(Config{}, selectors, []string{"id"}, 0, 0); ok {
		t.Fatalf("selectedSeriesExactMatcherUnionSQL returned ok=true with SQL:\n%s", sql)
	}
}

func TestInstantSeriesExprBranchesSupportRangeFunctionScalarUnion(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (topic, consumer_group) (
		increase(warpstream_consumer_group_max_offset{topic="a",consumer_group="a_ws"}[5m]) / 5
		or
		increase(warpstream_consumer_group_max_offset{topic="b",consumer_group="b_ws"}[5m]) / 5
	)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	branches, ok := s.instantSeriesExprBranches(aggregate.Expr, time.Unix(1700000000, 0).UTC())
	if !ok {
		t.Fatal("instantSeriesExprBranches returned ok=false")
	}
	if len(branches) != 2 {
		t.Fatalf("branch count = %d, want 2", len(branches))
	}
	if !branches[0].equivalentTo(branches[1]) {
		t.Fatalf("branches should have equivalent range-function/scalar shape: %#v vs %#v", branches[0], branches[1])
	}
	if branches[0].kind != instantSeriesRangeFunction || branches[0].transform.identity() {
		t.Fatalf("branch did not capture range-function scalar transform: %#v", branches[0])
	}
	source, sourceName := s.instantSeriesExprSourceSQL(branches[0], []*parser.VectorSelector{branches[0].selector, branches[1].selector})
	if sourceName != "per_series" {
		t.Fatalf("sourceName = %q, want per_series", sourceName)
	}
	for _, want := range []string{
		"`default`.`samples`",
		"metric_name = 'warpstream_consumer_group_max_offset'",
		"id IN (SELECT id FROM selected_series)",
		"arraySort",
		"reset_correction",
		"SELECT id, (value / 5) AS value",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("source SQL does not contain %q:\n%s", want, source)
		}
	}
}

func TestInstantSeriesExprRateFunctionsUsePromQLRange(t *testing.T) {
	cfg := Config{
		CHDatabase:          "default",
		SamplesTable:        "samples",
		SeriesTable:         "series",
		LabelIndexTable:     "label_index",
		LookbackDelta:       5 * time.Minute,
		RemoteWriteInterval: 15 * time.Second,
		TeamID:              42,
	}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	evalTime := time.Unix(1700000000, 0).UTC()

	for _, tc := range []struct {
		name      string
		query     string
		mint      int64
		sourceSQL []string
	}{
		{
			name:  "rate",
			query: `sum(rate(up{job="api"}[15s]))`,
			mint:  evalTime.Add(-15 * time.Second).UnixMilli(),
			sourceSQL: []string{
				"timestamp >= fromUnixTimestamp64Milli(1699999985000, 'UTC')",
				"/ 15",
			},
		},
		{
			name:  "irate",
			query: `sum(irate(up{job="api"}[15s]))`,
			mint:  evalTime.Add(-15 * time.Second).UnixMilli(),
			sourceSQL: []string{
				"timestamp >= fromUnixTimestamp64Milli(1699999985000, 'UTC')",
				"vals[-2] AS prev_v",
			},
		},
		{
			name:  "increase",
			query: `sum(increase(up{job="api"}[5m]))`,
			mint:  evalTime.Add(-300 * time.Second).UnixMilli(),
			sourceSQL: []string{
				"timestamp >= fromUnixTimestamp64Milli(1699999700000, 'UTC')",
				"/ 300",
				"value * 300",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := p.ParseExpr(tc.query)
			if err != nil {
				t.Fatalf("ParseExpr returned error: %v", err)
			}
			aggregate := expr.(*parser.AggregateExpr)
			branches, ok := s.instantSeriesExprBranches(aggregate.Expr, evalTime)
			if !ok || len(branches) != 1 {
				t.Fatalf("instantSeriesExprBranches returned %#v ok=%v, want one branch", branches, ok)
			}
			if branches[0].window.mint != tc.mint || branches[0].window.maxt != evalTime.UnixMilli() {
				t.Fatalf("window = %#v", branches[0].window)
			}
			source, sourceName := s.instantSeriesExprSourceSQL(branches[0], []*parser.VectorSelector{branches[0].selector})
			if sourceName != "per_series" {
				t.Fatalf("sourceName = %q, want per_series", sourceName)
			}
			for _, want := range tc.sourceSQL {
				if !strings.Contains(source, want) {
					t.Fatalf("source SQL does not contain %q:\n%s", want, source)
				}
			}
		})
	}
}

func TestRangeUnionSupportsRangeFunctionScalarBranches(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (topic, consumer_group) (
		increase(warpstream_consumer_group_max_offset{topic="a",consumer_group="a_ws"}[5m]) / 5
		or
		increase(warpstream_consumer_group_max_offset{topic="b",consumer_group="b_ws"}[5m]) / 5
	)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	branches, ok := seriesExprBranches(aggregate.Expr)
	if !ok {
		t.Fatal("seriesExprBranches returned ok=false")
	}
	if len(branches) != 2 {
		t.Fatalf("branch count = %d, want 2", len(branches))
	}
	if !branches[0].equivalentTo(branches[1]) {
		t.Fatalf("branches should have equivalent range-function/scalar shape: %#v vs %#v", branches[0], branches[1])
	}

	start := time.Unix(1700000000, 0).UTC()
	end := start.Add(time.Hour)
	step := time.Minute
	source, mint, maxt, ok := s.rangeUnionBranchSource(branches[0], start, end, step)
	if !ok {
		t.Fatal("rangeUnionBranchSource returned ok=false")
	}
	otherSource, otherMint, otherMaxt, ok := s.rangeUnionBranchSource(branches[1], start, end, step)
	if !ok || otherMint != mint || otherMaxt != maxt || otherSource.gridExpr != source.gridExpr {
		t.Fatalf("equivalent branches should produce the same grid source: %q vs %q", source.gridExpr, otherSource.gridExpr)
	}
	if !source.filterStaleSamples {
		t.Fatal("range-function branch should filter stale samples")
	}
	for _, want := range []string{"timeSeriesRateToGrid", "x * 300", "(x / 5)"} {
		if !strings.Contains(source.gridExpr, want) {
			t.Fatalf("grid expression does not contain %q:\n%s", want, source.gridExpr)
		}
	}

	selectors := branchSelectors(t, aggregate.Expr)
	selectedSeries, groupValues, ok := selectedSeriesExactMatcherUnionBranchesSQL(cfg, selectors, aggregate.Grouping, mint, maxt)
	if !ok {
		t.Fatal("selectedSeriesExactMatcherUnionBranchesSQL returned ok=false")
	}
	// Branch resolution must intersect with the series alive in the window;
	// without it a high-cardinality metric resolves its whole lifetime.
	if !strings.Contains(selectedSeries, "li.id IN (SELECT id FROM `default`.`samples`") {
		t.Fatalf("branch SQL does not restrict to active series:\n%s", selectedSeries)
	}
	wantGroupValues := [][]string{{"a", "b"}, {"a_ws", "b_ws"}}
	if !reflect.DeepEqual(groupValues, wantGroupValues) {
		t.Fatalf("group values = %#v, want %#v", groupValues, wantGroupValues)
	}
	where := []string{teamFilter(cfg)}
	where = append(where, sampleTimeFilters(cfg, mint, maxt)...)
	where = append(where, metricNamesCondition(exactMetricNamesForSelectors(selectors)))
	sampleSource := rawSamplesSourceSQL(cfg, strings.Join(where, " AND "))
	sql := fastAggregateRangeExactUnionSQL(cfg, selectedSeries, groupValues, source, sampleSource, start, step.Milliseconds(), "sum(sample_value)")
	for _, want := range []string{
		"`default`.`samples`",
		"metric_name = 'warpstream_consumer_group_max_offset'",
		"min(branch) AS branch",
		"ALL INNER JOIN selected_series USING id",
		"timeSeriesRateToGrid",
		"(x / 5)",
		"arrayElement(['a','b'], toUInt64(branch) + 1) AS `__group_0`",
		"arrayElement(['a_ws','b_ws'], toUInt64(branch) + 1) AS `__group_1`",
		"GROUP BY `__group_0`, `__group_1`, idx",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"group_labels", "id IN (SELECT id FROM selected_series)"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
	if sql, _, ok := selectedSeriesExactMatcherUnionBranchesSQL(cfg, selectors, []string{"instance"}, 0, 0); ok {
		t.Fatalf("unknown grouping label returned ok=true with SQL:\n%s", sql)
	}
}

func TestRangeUnionRejectsMixedBranches(t *testing.T) {
	s := &Server{cfg: Config{LookbackDelta: 5 * time.Minute}}
	p := parser.NewParser(parser.Options{})
	start := time.Unix(1700000000, 0).UTC()
	for _, query := range []string{
		`sum(increase(up{job="api"}[5m]) / 5 or rate(up{job="worker"}[5m]))`,
		`sum(increase(up{job="api"}[5m]) / 5 or increase(up{job="worker"}[10m]) / 5)`,
		`sum(increase(up{job="api"}[5m]) / 5 or increase(up{job="worker"}[5m]) / 6)`,
		`sum(increase(up{job="api"}[5m]) / 5 or increase(up{job="worker"}[5m] offset 1m) / 5)`,
	} {
		expr, err := p.ParseExpr(query)
		if err != nil {
			t.Fatalf("ParseExpr(%q) returned error: %v", query, err)
		}
		aggregate := expr.(*parser.AggregateExpr)
		if _, handled, err := s.tryFastAggregateRangeUnionQuery(context.Background(), aggregate, start, start.Add(time.Hour), time.Minute, time.Minute.Milliseconds(), "sum(sample_value)"); handled || err != nil {
			t.Fatalf("tryFastAggregateRangeUnionQuery(%q) handled=%v err=%v, want unhandled", query, handled, err)
		}
	}
}

func TestRangeUnionSupportsSingleScalarTransformBranch(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum by (job) (increase(up{job="api"}[5m]) / 5)`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	branches, ok := seriesExprBranches(aggregate.Expr)
	if !ok || len(branches) != 1 {
		t.Fatalf("branches = %#v ok=%v, want one branch", branches, ok)
	}
	if branches[0].transform.identity() {
		t.Fatal("branch should capture the scalar transform")
	}

	start := time.Unix(1700000000, 0).UTC()
	source, _, _, ok := s.rangeUnionBranchSource(branches[0], start, start.Add(time.Hour), time.Minute)
	if !ok {
		t.Fatal("rangeUnionBranchSource returned ok=false")
	}
	if !strings.Contains(source.gridExpr, "(x / 5)") {
		t.Fatalf("grid expression does not apply transform:\n%s", source.gridExpr)
	}
}

func TestInstantSeriesExprBranchesRejectMixedUnionComputations(t *testing.T) {
	cfg := Config{LookbackDelta: 5 * time.Minute}
	s := &Server{cfg: cfg}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(increase(up{job="api"}[5m]) / 5 or rate(up{job="worker"}[5m]))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	branches, ok := s.instantSeriesExprBranches(aggregate.Expr, time.Unix(1700000000, 0).UTC())
	if !ok {
		t.Fatal("instantSeriesExprBranches returned ok=false")
	}
	if len(branches) != 2 {
		t.Fatalf("branch count = %d, want 2", len(branches))
	}
	if branches[0].equivalentTo(branches[1]) {
		t.Fatalf("mixed increase/division and rate branches should not be equivalent: %#v vs %#v", branches[0], branches[1])
	}
}

func TestExactMetricNamesForSelectorsRequiresExactMetrics(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job="api"} or process_start_time_seconds{job="api"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	names := exactMetricNamesForSelectors(branchSelectors(t, aggregate.Expr))
	if got, want := strings.Join(names, ","), "up,process_start_time_seconds"; got != want {
		t.Fatalf("exactMetricNamesForSelectors = %q, want %q", got, want)
	}

	expr, err = p.ParseExpr(`sum({__name__=~"up|process_start_time_seconds", job="api"} or up{job="worker"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate = expr.(*parser.AggregateExpr)
	if names := exactMetricNamesForSelectors(branchSelectors(t, aggregate.Expr)); names != nil {
		t.Fatalf("exactMetricNamesForSelectors = %#v, want nil", names)
	}
}

func TestCommonExactMetricMatchersRequiresOneMetric(t *testing.T) {
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`sum(up{job="api"} or up{job="worker"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate := expr.(*parser.AggregateExpr)
	if matchers := commonExactMetricMatchers(branchSelectors(t, aggregate.Expr)); matchers == nil {
		t.Fatal("commonExactMetricMatchers returned nil for same-metric union")
	}

	expr, err = p.ParseExpr(`sum(up{job="api"} or process_start_time_seconds{job="worker"})`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	aggregate = expr.(*parser.AggregateExpr)
	if matchers := commonExactMetricMatchers(branchSelectors(t, aggregate.Expr)); matchers != nil {
		t.Fatalf("commonExactMetricMatchers = %#v, want nil for mixed metrics", matchers)
	}
}

func branchSelectors(t *testing.T, expr parser.Expr) []*parser.VectorSelector {
	t.Helper()
	branches, ok := seriesExprBranches(expr)
	if !ok {
		t.Fatal("seriesExprBranches returned ok=false")
	}
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	for _, branch := range branches {
		selectors = append(selectors, branch.selector)
	}
	return selectors
}

func TestPostHogSampleGroupSQLSupportsMapLabels(t *testing.T) {
	_, _, _, ok := postHogSampleGroupSQL([]string{"service_name", "__name__"})
	if !ok {
		t.Fatal("expected physical posthog grouping labels to be supported")
	}
	_, _, perIDSelects, ok := postHogSampleGroupSQL([]string{"region"})
	if !ok {
		t.Fatal("expected ordinary posthog labels to be grouped from maps")
	}
	if got := strings.Join(perIDSelects, ", "); !strings.Contains(got, "attributes_map_str['region__str']") || !strings.Contains(got, "resource_attributes['region']") {
		t.Fatalf("expected map-backed group expression, got %s", got)
	}
}

func TestNestedCountBitmapRangeSQLUsesPostingBitmaps(t *testing.T) {
	cfg := Config{
		CHDatabase:         "default",
		LabelPostingsTable: "label_postings",
		ActivityTable:      "series_activity",
		LookbackDelta:      5 * time.Minute,
		TeamID:             42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	stepMillis := time.Minute.Milliseconds()
	sql, ok := nestedCountBitmapRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 61, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountBitmapRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`label_postings`",
		"`default`.`series_activity`",
		"metric_ids AS",
		"selected_ids AS",
		"active_by_step AS",
		"active_selected AS",
		"group_label_values AS",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"groupBitmapOrState(ids)",
		"bitmapAndCardinality",
		"range(toUInt64(61))",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"metrics_samples", "timeSeriesLastToGrid", "metrics_series", "metrics_label_index", "type"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountSamplesRangeSQLUsesLastGridAndLabelIndex(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count(node_cpu_seconds_total{type=~".*", ready=~"true"}) by (cpu))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	stepMillis := time.Minute.Milliseconds()
	sql, ok := nestedCountSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, start, start, stepMillis, 61, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name = 'node_cpu_seconds_total'",
		"label_name = 'ready'",
		"label_name = 'cpu'",
		"group_labels AS",
		"per_series AS",
		"timeSeriesLastToGrid",
		"arrayMap(x -> if(isNull(x) OR",
		"active_groups AS",
		"arrayEnumerate(vals) AS idx",
		nonStaleNullableSampleSQL("sample_value"),
		"ANY LEFT JOIN",
		"GROUP BY idx, `__group_0`",
		"toFloat64(count()) AS value",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"series_activity", "label_postings", "groupBitmap", "active_ids AS", "argMax(value, ts)", "range(toUInt64"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}

func TestNestedCountSelectedSamplesRangeSQLUsesLastGridFallback(t *testing.T) {
	cfg := Config{
		CHDatabase:      "default",
		SamplesTable:    "samples",
		SeriesTable:     "series",
		LabelIndexTable: "label_index",
		LookbackDelta:   5 * time.Minute,
		TeamID:          42,
	}
	p := parser.NewParser(parser.Options{})
	expr, err := p.ParseExpr(`count(count({__name__=~"node_.*", ready=~"true"}) by (cpu, mode))`)
	if err != nil {
		t.Fatalf("ParseExpr returned error: %v", err)
	}
	outer := expr.(*parser.AggregateExpr)
	inner := outer.Expr.(*parser.AggregateExpr)
	selector := inner.Expr.(*parser.VectorSelector)

	start := time.Unix(1778398980, 0).UTC()
	end := time.Unix(1778402580, 0).UTC()
	step := time.Minute
	stepMillis := step.Milliseconds()
	source, mint, maxt, ok := selectorRangeGridSource(cfg, selector, start, end, step)
	if !ok {
		t.Fatal("selectorRangeGridSource returned ok=false")
	}
	selectedSeries, ok := selectedSeriesSQL(cfg, selector.LabelMatchers, mint, maxt, []string{"id", "min_time", "max_time"})
	if !ok {
		t.Fatal("selectedSeriesSQL returned ok=false")
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	sql, ok := nestedCountSelectedSamplesRangeSQL(cfg, selector.LabelMatchers, inner.Grouping, selectedSeries, source.start, start, stepMillis, steps, cfg.LookbackDelta)
	if !ok {
		t.Fatal("nestedCountSelectedSamplesRangeSQL returned ok=false")
	}

	for _, want := range []string{
		"selected_series AS",
		"`default`.`series`",
		"`default`.`samples`",
		"`default`.`label_index`",
		"metric_name",
		"min(min_time) AS min_time",
		"max(max_time) AS max_time",
		"max_time >= fromUnixTimestamp64Milli(",
		"min_time <= fromUnixTimestamp64Milli(",
		"id IN (SELECT id FROM selected_series)",
		"label_name IN ('cpu', 'mode')",
		"group_labels AS",
		"per_series AS",
		"timeSeriesLastToGrid",
		"active_groups AS",
		"arrayEnumerate(vals) AS idx",
		nonStaleNullableSampleSQL("sample_value"),
		"GROUP BY idx, `__group_0`, `__group_1`",
		"toFloat64(count()) AS value",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL does not contain %q:\n%s", want, sql)
		}
	}
	for _, notWant := range []string{"series_activity", "label_postings", "groupBitmap", "active_ids AS", "argMax(value, ts)", "range(toUInt64"} {
		if strings.Contains(sql, notWant) {
			t.Fatalf("SQL contains %q:\n%s", notWant, sql)
		}
	}
}
