package snuffle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func (s *Server) tryFastInstantQuery(ctx context.Context, query string, evalTime time.Time) (queryData, bool, error) {
	expr, err := s.parser.ParseExpr(query)
	if err != nil {
		return queryData{}, false, nil
	}

	if aggregate, ok := expr.(*parser.AggregateExpr); ok {
		if s.cfg.postHogSchemaLayout() {
			if data, ok, err := s.tryPostHogNestedCountInstantQuery(ctx, aggregate, evalTime); ok || err != nil {
				return data, ok, err
			}
			if data, ok, err := s.tryFastAggregate(ctx, aggregate, evalTime); ok || err != nil {
				return data, ok, err
			}
			if data, ok, err := s.tryFastTopK(ctx, aggregate, evalTime); ok || err != nil {
				return data, ok, err
			}
			return queryData{}, false, nil
		}
		if data, ok, err := s.tryFastNestedCountInstantQuery(ctx, aggregate, evalTime); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastAggregate(ctx, aggregate, evalTime); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastTopK(ctx, aggregate, evalTime); ok || err != nil {
			return data, ok, err
		}
	}
	if exprHasFunction(expr, "running_sum") {
		return queryData{}, true, errors.New("MetricsQL running_sum is only supported for range queries")
	}
	return queryData{}, false, nil
}

func (s *Server) tryFastRangeQuery(ctx context.Context, query string, start, end time.Time, step time.Duration) (queryData, bool, error) {
	if step <= 0 {
		return queryData{}, false, nil
	}
	stepMillis := step.Milliseconds()
	if stepMillis <= 0 {
		return queryData{}, false, nil
	}
	expr, hasMetricsQLRewrite, err := s.parseFastRangeExpr(query, step)
	if err != nil {
		return queryData{}, false, nil
	}
	if s.cfg.postHogSchemaLayout() {
		if selector, ok := expr.(*parser.VectorSelector); ok {
			return s.tryPostHogFastSelectorRangeQuery(ctx, selector, start, end, step, stepMillis)
		}
		if aggregate, ok := expr.(*parser.AggregateExpr); ok {
			if data, ok, err := s.tryPostHogNestedCountRangeQuery(ctx, aggregate, start, end, step, stepMillis); ok || err != nil {
				return data, ok, err
			}
			if data, ok, err := s.tryPostHogFastAggregateRangeQuery(ctx, aggregate, start, end, step, stepMillis); ok || err != nil {
				return data, ok, err
			}
			if data, ok, err := s.tryPostHogAggregateRangeUnionQuery(ctx, aggregate, start, end, step); ok || err != nil {
				return data, ok, err
			}
		}
		if hasMetricsQLRewrite || exprHasFunction(expr, "running_sum") {
			return queryData{}, true, errors.New("MetricsQL extensions are only supported in fast range queries when they wrap a selector or supported range function")
		}
		return queryData{}, false, nil
	}
	if selector, ok := expr.(*parser.VectorSelector); ok {
		return s.tryFastSelectorRangeQuery(ctx, selector, start, end, step, stepMillis)
	}
	if aggregate, ok := expr.(*parser.AggregateExpr); ok {
		if data, ok, err := s.tryFastNestedCountRangeQuery(ctx, aggregate, start, end, step, stepMillis); ok || err != nil {
			return data, ok, err
		}
		if data, ok, err := s.tryFastAggregateRangeQuery(ctx, aggregate, start, end, step, stepMillis); ok || err != nil {
			return data, ok, err
		}
	}
	hasRunningSum := exprHasFunction(expr, "running_sum")
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(expr, start, end, step)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		if hasRunningSum || hasMetricsQLRewrite {
			return queryData{}, true, errors.New("MetricsQL extensions are only supported in fast range queries when they wrap a selector or supported range function")
		}
		return queryData{}, false, nil
	}

	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return queryData{}, false, nil
	}
	selectedSeries += fmt.Sprintf(" LIMIT %d", s.cfg.MaxSeries)
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt)

	perSeries := rangeSourcePerSeriesSQL(source, sampleSource)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT id, metric_name, labels_json, per_series.vals AS vals FROM per_series ANY INNER JOIN selected_series USING id ORDER BY id",
		selectedSeries,
		perSeries,
	)

	results, err := s.queryRangeGridResults(ctx, sql, selector.LabelMatchers, start, end, stepMillis)
	if err != nil {
		return queryData{}, true, err
	}
	if len(results) >= s.cfg.MaxSeries {
		return queryData{}, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) parseFastRangeExpr(query string, step time.Duration) (parser.Expr, bool, error) {
	expr, err := s.parser.ParseExpr(query)
	if err == nil {
		return expr, false, nil
	}
	rewritten := rewriteImplicitMetricsQLSubquerySteps(query, step)
	if rewritten == query {
		return nil, false, err
	}
	expr, err = s.parser.ParseExpr(rewritten)
	if err != nil {
		return nil, false, err
	}
	return expr, true, nil
}

func rewriteImplicitMetricsQLSubquerySteps(query string, step time.Duration) string {
	stepText := promDuration(step)
	if stepText == "" {
		return query
	}
	var out strings.Builder
	last := 0
	changed := false
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '"', '\'', '`':
			i = skipQuoted(query, i)
		case '[':
			prev := previousNonSpace(query, i-1)
			if prev < 0 || query[prev] != ')' {
				continue
			}
			end := strings.IndexByte(query[i+1:], ']')
			if end < 0 {
				continue
			}
			end += i + 1
			window := strings.TrimSpace(query[i+1 : end])
			if strings.Contains(window, ":") || !looksLikeDuration(window) {
				continue
			}
			out.WriteString(query[last:end])
			out.WriteByte(':')
			out.WriteString(stepText)
			last = end
			changed = true
			i = end
		}
	}
	if !changed {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

func skipQuoted(query string, start int) int {
	quote := query[start]
	for i := start + 1; i < len(query); i++ {
		if quote != '`' && query[i] == '\\' {
			i++
			continue
		}
		if query[i] == quote {
			return i
		}
	}
	return len(query) - 1
}

func previousNonSpace(query string, idx int) int {
	for idx >= 0 {
		switch query[idx] {
		case ' ', '\t', '\n', '\r':
			idx--
		default:
			return idx
		}
	}
	return -1
}

func looksLikeDuration(value string) bool {
	if value == "" {
		return false
	}
	hasDigit := false
	for _, r := range value {
		if r >= '0' && r <= '9' {
			hasDigit = true
			continue
		}
		if r == '.' || r == 'y' || r == 'w' || r == 'd' || r == 'h' || r == 'm' || r == 's' {
			continue
		}
		return false
	}
	return hasDigit
}

func promDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

func (s *Server) tryFastSelectorRangeQuery(ctx context.Context, selector *parser.VectorSelector, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	if selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	mint := shiftedStart.Add(-s.cfg.LookbackDelta).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return queryData{}, false, nil
	}
	selectedSeries += fmt.Sprintf(" LIMIT %d", s.cfg.MaxSeries)
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt)
	gridExpr := lastGridExpr(shiftedStart, shiftedEnd, step, s.cfg.LookbackDelta)
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (%s) GROUP BY id",
		gridExpr,
		sampleSource,
	)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT id, metric_name, labels_json, per_series.vals AS vals FROM per_series ANY INNER JOIN selected_series USING id ORDER BY id",
		selectedSeries,
		perSeries,
	)

	results, err := s.queryRangeGridResults(ctx, sql, selector.LabelMatchers, start, end, stepMillis)
	if err != nil {
		return queryData{}, true, err
	}
	if len(results) >= s.cfg.MaxSeries {
		return queryData{}, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) queryRangeGridResults(ctx context.Context, sql string, matchers []*labels.Matcher, start, end time.Time, stepMillis int64) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 1024)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var labelsJSON string
		var vals []*float64
		if err := row.Scan(&id, &metricName, &labelsJSON, &vals); err != nil {
			return err
		}
		_ = id
		metric, ok, err := metricLabelMap(metricName, []byte(labelsJSON), matchers)
		if err != nil || !ok {
			return err
		}
		values := make([][]any, 0, len(vals))
		for i, value := range vals {
			if value == nil || isStaleSampleValue(*value) {
				continue
			}
			ts := start.UnixMilli() + int64(i)*stepMillis
			if ts > end.UnixMilli() {
				break
			}
			values = append(values, []any{float64(ts) / 1000, formatSample(*value)})
		}
		if len(values) > 0 {
			results = append(results, sampleResult{Metric: metric, Values: values})
		}
		return nil
	})
	return results, err
}

type rangeGridFunctionSpec struct {
	name               string
	increase           bool
	usesPreviousSample bool
}

const maxAggregateUnionSelectors = 1024

func rangeGridFunction(name string) (rangeGridFunctionSpec, bool) {
	switch name {
	case "rate":
		return rangeGridFunctionSpec{name: "timeSeriesRateToGrid"}, true
	case "irate":
		return rangeGridFunctionSpec{name: "timeSeriesInstantRateToGrid"}, true
	case "increase":
		return rangeGridFunctionSpec{name: "timeSeriesRateToGrid", increase: true}, true
	case "delta":
		return rangeGridFunctionSpec{name: "timeSeriesDeltaToGrid", usesPreviousSample: true}, true
	case "idelta":
		return rangeGridFunctionSpec{name: "timeSeriesInstantDeltaToGrid", usesPreviousSample: true}, true
	default:
		return rangeGridFunctionSpec{}, false
	}
}

func (s *Server) tryFastAggregate(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "value")
	if !ok {
		return queryData{}, false, nil
	}
	if s.cfg.postHogSchemaLayout() {
		if data, ok, err := s.tryPostHogInstantAggregateUnionQuery(ctx, expr, evalTime); ok || err != nil {
			return data, ok, err
		}
		selector, ok := expr.Expr.(*parser.VectorSelector)
		if !ok {
			return queryData{}, false, nil
		}
		window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
		if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		return s.tryPostHogInstantAggregate(ctx, expr, selector, window, evalTime, aggSQL)
	}
	if data, ok, err := s.tryFastInstantSeriesExprAggregate(ctx, expr, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}
	selector, ok := expr.Expr.(*parser.VectorSelector)
	if !ok {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}

	if data, ok, err := s.tryPostHogInstantAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}
	if data, ok, err := s.trySampleLabelIndexInstantAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}
	if data, ok, err := s.tryLabelIndexAggregate(ctx, expr, selector, window, evalTime, aggSQL); ok || err != nil {
		return data, ok, err
	}

	return queryData{}, false, nil
}

func (s *Server) tryPostHogInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	if !s.cfg.postHogSchemaLayout() {
		return queryData{}, false, nil
	}
	if sampleMillis, ok := postHogExactInstantSampleMillis(s.cfg, selector, evalTime); ok {
		return s.tryPostHogExactInstantAggregate(ctx, expr, selector, sampleMillis, evalTime, aggSQL)
	}
	groupSelect, groupBy, perIDGroupSelect, ok := postHogSampleGroupSQL(expr.Grouping)
	if !ok {
		return queryData{}, false, nil
	}

	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, window.mint, window.maxt)

	perIDSelect := []string{
		postHogSeriesIDExpr() + " AS series_id",
		"argMax(value, timestamp) AS value",
	}
	perIDSelect = append(perIDSelect, perIDGroupSelect...)
	perID := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY series_id",
		strings.Join(perIDSelect, ", "),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	perID = fmt.Sprintf("SELECT * FROM (%s) WHERE %s", perID, nonStaleSampleSQL("value"))

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	sql := fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(selectParts, ", "), perID)
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryPostHogExactInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, sampleMillis int64, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	groupSelect, groupBy, ok := postHogRangeGroupSQL(expr.Grouping)
	if !ok {
		return queryData{}, false, nil
	}
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, sampleMillis, sampleMillis)

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS agg_value")

	sql := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		strings.Join(selectParts, ", "),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	sql += " AND " + nonStaleSampleSQL("value")
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryPostHogFastAggregateRangeQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, _ int64) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "value")
	if !ok {
		return queryData{}, false, nil
	}
	selector, ok := expr.Expr.(*parser.VectorSelector)
	if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) || !exactBucketRangeSelector(s.cfg, selector, start, end, step) {
		return queryData{}, false, nil
	}
	groupSelect, groupBy, ok := postHogRangeGroupSQL(expr.Grouping)
	if !ok {
		return queryData{}, false, nil
	}

	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, start.UnixMilli(), end.UnixMilli())
	where = append(where, nonStaleSampleSQL("value"))
	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toUnixTimestamp64Milli(timestamp) AS ts")
	selectParts = append(selectParts, aggSQL+" AS agg_value")

	groupByParts := append([]string{}, groupBy...)
	groupByParts = append(groupByParts, "ts")
	orderByParts := append([]string{}, groupBy...)
	orderByParts = append(orderByParts, "ts")

	sql := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY %s ORDER BY %s",
		strings.Join(selectParts, ", "),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
		strings.Join(groupByParts, ", "),
		strings.Join(orderByParts, ", "),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

// postHogAggregateUnionPlan describes one ClickHouse query for an
// `agg by (...) (branch or branch or ...)` expression over the PostHog layout,
// where every branch is an equivalent selector or range-function expression.
type postHogAggregateUnionPlan struct {
	gridVals string
	where    []string
	groupAny []string
}

func (s *Server) tryPostHogInstantAggregateUnionQuery(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "sample_value")
	if !ok {
		return queryData{}, false, nil
	}
	plan, ok := s.postHogAggregateUnionPlan(expr, evalTime, evalTime, time.Second)
	if !ok {
		return queryData{}, false, nil
	}
	sql := postHogAggregateUnionSQL(s.cfg, plan, expr.Grouping, evalTime, time.Second.Milliseconds(), aggSQL)
	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryPostHogAggregateRangeUnionQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "sample_value")
	if !ok {
		return queryData{}, false, nil
	}
	plan, ok := s.postHogAggregateUnionPlan(expr, start, end, step)
	if !ok {
		return queryData{}, false, nil
	}
	sql := postHogAggregateUnionSQL(s.cfg, plan, expr.Grouping, start, step.Milliseconds(), aggSQL)
	results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) postHogAggregateUnionPlan(expr *parser.AggregateExpr, start, end time.Time, step time.Duration) (*postHogAggregateUnionPlan, bool) {
	branches, ok := seriesExprBranches(expr.Expr)
	if !ok || len(branches) == 0 || len(branches) > maxAggregateUnionSelectors {
		return nil, false
	}
	if len(branches) == 1 && branches[0].kind == seriesExprSelector && branches[0].transform.identity() {
		// the dedicated plain selector aggregate paths handle it
		return nil, false
	}

	first := branches[0]
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(first.node, start, end, step)
	if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
		return nil, false
	}
	if source.sumOverTime != nil || source.runningSumWrap {
		return nil, false
	}
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	selectors = append(selectors, first.selector)
	for _, branch := range branches[1:] {
		if !first.equivalentTo(branch) || !postHogMatchersPushdownSafe(branch.selector.LabelMatchers) {
			return nil, false
		}
		branchSource, _, branchMint, branchMaxt, ok := s.aggregateRangeSourceSQL(branch.node, start, end, step)
		if !ok || branchMint != mint || branchMaxt != maxt || branchSource.gridExpr != source.gridExpr {
			return nil, false
		}
		selectors = append(selectors, branch.selector)
	}

	matcherCondition, ok := postHogUnionMatcherCondition(selectors)
	if !ok {
		return nil, false
	}
	groupAny := make([]string, 0, len(expr.Grouping))
	for i, name := range expr.Grouping {
		groupExpr, ok := postHogSampleGroupExpr(name)
		if !ok {
			return nil, false
		}
		groupAny = append(groupAny, "any("+groupExpr+") AS "+quoteIdent(groupAlias(i)))
	}

	where := []string{teamFilter(s.cfg)}
	where = append(where, sampleTimeFilters(s.cfg, mint, maxt)...)
	if matcherCondition != "" {
		where = append(where, matcherCondition)
	}
	if source.filterStaleSamples {
		where = append(where, nonStaleSampleSQL("value"))
	}
	return &postHogAggregateUnionPlan{
		gridVals: transformGridValsSQL(source.gridExpr, first.transform),
		where:    where,
		groupAny: groupAny,
	}, true
}

func postHogAggregateUnionSQL(cfg Config, plan *postHogAggregateUnionPlan, grouping []string, start time.Time, stepMillis int64, aggSQL string) string {
	perSeriesSelects := make([]string, 0, len(plan.groupAny)+2)
	perSeriesSelects = append(perSeriesSelects, postHogSeriesIDExpr()+" AS series_id")
	perSeriesSelects = append(perSeriesSelects, plan.groupAny...)
	perSeriesSelects = append(perSeriesSelects, plan.gridVals+" AS vals")
	perSeries := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY series_id",
		strings.Join(perSeriesSelects, ", "),
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(plan.where, " AND "),
	)

	groupAliases := make([]string, 0, len(grouping))
	for i := range grouping {
		groupAliases = append(groupAliases, quoteIdent(groupAlias(i)))
	}
	selectParts := append([]string{}, groupAliases...)
	selectParts = append(selectParts, fmt.Sprintf("toInt64(%d) + (toInt64(idx) - 1) * %d AS ts", start.UnixMilli(), stepMillis))
	selectParts = append(selectParts, aggSQL+" AS value")
	groupByParts := append(append([]string{}, groupAliases...), "idx")

	sql := fmt.Sprintf(
		"WITH per_series AS (%s) SELECT %s FROM per_series ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE %s GROUP BY %s ORDER BY %s",
		perSeries,
		strings.Join(selectParts, ", "),
		nonStaleNullableSampleSQL("sample_value"),
		strings.Join(groupByParts, ", "),
		strings.Join(groupByParts, ", "),
	)
	return withMaxThreads(sql, cfg.AggregateThreads)
}

// postHogUnionMatcherCondition builds one WHERE condition matching rows
// selected by any of the union's selectors. Returns "" when a branch matches
// all rows.
func postHogUnionMatcherCondition(selectors []*parser.VectorSelector) (string, bool) {
	if condition, ok := postHogUnionInCondition(selectors); ok {
		return condition, true
	}
	branchConditions := make([]string, 0, len(selectors))
	for _, selector := range selectors {
		conditions, ok := postHogSelectorMatcherConditions(selector.LabelMatchers)
		if !ok {
			return "", false
		}
		if len(conditions) == 0 {
			return "", true
		}
		branchConditions = append(branchConditions, "("+strings.Join(conditions, " AND ")+")")
	}
	if len(branchConditions) == 1 {
		return branchConditions[0], true
	}
	return "(" + strings.Join(branchConditions, " OR ") + ")", true
}

func postHogSelectorMatcherConditions(matchers []*labels.Matcher) ([]string, bool) {
	conditions := make([]string, 0, len(matchers))
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
			continue
		}
		condition, ok := postHogMatcherCondition(matcher)
		if !ok {
			return nil, false
		}
		conditions = append(conditions, condition)
	}
	return conditions, true
}

// postHogUnionInCondition recognizes selectors that differ on exactly one
// equality matcher and emits `common AND label_expr IN (...)` so the per-row
// label lookup runs once instead of once per branch.
func postHogUnionInCondition(selectors []*parser.VectorSelector) (string, bool) {
	if len(selectors) < 2 {
		return "", false
	}
	matcherMaps := make([]map[string]*labels.Matcher, len(selectors))
	for i, selector := range selectors {
		matcherMap := make(map[string]*labels.Matcher, len(selector.LabelMatchers))
		for _, matcher := range selector.LabelMatchers {
			if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
				continue
			}
			if _, dup := matcherMap[matcher.Name]; dup {
				return "", false
			}
			matcherMap[matcher.Name] = matcher
		}
		if i > 0 && len(matcherMap) != len(matcherMaps[0]) {
			return "", false
		}
		matcherMaps[i] = matcherMap
	}

	names := make([]string, 0, len(matcherMaps[0]))
	for name := range matcherMaps[0] {
		names = append(names, name)
	}
	sort.Strings(names)

	varying := ""
	common := make([]string, 0, len(names))
	for _, name := range names {
		firstMatcher := matcherMaps[0][name]
		same := true
		for _, matcherMap := range matcherMaps[1:] {
			other, ok := matcherMap[name]
			if !ok {
				return "", false
			}
			if other.Type != firstMatcher.Type || other.Value != firstMatcher.Value {
				same = false
			}
		}
		if same {
			condition, ok := postHogMatcherCondition(firstMatcher)
			if !ok {
				return "", false
			}
			common = append(common, condition)
			continue
		}
		if varying != "" {
			return "", false
		}
		for _, matcherMap := range matcherMaps {
			if matcherMap[name].Type != labels.MatchEqual {
				return "", false
			}
		}
		varying = name
	}
	if varying == "" {
		return "", false
	}

	values := make([]string, 0, len(selectors))
	seen := make(map[string]struct{}, len(selectors))
	for _, matcherMap := range matcherMaps {
		value := matcherMap[varying].Value
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, sqlString(value))
	}
	var column string
	switch varying {
	case labels.MetricName:
		column = "metric_name"
	case "service_name":
		column = "service_name"
	default:
		column = postHogLabelValueExpr(varying)
	}
	conditions := append(common, column+" IN ("+strings.Join(values, ",")+")")
	return "(" + strings.Join(conditions, " AND ") + ")", true
}

func (s *Server) tryPostHogFastSelectorRangeQuery(ctx context.Context, selector *parser.VectorSelector, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	if selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	if exactBucketRangeSelector(s.cfg, selector, start, end, step) && postHogSelectorHasNonMetricMatcher(selector.LabelMatchers) {
		return s.tryPostHogExactSelectorRangeRows(ctx, selector, start, end, stepMillis)
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	mint := shiftedStart.Add(-s.cfg.LookbackDelta).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, mint, maxt)
	gridExpr := lastGridExpr(shiftedStart, shiftedEnd, step, s.cfg.LookbackDelta)
	sql := fmt.Sprintf(
		"SELECT %s AS series_id, any(metric_name) AS out_metric_name, any(service_name) AS out_service_name, any(resource_attributes) AS out_resource_attributes, any(attributes_map_str) AS out_attributes_map_str, %s AS vals FROM %s WHERE %s GROUP BY series_id ORDER BY series_id LIMIT %d",
		postHogSeriesIDExpr(),
		gridExpr,
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
		s.cfg.MaxSeries,
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results := make([]sampleResult, 0, 1024)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var serviceName string
		var resourceAttrs map[string]string
		var attrs map[string]string
		var vals []*float64
		if err := row.Scan(&id, &metricName, &serviceName, &resourceAttrs, &attrs, &vals); err != nil {
			return err
		}
		_ = id
		metric := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
		if !matchesAll(metric, selector.LabelMatchers) {
			return nil
		}
		values := make([][]any, 0, len(vals))
		for i, value := range vals {
			if value == nil || isStaleSampleValue(*value) {
				continue
			}
			ts := start.UnixMilli() + int64(i)*stepMillis
			if ts > end.UnixMilli() {
				break
			}
			values = append(values, []any{float64(ts) / 1000, formatSample(*value)})
		}
		if len(values) > 0 {
			results = append(results, sampleResult{Metric: metric, Values: values})
		}
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	if len(results) >= s.cfg.MaxSeries {
		return queryData{}, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) tryPostHogExactSelectorRangeRows(ctx context.Context, selector *parser.VectorSelector, start, end time.Time, stepMillis int64) (queryData, bool, error) {
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, start.UnixMilli(), end.UnixMilli())
	sql := fmt.Sprintf(
		"SELECT %s AS series_id, metric_name, service_name, resource_attributes, attributes_map_str, toUnixTimestamp64Milli(timestamp) AS ts, value FROM %s WHERE %s ORDER BY series_id, timestamp",
		postHogSeriesIDExpr(),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results := make([]sampleResult, 0, 128)
	seen := make(map[uint64]int, 128)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var serviceName string
		var resourceAttrs map[string]string
		var attrs map[string]string
		var ts int64
		var value float64
		if err := row.Scan(&id, &metricName, &serviceName, &resourceAttrs, &attrs, &ts, &value); err != nil {
			return err
		}
		if isStaleSampleValue(value) {
			return nil
		}
		idx, ok := seen[id]
		if !ok {
			metric := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
			if !matchesAll(metric, selector.LabelMatchers) {
				return nil
			}
			if len(results) >= s.cfg.MaxSeries {
				return fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", s.cfg.MaxSeries)
			}
			idx = len(results)
			seen[id] = idx
			results = append(results, sampleResult{Metric: metric})
		}
		if ts >= start.UnixMilli() && ts <= end.UnixMilli() && (ts-start.UnixMilli())%stepMillis == 0 {
			results[idx].Values = append(results[idx].Values, []any{float64(ts) / 1000, formatSample(value)})
		}
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func postHogSelectorHasNonMetricMatcher(matchers []*labels.Matcher) bool {
	for _, matcher := range matchers {
		if matcher.Name != "" && matcher.Name != labels.MetricName && !matcherIsNoop(matcher) {
			return true
		}
	}
	return false
}

func (s *Server) tryLabelIndexAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	latest := latestSamplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt)

	return s.queryLabelIndexInstantAggregate(ctx, expr, selectedSeries, latest, selector.LabelMatchers, evalTime, aggSQL)
}

func (s *Server) tryFastInstantSeriesExprAggregate(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	branches, ok := s.instantSeriesExprBranches(expr.Expr, evalTime)
	if !ok || len(branches) == 0 || len(branches) > maxAggregateUnionSelectors {
		return queryData{}, false, nil
	}
	if len(branches) == 1 && branches[0].kind == instantSeriesSelector && branches[0].transform.identity() {
		return queryData{}, false, nil
	}

	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	first := branches[0]
	for _, branch := range branches {
		if !first.equivalentTo(branch) || !matchersPushdownSafe(branch.selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		selectors = append(selectors, branch.selector)
	}

	groupMatchers := first.selector.LabelMatchers
	selectedSeries, ok := selectedSeriesForInstantBranchesSQL(s.cfg, branches, selectors, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	if len(branches) > 1 {
		groupMatchers = commonExactMetricMatchers(selectors)
	}
	sourceSQL, sourceName := s.instantSeriesExprSourceSQL(first, selectors)
	if sourceSQL == "" {
		return queryData{}, false, nil
	}
	return s.queryInstantSeriesAggregate(ctx, expr, selectedSeries, sourceName, sourceSQL, groupMatchers, evalTime, aggSQL)
}

func (s *Server) queryLabelIndexInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selectedSeries, latest string, groupMatchers []*labels.Matcher, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	return s.queryInstantSeriesAggregate(ctx, expr, selectedSeries, "latest", latest, groupMatchers, evalTime, aggSQL)
}

func (s *Server) queryInstantSeriesAggregate(ctx context.Context, expr *parser.AggregateExpr, selectedSeries, sourceName, sourceSQL string, groupMatchers []*labels.Matcher, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, groupMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		sourceName + " AS (" + sourceSQL + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	source := aggregateSourceSQL(sourceName, expr.Grouping, groupJoin)
	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM %s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		source,
	)
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryFastAggregateRangeQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	if expr.Without {
		return queryData{}, false, nil
	}
	aggSQL, ok := aggregateSQL(expr, "sample_value")
	if !ok {
		return queryData{}, false, nil
	}
	if data, ok, err := s.tryFastAggregateRangeUnionQuery(ctx, expr, start, end, step, stepMillis, aggSQL); ok || err != nil {
		return data, ok, err
	}

	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(expr.Expr, start, end, step)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}

	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}
	if vectorSelector, ok := expr.Expr.(*parser.VectorSelector); ok && exactBucketRangeSelector(s.cfg, vectorSelector, start, end, step) {
		return s.queryFastExactAggregateRange(ctx, expr, selectedSeries, selector.LabelMatchers, start, end, aggSQL)
	}
	return s.queryFastAggregateRange(ctx, expr, selectedSeries, selector.LabelMatchers, source, mint, maxt, start, stepMillis, aggSQL)
}

func (s *Server) tryFastAggregateRangeUnionQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, stepMillis int64, aggSQL string) (queryData, bool, error) {
	branches, ok := seriesExprBranches(expr.Expr)
	if !ok || len(branches) == 0 || len(branches) > maxAggregateUnionSelectors {
		return queryData{}, false, nil
	}
	if len(branches) == 1 && branches[0].transform.identity() {
		// single selector or range function without scalar arithmetic: the dedicated paths handle it
		return queryData{}, false, nil
	}

	first := branches[0]
	source, mint, maxt, ok := s.rangeUnionBranchSource(first, start, end, step)
	if !ok {
		return queryData{}, false, nil
	}
	selectors := make([]*parser.VectorSelector, 0, len(branches))
	selectors = append(selectors, first.selector)
	for _, branch := range branches[1:] {
		if !first.equivalentTo(branch) {
			return queryData{}, false, nil
		}
		branchSource, branchMint, branchMaxt, ok := s.rangeUnionBranchSource(branch, start, end, step)
		if !ok || branchMint != mint || branchMaxt != maxt || branchSource.gridExpr != source.gridExpr {
			return queryData{}, false, nil
		}
		selectors = append(selectors, branch.selector)
	}
	if source.sumOverTime == nil && !source.runningSumWrap {
		if selectedSeries, groupValues, ok := selectedSeriesExactMatcherUnionBranchesSQL(s.cfg, selectors, expr.Grouping); ok {
			where := []string{teamFilter(s.cfg)}
			where = append(where, sampleTimeFilters(s.cfg, mint, maxt)...)
			if condition := metricNamesCondition(exactMetricNamesForSelectors(selectors)); condition != "" {
				where = append(where, condition)
			}
			sampleSource := rawSamplesSourceSQL(s.cfg, strings.Join(where, " AND "))
			sql := fastAggregateRangeExactUnionSQL(s.cfg, selectedSeries, groupValues, source, sampleSource, start, stepMillis, aggSQL)
			results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
			if err != nil {
				return queryData{}, true, err
			}
			return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
		}
	}

	selectedProjection := []string{"id"}
	selectedProjection = append(selectedProjection, metricGroupProjection(expr.Grouping)...)
	windows := make([]selectorWindow, len(selectors))
	for i := range windows {
		windows[i] = selectorWindow{mint: mint, maxt: maxt}
	}
	selectedSeries, ok := selectedSeriesUnionSQL(s.cfg, selectors, windows, selectedProjection)
	if !ok {
		return queryData{}, false, nil
	}

	groupMatchers := first.selector.LabelMatchers
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, groupMatchers, mint, maxt)
	if len(branches) > 1 {
		groupMatchers = commonExactMetricMatchers(selectors)
		sampleSource = samplesForSelectedSeriesUnionSQL(s.cfg, exactMetricNamesForSelectors(selectors), mint, maxt)
	}
	return s.queryFastAggregateRangeFromSamples(ctx, expr, selectedSeries, groupMatchers, source, sampleSource, start, stepMillis, aggSQL)
}

func (s *Server) rangeUnionBranchSource(branch seriesExprBranch, start, end time.Time, step time.Duration) (aggregateRangeSource, int64, int64, bool) {
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(branch.node, start, end, step)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return aggregateRangeSource{}, 0, 0, false
	}
	source.gridExpr = transformGridValsSQL(source.gridExpr, branch.transform)
	return source, mint, maxt, true
}

func (s *Server) queryFastAggregateRange(ctx context.Context, expr *parser.AggregateExpr, selectedSeries string, groupMatchers []*labels.Matcher, source aggregateRangeSource, mint, maxt int64, start time.Time, stepMillis int64, aggSQL string) (queryData, bool, error) {
	sampleSource := samplesForSelectedSeriesSQL(s.cfg, groupMatchers, mint, maxt)
	return s.queryFastAggregateRangeFromSamples(ctx, expr, selectedSeries, groupMatchers, source, sampleSource, start, stepMillis, aggSQL)
}

func (s *Server) queryFastAggregateRangeFromSamples(ctx context.Context, expr *parser.AggregateExpr, selectedSeries string, groupMatchers []*labels.Matcher, source aggregateRangeSource, sampleSource string, start time.Time, stepMillis int64, aggSQL string) (queryData, bool, error) {
	sql := fastAggregateRangeSQL(s.cfg, expr, selectedSeries, groupMatchers, source, sampleSource, start, stepMillis, aggSQL)
	results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func fastAggregateRangeSQL(cfg Config, expr *parser.AggregateExpr, selectedSeries string, groupMatchers []*labels.Matcher, source aggregateRangeSource, sampleSource string, start time.Time, stepMillis int64, aggSQL string) string {
	perSeries := rangeSourcePerSeriesSQL(source, sampleSource)

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(cfg, groupMatchers, expr.Grouping)
	if exactGroups, ok := exactMatcherGroupSQL(groupMatchers, expr.Grouping); ok {
		groupSelect = exactGroups
		groupLabels = ""
		groupJoin = ""
	}
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		"per_series AS (" + perSeries + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, fmt.Sprintf("toInt64(%d) + (toInt64(idx) - 1) * %d AS ts", start.UnixMilli(), stepMillis))
	selectParts = append(selectParts, aggSQL+" AS value")

	groupByParts := append([]string{}, groupBy...)
	groupByParts = append(groupByParts, "idx")
	orderByParts := append([]string{}, groupBy...)
	orderByParts = append(orderByParts, "idx")

	rangeSource := aggregateSourceSQL("per_series", expr.Grouping, groupJoin)
	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM %s ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE %s GROUP BY %s ORDER BY %s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		rangeSource,
		nonStaleNullableSampleSQL("sample_value"),
		strings.Join(groupByParts, ", "),
		strings.Join(orderByParts, ", "),
	)
	return withMaxThreads(sql, cfg.AggregateThreads)
}

func fastAggregateRangeExactUnionSQL(cfg Config, selectedSeries string, groupValues [][]string, source aggregateRangeSource, sampleSource string, start time.Time, stepMillis int64, aggSQL string) string {
	if source.filterStaleSamples {
		sampleSource = "SELECT * FROM (" + sampleSource + ") WHERE " + nonStaleSampleSQL("value")
	}
	perSeries := fmt.Sprintf(
		"SELECT samples.id AS id, any(selected_series.branch) AS branch, %s AS vals FROM (%s) AS samples ALL INNER JOIN selected_series USING id GROUP BY samples.id",
		source.gridExpr,
		sampleSource,
	)

	groupSelect := make([]string, 0, len(groupValues))
	groupBy := make([]string, 0, len(groupValues)+1)
	for i, values := range groupValues {
		quoted := make([]string, 0, len(values))
		for _, value := range values {
			quoted = append(quoted, sqlString(value))
		}
		alias := quoteIdent(groupAlias(i))
		groupSelect = append(groupSelect, "arrayElement(["+strings.Join(quoted, ",")+"], toUInt64(branch) + 1) AS "+alias)
		groupBy = append(groupBy, alias)
	}
	groupBy = append(groupBy, "idx")

	selectParts := append([]string{}, groupSelect...)
	selectParts = append(selectParts, fmt.Sprintf("toInt64(%d) + (toInt64(idx) - 1) * %d AS ts", start.UnixMilli(), stepMillis))
	selectParts = append(selectParts, aggSQL+" AS value")
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), per_series AS (%s) SELECT %s FROM per_series ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE %s GROUP BY %s ORDER BY %s",
		selectedSeries,
		perSeries,
		strings.Join(selectParts, ", "),
		nonStaleNullableSampleSQL("sample_value"),
		strings.Join(groupBy, ", "),
		strings.Join(groupBy, ", "),
	)
	return withMaxThreads(sql, cfg.AggregateThreads)
}

func (s *Server) queryFastExactAggregateRange(ctx context.Context, expr *parser.AggregateExpr, selectedSeries string, groupMatchers []*labels.Matcher, start, end time.Time, aggSQL string) (queryData, bool, error) {
	where := sampleBaseFilters(s.cfg, groupMatchers, start.UnixMilli(), end.UnixMilli())
	where = append(where, sampleSelectedSeriesFilters(s.cfg)...)
	where = append(where, nonStaleSampleSQL("value"))
	samples := fmt.Sprintf(
		"SELECT id, timestamp, value AS sample_value FROM %s WHERE %s",
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQL(s.cfg, groupMatchers, expr.Grouping)
	withParts := []string{
		"selected_series AS (" + selectedSeries + ")",
		"samples AS (" + samples + ")",
	}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toUnixTimestamp64Milli(timestamp) AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	groupByParts := append([]string{}, groupBy...)
	groupByParts = append(groupByParts, "ts")
	orderByParts := append([]string{}, groupBy...)
	orderByParts = append(orderByParts, "ts")

	source := aggregateSourceSQL("samples", expr.Grouping, groupJoin)
	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM %s GROUP BY %s ORDER BY %s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		source,
		strings.Join(groupByParts, ", "),
		strings.Join(orderByParts, ", "),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryAggregateRangeRows(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: results}, true, nil
}

func (s *Server) trySampleLabelIndexInstantAggregate(ctx context.Context, expr *parser.AggregateExpr, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, aggSQL string) (queryData, bool, error) {
	if s.cfg.postHogSchemaLayout() || groupingHasMetricName(expr.Grouping) {
		return queryData{}, false, nil
	}
	metric := exactMetricName(selector.LabelMatchers)
	if metric == "" || s.cfg.SamplesTable == "" || s.cfg.LabelIndexTable == "" {
		return queryData{}, false, nil
	}
	idFilters, ok := nonMetricSampleIDFiltersFromMatchers(s.cfg, metric, selector.LabelMatchers)
	if !ok {
		return queryData{}, false, nil
	}

	where := sampleBaseFilters(s.cfg, selector.LabelMatchers, window.mint, window.maxt)
	where = append(where, idFilters...)
	latest := fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value FROM %s WHERE %s GROUP BY id",
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	latest = fmt.Sprintf("SELECT * FROM (%s) WHERE %s", latest, nonStaleSampleSQL("value"))

	groupSelect, groupBy := labelIndexGroupSQL(expr.Grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQLWithSelectedFilter(s.cfg, selector.LabelMatchers, expr.Grouping, false)
	withParts := []string{"latest AS (" + latest + ")"}
	if groupLabels != "" {
		withParts = append(withParts, "group_labels AS ("+groupLabels+")")
	}

	selectParts := make([]string, 0, len(groupSelect)+2)
	selectParts = append(selectParts, groupSelect...)
	selectParts = append(selectParts, "toInt64("+strconv.FormatInt(evalTime.UnixMilli(), 10)+") AS ts")
	selectParts = append(selectParts, aggSQL+" AS value")

	sql := fmt.Sprintf(
		"WITH %s SELECT %s FROM latest%s",
		strings.Join(withParts, ", "),
		strings.Join(selectParts, ", "),
		groupJoin,
	)
	if len(groupBy) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
		sql += " ORDER BY " + strings.Join(groupBy, ", ")
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results, err := s.queryInstantAggregateResults(ctx, sql, expr.Grouping)
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) queryAggregateRangeRows(ctx context.Context, sql string, grouping []string) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 128)
	seen := make(map[string]int, 128)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		groupValues := make([]string, len(grouping))
		var ts int64
		var value float64
		dest := make([]any, 0, len(groupValues)+2)
		for i := range groupValues {
			dest = append(dest, &groupValues[i])
		}
		dest = append(dest, &ts, &value)
		if err := row.Scan(dest...); err != nil {
			return err
		}
		metric, key := groupingMetricAndKeyValues(groupValues, grouping)
		idx, ok := seen[key]
		if !ok {
			idx = len(results)
			seen[key] = idx
			results = append(results, sampleResult{Metric: metric})
		}
		results[idx].Values = append(results[idx].Values, []any{float64(ts) / 1000, formatSample(value)})
		return nil
	})
	return results, err
}

func (s *Server) tryFastNestedCountRangeQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, stepMillis int64) (queryData, bool, error) {
	inner, selector, ok := nestedCountSelector(expr)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	source, mint, maxt, ok := selectorRangeGridSource(s.cfg, selector, start, end, step)
	if !ok {
		return queryData{}, false, nil
	}
	steps := ((end.UnixMilli() - start.UnixMilli()) / stepMillis) + 1
	if steps <= 0 {
		return queryData{}, false, nil
	}
	if sql, ok := nestedCountTimestampSamplesRangeSQL(s.cfg, selector.LabelMatchers, inner.Grouping, source.start, start, stepMillis, steps, s.cfg.LookbackDelta); ok {
		sql = withMaxThreads(sql, s.cfg.AggregateThreads)
		data, err := s.queryNestedCountRangeValues(ctx, sql)
		if err != nil {
			return queryData{}, true, err
		}
		return data, true, nil
	}
	if sql, ok := nestedCountSamplesRangeSQL(s.cfg, selector.LabelMatchers, inner.Grouping, source.start, start, stepMillis, steps, s.cfg.LookbackDelta); ok {
		sql = withMaxThreads(sql, s.cfg.AggregateThreads)
		data, err := s.queryNestedCountRangeValues(ctx, sql)
		if err != nil {
			return queryData{}, true, err
		}
		return data, true, nil
	}
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, mint, maxt, []string{"id", "min_time", "max_time"})
	if !ok {
		return queryData{}, false, nil
	}
	if sql, ok := nestedCountSelectedSamplesRangeSQL(s.cfg, selector.LabelMatchers, inner.Grouping, selectedSeries, source.start, start, stepMillis, steps, s.cfg.LookbackDelta); ok {
		sql = withMaxThreads(sql, s.cfg.AggregateThreads)
		data, err := s.queryNestedCountRangeValues(ctx, sql)
		if err != nil {
			return queryData{}, true, err
		}
		return data, true, nil
	}
	return queryData{}, false, nil
}

func (s *Server) tryFastNestedCountInstantQuery(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	inner, selector, ok := nestedCountSelector(expr)
	if !ok || !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	sql, ok := nestedCountSamplesInstantSQL(s.cfg, selector.LabelMatchers, inner.Grouping, window.mint, window.maxt, evalTime.UnixMilli())
	if !ok {
		return queryData{}, false, nil
	}
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)
	data, err := s.queryNestedCountInstantValue(ctx, sql)
	return data, true, err
}

func (s *Server) tryPostHogNestedCountRangeQuery(ctx context.Context, expr *parser.AggregateExpr, start, end time.Time, step time.Duration, _ int64) (queryData, bool, error) {
	inner, selector, ok := nestedCountSelector(expr)
	if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) || !exactBucketRangeSelector(s.cfg, selector, start, end, step) {
		return queryData{}, false, nil
	}
	groupExprs, ok := postHogSampleGroupExprs(inner.Grouping)
	if !ok || len(groupExprs) == 0 {
		return queryData{}, false, nil
	}
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, start.UnixMilli(), end.UnixMilli())
	where = append(where, nonStaleSampleSQL("value"))
	sql := fmt.Sprintf(
		"SELECT toUnixTimestamp64Milli(timestamp) AS ts, toFloat64(uniq(%s)) AS count_value FROM %s WHERE %s GROUP BY ts ORDER BY ts",
		postHogUniqGroupExpr(groupExprs),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)
	data, err := s.queryNestedCountRangeValues(ctx, sql)
	return data, true, err
}

func (s *Server) tryPostHogNestedCountInstantQuery(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	inner, selector, ok := nestedCountSelector(expr)
	if !ok || !postHogMatchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	groupExprs, ok := postHogSampleGroupExprs(inner.Grouping)
	if !ok || len(groupExprs) == 0 {
		return queryData{}, false, nil
	}
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, window.mint, window.maxt)
	perSeriesSelects := []string{
		postHogSeriesIDExpr() + " AS series_id",
		"argMax(value, timestamp) AS value",
	}
	uniqExprs := make([]string, 0, len(groupExprs))
	for i, expr := range groupExprs {
		alias := quoteIdent(groupAlias(i))
		perSeriesSelects = append(perSeriesSelects, "argMax("+expr+", timestamp) AS "+alias)
		uniqExprs = append(uniqExprs, alias)
	}
	perSeries := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY series_id",
		strings.Join(perSeriesSelects, ", "),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	sql := fmt.Sprintf(
		"SELECT toInt64(%d) AS ts, toFloat64(uniq(%s)) AS count_value FROM (%s) WHERE %s",
		evalTime.UnixMilli(),
		postHogUniqGroupExpr(uniqExprs),
		perSeries,
		nonStaleSampleSQL("value"),
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)
	data, err := s.queryNestedCountInstantValue(ctx, sql)
	return data, true, err
}

func nestedCountSelector(expr *parser.AggregateExpr) (*parser.AggregateExpr, *parser.VectorSelector, bool) {
	if expr.Op != parser.COUNT || expr.Without || len(expr.Grouping) != 0 {
		return nil, nil, false
	}
	inner, ok := expr.Expr.(*parser.AggregateExpr)
	if !ok || inner.Op != parser.COUNT || inner.Without || len(inner.Grouping) == 0 {
		return nil, nil, false
	}
	if groupingHasMetricName(inner.Grouping) {
		return nil, nil, false
	}
	selector, ok := inner.Expr.(*parser.VectorSelector)
	if !ok {
		return nil, nil, false
	}
	return inner, selector, true
}

func nestedCountTimestampSamplesRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	if cfg.SamplesTable == "" || cfg.LabelIndexTable == "" || len(grouping) != 1 || grouping[0] == labels.MetricName || steps <= 0 {
		return "", false
	}
	intervalMillis := cfg.RemoteWriteInterval.Milliseconds()
	evalStartMillis := evalStart.UnixMilli()
	if intervalMillis <= 0 || intervalMillis > lookback.Milliseconds() {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	idFilters, ok := nonMetricSampleIDFiltersFromMatchers(cfg, metric, matchers)
	if !ok {
		return "", false
	}

	evalEndMillis := evalStartMillis + (steps-1)*stepMillis
	groupColumn, groupLabels := nestedCountGroupLabelsSQL(cfg, metric, grouping[0])
	if stepMillis >= intervalMillis && stepMillis%intervalMillis == 0 && bucketTimestampMS(evalStartMillis, cfg.RemoteWriteInterval) == evalStartMillis {
		return nestedCountAlignedTimestampSamplesRangeSQL(cfg, metric, groupColumn, groupLabels, idFilters, evalStartMillis, outputStart.UnixMilli(), stepMillis, steps)
	}

	bucketStartMillis := bucketTimestampMS(evalStartMillis, cfg.RemoteWriteInterval)
	bucketEndMillis := bucketTimestampMS(evalEndMillis, cfg.RemoteWriteInterval)
	sampleWhere := sampleBaseFilters(cfg, matchers, bucketStartMillis, bucketEndMillis)
	sampleWhere = append(sampleWhere, sampleStepBucketFilters(cfg, evalStartMillis, stepMillis, steps)...)
	sampleWhere = append(sampleWhere, idFilters...)
	sampleWhere = append(sampleWhere, nonStaleSampleSQL("value"))

	return fmt.Sprintf(
		"WITH group_labels AS (%s), step_map AS (SELECT toInt64(%d) + toInt64(number) * %d AS ts, intDiv(toInt64(%d) + toInt64(number) * %d, %d) * %d AS bucket_ms FROM numbers(toUInt64(%d))) SELECT step_map.ts AS ts, %s AS value FROM step_map INNER JOIN (SELECT timestamp, id FROM %s WHERE %s) AS active_samples ON active_samples.timestamp = %s ANY LEFT JOIN group_labels USING id GROUP BY ts ORDER BY ts",
		groupLabels,
		outputStart.UnixMilli(),
		stepMillis,
		evalStartMillis,
		stepMillis,
		intervalMillis,
		intervalMillis,
		steps,
		nestedCountDistinctGroupSQL(groupColumn),
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
		chTimeMillisExpr("step_map.bucket_ms"),
	), true
}

func nestedCountAlignedTimestampSamplesRangeSQL(cfg Config, metric, groupColumn, groupLabels string, idFilters []string, evalStartMillis, outputStartMillis, stepMillis, steps int64) (string, bool) {
	evalEndMillis := evalStartMillis + (steps-1)*stepMillis
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric)}
	sampleWhere := sampleBaseFilters(cfg, matchers, evalStartMillis, evalEndMillis)
	if stepMillis != cfg.RemoteWriteInterval.Milliseconds() {
		sampleWhere = append(sampleWhere, fmt.Sprintf("modulo(toUnixTimestamp64Milli(timestamp) - %d, %d) = 0", evalStartMillis, stepMillis))
	}
	sampleWhere = append(sampleWhere, sampleStepBucketFilters(cfg, evalStartMillis, stepMillis, steps)...)
	sampleWhere = append(sampleWhere, idFilters...)
	sampleWhere = append(sampleWhere, nonStaleSampleSQL("value"))

	return fmt.Sprintf(
		"WITH group_labels AS (%s), active_samples AS (SELECT timestamp, id FROM %s WHERE %s) SELECT toInt64(%d) + intDiv(toUnixTimestamp64Milli(timestamp) - %d, %d) * %d AS ts, %s AS value FROM active_samples ANY LEFT JOIN group_labels USING id GROUP BY ts ORDER BY ts",
		groupLabels,
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
		outputStartMillis,
		evalStartMillis,
		stepMillis,
		stepMillis,
		nestedCountDistinctGroupSQL(groupColumn),
	), true
}

func nestedCountSamplesRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	if cfg.SamplesTable == "" || cfg.LabelIndexTable == "" || len(grouping) == 0 || groupingHasMetricName(grouping) || steps <= 0 {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	idFilters, ok := nonMetricSampleIDFiltersFromMatchers(cfg, metric, matchers)
	if !ok {
		return "", false
	}

	sampleStart := evalStart.Add(-lookback)
	sampleEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)
	sampleWhere := sampleBaseFilters(cfg, matchers, sampleStart.UnixMilli(), sampleEnd.UnixMilli())
	sampleWhere = append(sampleWhere, idFilters...)

	groupSelect, groupBy := labelIndexGroupSQL(grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQLWithSelectedFilter(cfg, matchers, grouping, false)
	if groupLabels == "" || len(groupBy) == 0 {
		return "", false
	}
	gridEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)
	gridExpr := lastGridExpr(evalStart, gridEnd, time.Duration(stepMillis)*time.Millisecond, lookback)
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (SELECT id, timestamp, value FROM %s WHERE %s) GROUP BY id",
		gridExpr,
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
	)

	return fmt.Sprintf(
		"WITH group_labels AS (%s), per_series AS (%s), active_groups AS (SELECT idx, %s FROM per_series%s ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE %s GROUP BY idx, %s) SELECT toInt64(%d) + (toInt64(idx) - 1) * %d AS ts, toFloat64(count()) AS value FROM active_groups GROUP BY idx ORDER BY idx",
		groupLabels,
		perSeries,
		strings.Join(groupSelect, ", "),
		groupJoin,
		nonStaleNullableSampleSQL("sample_value"),
		strings.Join(groupBy, ", "),
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func nestedCountSelectedSamplesRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, selectedSeries string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	if cfg.SamplesTable == "" || cfg.LabelIndexTable == "" || steps <= 0 {
		return "", false
	}
	groupSelect, groupBy := labelIndexGroupSQL(grouping)
	groupLabels, groupJoin := labelIndexGroupLabelsSQLWithSelectedFilter(cfg, matchers, grouping, true)
	if groupLabels == "" || len(groupBy) == 0 {
		return "", false
	}

	sampleStart := evalStart.Add(-lookback)
	sampleEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)
	sampleWhere := sampleBaseFilters(cfg, matchers, sampleStart.UnixMilli(), sampleEnd.UnixMilli())
	sampleWhere = append(sampleWhere, sampleSelectedSeriesFilters(cfg)...)
	gridEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)
	gridExpr := lastGridExpr(evalStart, gridEnd, time.Duration(stepMillis)*time.Millisecond, lookback)
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (SELECT id, timestamp, value FROM %s WHERE %s) GROUP BY id",
		gridExpr,
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
	)

	return fmt.Sprintf(
		"WITH selected_series AS (%s), group_labels AS (%s), per_series AS (%s), active_groups AS (SELECT idx, %s FROM per_series%s ARRAY JOIN arrayEnumerate(vals) AS idx, vals AS sample_value WHERE %s GROUP BY idx, %s) SELECT toInt64(%d) + (toInt64(idx) - 1) * %d AS ts, toFloat64(count()) AS value FROM active_groups GROUP BY idx ORDER BY idx",
		selectedSeries,
		groupLabels,
		perSeries,
		strings.Join(groupSelect, ", "),
		groupJoin,
		nonStaleNullableSampleSQL("sample_value"),
		strings.Join(groupBy, ", "),
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func nestedCountSamplesInstantSQL(cfg Config, matchers []*labels.Matcher, grouping []string, mint, maxt, evalMillis int64) (string, bool) {
	if cfg.SamplesTable == "" || cfg.LabelIndexTable == "" || len(grouping) != 1 || grouping[0] == labels.MetricName {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	idFilters, ok := nonMetricSampleIDFiltersFromMatchers(cfg, metric, matchers)
	if !ok {
		return "", false
	}

	sampleWhere := sampleBaseFilters(cfg, matchers, mint, maxt)
	sampleWhere = append(sampleWhere, idFilters...)

	groupColumn, groupLabels := nestedCountGroupLabelsSQL(cfg, metric, grouping[0])
	return fmt.Sprintf(
		"WITH group_labels AS (%s), active_ids AS (SELECT id FROM (SELECT id, argMax(value, timestamp) AS value FROM %s WHERE %s GROUP BY id) WHERE %s) SELECT toInt64(%d) AS ts, %s AS value FROM active_ids ANY LEFT JOIN group_labels USING id",
		groupLabels,
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(sampleWhere, " AND "),
		nonStaleSampleSQL("value"),
		evalMillis,
		nestedCountDistinctGroupSQL(groupColumn),
	), true
}

func nestedCountDistinctGroupSQL(groupColumn string) string {
	return "toFloat64(uniq(ifNull(" + groupColumn + ", '')))"
}

func nestedCountGroupLabelsSQL(cfg Config, metric, groupName string) (string, string) {
	groupColumn := quoteIdent(groupAlias(0))
	return groupColumn, fmt.Sprintf(
		"SELECT id, label_value AS %s FROM %s WHERE %s AND metric_name = %s AND label_name = %s",
		groupColumn,
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		teamFilter(cfg),
		sqlString(metric),
		sqlString(groupName),
	)
}

func nonMetricSampleIDFiltersFromMatchers(cfg Config, metric string, matchers []*labels.Matcher) ([]string, bool) {
	var filters []string
	for _, m := range matchers {
		if m.Name == labels.MetricName || matcherIsNoop(m) {
			continue
		}
		membership, condition, ok := labelIndexMembershipCondition(m)
		if !ok {
			return nil, false
		}
		source := fmt.Sprintf(
			"SELECT id FROM %s WHERE %s AND metric_name = %s AND label_name = %s AND %s",
			tableName(cfg.CHDatabase, cfg.LabelIndexTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(m.Name),
			condition,
		)
		filters = append(filters, sampleIDMembershipFilters(cfg, membership, source)...)
	}
	return filters, true
}

func (s *Server) queryNestedCountInstantValue(ctx context.Context, sql string) (queryData, error) {
	var result []sampleResult
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var ts int64
		var value float64
		if err := row.Scan(&ts, &value); err != nil {
			return err
		}
		result = append(result, sampleResult{
			Metric: map[string]string{},
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: result}, nil
}

func (s *Server) queryNestedCountRangeValues(ctx context.Context, sql string) (queryData, error) {
	values := make([][]any, 0, 128)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var ts int64
		var value float64
		if err := row.Scan(&ts, &value); err != nil {
			return err
		}
		values = append(values, []any{float64(ts) / 1000, formatSample(value)})
		return nil
	})
	if err != nil {
		return queryData{}, err
	}
	result := []sampleResult{{Metric: map[string]string{}, Values: values}}
	return queryData{ResultType: string(parser.ValueTypeMatrix), Result: result}, nil
}

func nestedCountRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, selectedSeries string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	groupSelect, groupBy := labelIndexGroupSQL(grouping)
	filterGroupLabelsToSelected := exactMetricName(matchers) == ""
	groupLabels, groupJoin := labelIndexGroupLabelsSQLWithSelectedFilter(cfg, matchers, grouping, filterGroupLabelsToSelected)
	if groupLabels == "" || len(groupBy) == 0 {
		return "", false
	}

	evalMillisExpr := fmt.Sprintf("(%d + toInt64(step_idx) * %d)", evalStart.UnixMilli(), stepMillis)
	lookbackStartExpr := fmt.Sprintf("(%s - %d)", evalMillisExpr, lookback.Milliseconds())

	return fmt.Sprintf(
		"WITH selected_series AS (%s), group_labels AS (%s), group_intervals AS (SELECT %s, groupArray((toUnixTimestamp64Milli(min_time), toUnixTimestamp64Milli(max_time))) AS active_ranges FROM selected_series%s GROUP BY %s), active_groups AS (SELECT step_idx FROM group_intervals ARRAY JOIN range(toUInt64(%d)) AS step_idx WHERE arrayExists(x -> (tupleElement(x, 2) >= %s AND tupleElement(x, 1) <= %s), active_ranges)) SELECT toInt64(%d) + toInt64(step_idx) * %d AS ts, toFloat64(count()) AS value FROM active_groups GROUP BY step_idx ORDER BY step_idx",
		selectedSeries,
		groupLabels,
		strings.Join(groupSelect, ", "),
		groupJoin,
		strings.Join(groupBy, ", "),
		steps,
		lookbackStartExpr,
		evalMillisExpr,
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func nestedCountBitmapRangeSQL(cfg Config, matchers []*labels.Matcher, grouping []string, evalStart, outputStart time.Time, stepMillis, steps int64, lookback time.Duration) (string, bool) {
	if cfg.LabelPostingsTable == "" || cfg.ActivityTable == "" || len(grouping) != 1 || grouping[0] == labels.MetricName {
		return "", false
	}
	metric := exactMetricName(matchers)
	if metric == "" {
		return "", false
	}
	selectedCTEs, selectedFrom, selectedExpr, ok := bitmapSelectedIDsSQL(cfg, metric, matchers)
	if !ok {
		return "", false
	}

	groupColumn := quoteIdent(groupAlias(0))
	groupName := grouping[0]
	evalMillisExpr := fmt.Sprintf("(%d + toInt64(step_idx) * %d)", evalStart.UnixMilli(), stepMillis)
	lookbackStartExpr := fmt.Sprintf("(%s - %d)", evalMillisExpr, lookback.Milliseconds())
	activityStart := evalStart.Add(-lookback)
	activityEnd := evalStart.Add(time.Duration(steps-1) * time.Duration(stepMillis) * time.Millisecond)

	withParts := append([]string{}, selectedCTEs...)
	withParts = append(withParts,
		fmt.Sprintf("selected_ids AS (SELECT %s AS bm FROM %s)", selectedExpr, selectedFrom),
		fmt.Sprintf(
			"active_by_step AS (SELECT step_idx, groupBitmapState(id) AS bm FROM (SELECT bucket, arrayJoin(ids) AS id FROM %s WHERE %s AND metric_name = %s AND bucket >= %s AND bucket <= %s) ARRAY JOIN range(toUInt64(%d)) AS step_idx WHERE toUnixTimestamp64Milli(bucket) >= %s AND toUnixTimestamp64Milli(bucket) <= %s GROUP BY step_idx)",
			tableName(cfg.CHDatabase, cfg.ActivityTable),
			teamFilter(cfg),
			sqlString(metric),
			chTimeMillis(activityStart.UnixMilli()),
			chTimeMillis(activityEnd.UnixMilli()),
			steps,
			lookbackStartExpr,
			evalMillisExpr,
		),
		"active_selected AS (SELECT step_idx, bitmapAnd(active_by_step.bm, selected_ids.bm) AS bm FROM active_by_step CROSS JOIN selected_ids WHERE bitmapAndCardinality(active_by_step.bm, selected_ids.bm) > 0)",
		fmt.Sprintf(
			"group_label_values AS (SELECT label_value AS %s, groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s GROUP BY label_value)",
			groupColumn,
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(groupName),
		),
		fmt.Sprintf(
			"group_label_present AS (SELECT groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s)",
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(groupName),
		),
		fmt.Sprintf(
			"label_groups AS (SELECT %s, bm FROM group_label_values UNION ALL SELECT '' AS %s, bitmapAndnot(metric_ids.bm, group_label_present.bm) AS bm FROM metric_ids CROSS JOIN group_label_present)",
			groupColumn,
			groupColumn,
		),
	)

	return fmt.Sprintf(
		"WITH %s SELECT toInt64(%d) + toInt64(active_selected.step_idx) * %d AS ts, toFloat64(count()) AS value FROM active_selected CROSS JOIN label_groups WHERE bitmapAndCardinality(active_selected.bm, label_groups.bm) > 0 GROUP BY active_selected.step_idx ORDER BY active_selected.step_idx",
		strings.Join(withParts, ", "),
		outputStart.UnixMilli(),
		stepMillis,
	), true
}

func bitmapSelectedIDsSQL(cfg Config, metric string, matchers []*labels.Matcher) ([]string, string, string, bool) {
	ctes := []string{
		fmt.Sprintf(
			"metric_ids AS (SELECT groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s AND label_value = %s)",
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(labels.MetricName),
			sqlString(metric),
		),
	}
	fromParts := []string{"metric_ids"}
	expr := "metric_ids.bm"
	matcherIndex := 0
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName || matcherIsNoop(matcher) {
			continue
		}
		positive, condition, ok := bitmapMatcherCondition(matcher)
		if !ok {
			return nil, "", "", false
		}
		alias := fmt.Sprintf("matcher_%d", matcherIndex)
		matcherIndex++
		ctes = append(ctes, fmt.Sprintf(
			"%s AS (SELECT groupBitmapOrState(ids) AS bm FROM %s WHERE %s AND metric_name = %s AND label_name = %s AND %s)",
			alias,
			tableName(cfg.CHDatabase, cfg.LabelPostingsTable),
			teamFilter(cfg),
			sqlString(metric),
			sqlString(matcher.Name),
			condition,
		))
		fromParts = append(fromParts, alias)
		if positive {
			expr = "bitmapAnd(" + expr + ", " + alias + ".bm)"
		} else {
			expr = "bitmapAndnot(" + expr + ", " + alias + ".bm)"
		}
	}
	return ctes, strings.Join(fromParts, " CROSS JOIN "), expr, true
}

func bitmapMatcherCondition(matcher *labels.Matcher) (bool, string, bool) {
	switch matcher.Type {
	case labels.MatchEqual, labels.MatchRegexp:
		_, condition, ok := positiveLabelIndexMembershipCondition(matcher)
		return true, condition, ok
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		condition, ok := negativeLabelIndexCondition(matcher)
		return false, condition, ok
	default:
		return false, "", false
	}
}

type selectorGridSource struct {
	start time.Time
	end   time.Time
}

type aggregateRangeSource struct {
	gridExpr           string
	gridStart          time.Time
	gridEnd            time.Time
	gridStep           time.Duration
	avgOverTime        time.Duration
	filterStaleSamples bool
	runningSumWrap     bool
	sumOverTime        *rangeSourceSumOverTime
}

type rangeSourceSumOverTime struct {
	outputStart time.Time
	outputEnd   time.Time
	outputStep  time.Duration
	window      time.Duration
}

func (s *Server) aggregateRangeSourceSQL(expr parser.Expr, start, end time.Time, step time.Duration) (aggregateRangeSource, *parser.VectorSelector, int64, int64, bool) {
	if selector, ok := expr.(*parser.VectorSelector); ok {
		source, mint, maxt, ok := selectorRangeGridSource(s.cfg, selector, start, end, step)
		if !ok {
			return aggregateRangeSource{}, nil, 0, 0, false
		}
		return aggregateRangeSource{gridExpr: lastGridExpr(source.start, source.end, step, s.cfg.LookbackDelta), gridStart: source.start, gridEnd: source.end, gridStep: step}, selector, mint, maxt, true
	}

	call, ok := expr.(*parser.Call)
	if !ok || len(call.Args) != 1 {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	if call.Func.Name == "running_sum" {
		source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(call.Args[0], start, end, step)
		if !ok {
			return aggregateRangeSource{}, nil, 0, 0, false
		}
		source.runningSumWrap = true
		return source, selector, mint, maxt, true
	}
	if call.Func.Name == "sum_over_time" {
		return s.sumOverTimeSubquerySourceSQL(call.Args[0], start, end, step)
	}
	matrix, ok := call.Args[0].(*parser.MatrixSelector)
	if !ok || matrix.Range <= 0 {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok || selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	if call.Func.Name == "avg_over_time" {
		return aggregateRangeSource{
			gridStart:          shiftedStart,
			gridEnd:            shiftedEnd,
			gridStep:           step,
			avgOverTime:        matrix.Range,
			filterStaleSamples: true,
		}, selector, shiftedStart.Add(-matrix.Range).UnixMilli() + 1, shiftedEnd.UnixMilli(), true
	}
	fn, ok := rangeGridFunction(call.Func.Name)
	if !ok {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	gridRange := matrix.Range + previousSampleLookback(s.cfg, fn)
	mint := shiftedStart.Add(-gridRange).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	gridRangeSeconds := formatDurationSeconds(gridRange)
	gridExpr := fmt.Sprintf(
		"%s(%s, %s, %s, %s)(timestamp, value)",
		fn.name,
		chTimeMillis(shiftedStart.UnixMilli()),
		chTimeMillis(shiftedEnd.UnixMilli()),
		formatDurationSeconds(step),
		gridRangeSeconds,
	)
	if fn.increase {
		gridExpr = fmt.Sprintf("arrayMap(x -> if(isNull(x), NULL, x * %s), %s)", formatDurationSeconds(matrix.Range), gridExpr)
	}
	return aggregateRangeSource{gridExpr: gridExpr, gridStart: shiftedStart, gridEnd: shiftedEnd, gridStep: step, filterStaleSamples: true}, selector, mint, maxt, true
}

func previousSampleLookback(cfg Config, fn rangeGridFunctionSpec) time.Duration {
	if !fn.usesPreviousSample || cfg.RemoteWriteInterval <= 0 {
		return 0
	}
	return cfg.RemoteWriteInterval
}

func (s *Server) sumOverTimeSubquerySourceSQL(expr parser.Expr, start, end time.Time, step time.Duration) (aggregateRangeSource, *parser.VectorSelector, int64, int64, bool) {
	subquery, ok := expr.(*parser.SubqueryExpr)
	if !ok || subquery.Range <= 0 || subquery.OriginalOffset != 0 || subquery.Offset != 0 || subquery.Timestamp != nil || subquery.StartOrEnd != 0 {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	subqueryStep := subquery.Step
	if subqueryStep <= 0 {
		subqueryStep = step
	}
	if subqueryStep <= 0 {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	innerStart := start.Add(-subquery.Range)
	source, selector, mint, maxt, ok := s.aggregateRangeSourceSQL(subquery.Expr, innerStart, end, subqueryStep)
	if !ok {
		return aggregateRangeSource{}, nil, 0, 0, false
	}
	source.sumOverTime = &rangeSourceSumOverTime{
		outputStart: start,
		outputEnd:   end,
		outputStep:  step,
		window:      subquery.Range,
	}
	return source, selector, mint, maxt, true
}

func rangeSourcePerSeriesSQL(source aggregateRangeSource, sampleSource string) string {
	if source.filterStaleSamples {
		sampleSource = "SELECT * FROM (" + sampleSource + ") WHERE " + nonStaleSampleSQL("value")
	}
	if source.avgOverTime > 0 {
		return fmt.Sprintf(
			"SELECT id, arrayMap(eval_ms -> arrayReduce('avgOrNull', arrayFilter((v, ts_ms) -> ts_ms > eval_ms - %d AND ts_ms <= eval_ms, vals, ts)), range(toInt64(%d), toInt64(%d), toInt64(%d))) AS vals FROM (%s)",
			source.avgOverTime.Milliseconds(),
			source.gridStart.UnixMilli(),
			source.gridEnd.UnixMilli()+source.gridStep.Milliseconds(),
			source.gridStep.Milliseconds(),
			sampleArraySourceFromSamplesSQL(sampleSource),
		)
	}
	perSeries := fmt.Sprintf(
		"SELECT id, %s AS vals FROM (%s) GROUP BY id",
		source.gridExpr,
		sampleSource,
	)
	if source.sumOverTime != nil {
		perSeries = fmt.Sprintf(
			"SELECT id, %s AS vals FROM (%s)",
			sumOverTimeArraySQL("vals", source.gridStart, source.gridStep, *source.sumOverTime),
			perSeries,
		)
	}
	if !source.runningSumWrap {
		return perSeries
	}
	return fmt.Sprintf(
		"SELECT id, %s AS vals FROM (%s)",
		runningSumArraySQL("vals"),
		perSeries,
	)
}

func runningSumArraySQL(valuesExpr string) string {
	return fmt.Sprintf(
		"arrayMap((running_sum, seen) -> if(seen = 0, NULL, running_sum), arrayCumSum(arrayMap(x -> %s, %s)), arrayCumSum(arrayMap(x -> %s, %s)))",
		nullableValueOrZeroSQL("x"),
		valuesExpr,
		nullableIsFiniteSQL("x"),
		valuesExpr,
	)
}

func sumOverTimeArraySQL(valuesExpr string, inputStart time.Time, inputStep time.Duration, rollup rangeSourceSumOverTime) string {
	inputStepMillis := inputStep.Milliseconds()
	outputStepMillis := rollup.outputStep.Milliseconds()
	if inputStepMillis <= 0 || outputStepMillis <= 0 {
		return "[]"
	}
	outputSteps := ((rollup.outputEnd.UnixMilli() - rollup.outputStart.UnixMilli()) / outputStepMillis) + 1
	if outputSteps <= 0 {
		return "[]"
	}
	sliceExpr := func(i string) string {
		startIdx := sumOverTimeStartIndexSQL(i, inputStart.UnixMilli(), inputStepMillis, rollup.outputStart.UnixMilli(), outputStepMillis, rollup.window.Milliseconds())
		count := sumOverTimeCountSQL(i, valuesExpr, inputStart.UnixMilli(), inputStepMillis, rollup.outputStart.UnixMilli(), outputStepMillis, rollup.window.Milliseconds())
		return fmt.Sprintf("arraySlice(%s, %s, %s)", valuesExpr, startIdx, count)
	}
	return fmt.Sprintf(
		"arrayMap(i -> if(arraySum(arrayMap(x -> %s, %s)) = 0, NULL, arraySum(arrayMap(x -> %s, %s))), range(toUInt64(%d)))",
		nullableIsFiniteSQL("x"),
		sliceExpr("i"),
		nullableValueOrZeroSQL("x"),
		sliceExpr("i"),
		outputSteps,
	)
}

func sumOverTimeStartIndexSQL(i string, inputStartMillis, inputStepMillis, outputStartMillis, outputStepMillis, windowMillis int64) string {
	evalMillis := fmt.Sprintf("(%d + toInt64(%s) * %d)", outputStartMillis, i, outputStepMillis)
	windowStartMillis := fmt.Sprintf("(%s - %d)", evalMillis, windowMillis)
	return fmt.Sprintf("greatest(toInt64(1), intDiv(greatest(%s - %d, 0) + %d - 1, %d) + 1)", windowStartMillis, inputStartMillis, inputStepMillis, inputStepMillis)
}

func sumOverTimeCountSQL(i, valuesExpr string, inputStartMillis, inputStepMillis, outputStartMillis, outputStepMillis, windowMillis int64) string {
	evalMillis := fmt.Sprintf("(%d + toInt64(%s) * %d)", outputStartMillis, i, outputStepMillis)
	startIdx := sumOverTimeStartIndexSQL(i, inputStartMillis, inputStepMillis, outputStartMillis, outputStepMillis, windowMillis)
	lastIdx := fmt.Sprintf("least(toInt64(length(%s)), intDiv(greatest(%s - %d, 0), %d) + 1)", valuesExpr, evalMillis, inputStartMillis, inputStepMillis)
	return fmt.Sprintf("greatest(toInt64(0), %s - %s + 1)", lastIdx, startIdx)
}

func nullableValueOrZeroSQL(value string) string {
	return fmt.Sprintf("if(isNull(%s), 0.0, if(isNaN(assumeNotNull(%s)), 0.0, assumeNotNull(%s)))", value, value, value)
}

func nullableIsFiniteSQL(value string) string {
	return fmt.Sprintf("if(isNull(%s), 0, if(isNaN(assumeNotNull(%s)), 0, 1))", value, value)
}

func selectorRangeGridSource(cfg Config, selector *parser.VectorSelector, start, end time.Time, step time.Duration) (selectorGridSource, int64, int64, bool) {
	if selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 || selector.Timestamp != nil {
		return selectorGridSource{}, 0, 0, false
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	shiftedStart := start.Add(-offset)
	shiftedEnd := end.Add(-offset)
	mint := shiftedStart.Add(-cfg.LookbackDelta).UnixMilli()
	maxt := shiftedEnd.UnixMilli()
	return selectorGridSource{start: shiftedStart, end: shiftedEnd}, mint, maxt, true
}

func exactBucketRangeSelector(cfg Config, selector *parser.VectorSelector, start, end time.Time, step time.Duration) bool {
	if cfg.RemoteWriteInterval <= 0 || step != cfg.RemoteWriteInterval {
		return false
	}
	if selector.OriginalOffset != 0 || selector.Offset != 0 {
		return false
	}
	intervalMillis := cfg.RemoteWriteInterval.Milliseconds()
	if intervalMillis <= 0 {
		return false
	}
	startMillis := start.UnixMilli()
	endMillis := end.UnixMilli()
	return bucketTimestampMS(startMillis, cfg.RemoteWriteInterval) == startMillis &&
		bucketTimestampMS(endMillis, cfg.RemoteWriteInterval) == endMillis
}

func postHogExactInstantSampleMillis(cfg Config, selector *parser.VectorSelector, evalTime time.Time) (int64, bool) {
	if cfg.RemoteWriteInterval <= 0 || selector.Anchored || selector.Smoothed || selector.StartOrEnd != 0 {
		return 0, false
	}
	ref := evalTime.UnixMilli()
	if selector.Timestamp != nil {
		ref = *selector.Timestamp
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	sampleMillis := ref - offset.Milliseconds()
	if !postHogExactSampleTimestamp(cfg, sampleMillis) {
		return 0, false
	}
	return sampleMillis, true
}

func lastGridExpr(start, end time.Time, step, lookback time.Duration) string {
	grid := fmt.Sprintf(
		"timeSeriesLastToGrid(%s, %s, %s, %s)(timestamp, value)",
		chTimeMillis(start.UnixMilli()),
		chTimeMillis(end.UnixMilli()),
		formatDurationSeconds(step),
		formatDurationSeconds(lookback),
	)
	return staleAwareNullableGridSQL(grid)
}

func (s *Server) queryInstantAggregateResults(ctx context.Context, sql string, grouping []string) ([]sampleResult, error) {
	results := make([]sampleResult, 0, 128)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		groupValues := make([]string, len(grouping))
		var ts int64
		var value float64
		dest := make([]any, 0, len(groupValues)+2)
		for i := range groupValues {
			dest = append(dest, &groupValues[i])
		}
		dest = append(dest, &ts, &value)
		if err := row.Scan(dest...); err != nil {
			return err
		}
		results = append(results, sampleResult{
			Metric: groupingMetricValues(groupValues, grouping),
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	return results, err
}

func groupingMetricValues(values []string, grouping []string) map[string]string {
	metric, _ := groupingMetricAndKeyValues(values, grouping)
	return metric
}

func groupingMetricAndKeyValues(values []string, grouping []string) (map[string]string, string) {
	metric := make(map[string]string, len(grouping))
	keyParts := make([]string, 0, len(grouping)*2)
	for i, name := range grouping {
		value := values[i]
		if value != "" {
			metric[name] = value
		}
		keyParts = append(keyParts, name, value)
	}
	return metric, strings.Join(keyParts, "\xff")
}

func (s *Server) tryFastTopK(ctx context.Context, expr *parser.AggregateExpr, evalTime time.Time) (queryData, bool, error) {
	if expr.Op != parser.TOPK && expr.Op != parser.BOTTOMK {
		return queryData{}, false, nil
	}
	if len(expr.Grouping) != 0 || expr.Without {
		return queryData{}, false, nil
	}
	limit, ok := aggregateLimit(expr.Param)
	if !ok || limit <= 0 {
		return queryData{}, false, nil
	}
	selector, ok := expr.Expr.(*parser.VectorSelector)
	if !ok {
		return queryData{}, false, nil
	}
	window, ok := selectorWindowFor(selector, evalTime, s.cfg.LookbackDelta)
	if !ok {
		return queryData{}, false, nil
	}
	if s.cfg.postHogSchemaLayout() {
		if !postHogMatchersPushdownSafe(selector.LabelMatchers) {
			return queryData{}, false, nil
		}
		return s.tryPostHogTopK(ctx, selector, window, evalTime, limit, expr.Op)
	}
	if !matchersPushdownSafe(selector.LabelMatchers) {
		return queryData{}, false, nil
	}
	if data, ok, err := s.tryLabelIndexTopK(ctx, selector, window, evalTime, limit, expr.Op); ok || err != nil {
		return data, ok, err
	}

	return queryData{}, false, nil
}

func (s *Server) tryPostHogTopK(ctx context.Context, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, limit int, op parser.ItemType) (queryData, bool, error) {
	direction := "DESC"
	if op == parser.BOTTOMK {
		direction = "ASC"
	}
	if sampleMillis, ok := postHogExactInstantSampleMillis(s.cfg, selector, evalTime); ok {
		return s.tryPostHogExactTopK(ctx, selector, sampleMillis, evalTime, limit, direction)
	}
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, window.mint, window.maxt)
	latest := fmt.Sprintf(
		"SELECT %s AS series_id, argMax(metric_name, timestamp) AS out_metric_name, argMax(service_name, timestamp) AS out_service_name, argMax(resource_attributes, timestamp) AS out_resource_attributes, argMax(attributes_map_str, timestamp) AS out_attributes_map_str, argMax(value, timestamp) AS value FROM %s WHERE %s GROUP BY series_id",
		postHogSeriesIDExpr(),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	sql := fmt.Sprintf(
		"SELECT out_metric_name, out_service_name, out_resource_attributes, out_attributes_map_str, toInt64(%d) AS ts, value FROM (%s) WHERE %s ORDER BY value %s LIMIT %d",
		evalTime.UnixMilli(),
		latest,
		nonStaleSampleSQL("value"),
		direction,
		limit,
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results := make([]sampleResult, 0, limit)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var metricName string
		var serviceName string
		var resourceAttrs map[string]string
		var attrs map[string]string
		var ts int64
		var value float64
		if err := row.Scan(&metricName, &serviceName, &resourceAttrs, &attrs, &ts, &value); err != nil {
			return err
		}
		metric := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
		if !matchesAll(metric, selector.LabelMatchers) {
			return nil
		}
		results = append(results, sampleResult{
			Metric: metric,
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryPostHogExactTopK(ctx context.Context, selector *parser.VectorSelector, sampleMillis int64, evalTime time.Time, limit int, direction string) (queryData, bool, error) {
	where := postHogSampleFilters(s.cfg, selector.LabelMatchers, sampleMillis, sampleMillis)
	where = append(where, nonStaleSampleSQL("value"))
	sql := fmt.Sprintf(
		"SELECT metric_name, service_name, resource_attributes, attributes_map_str, toInt64(%d) AS ts, value FROM %s WHERE %s ORDER BY value %s LIMIT %d",
		evalTime.UnixMilli(),
		tableName(s.cfg.CHDatabase, s.cfg.SamplesTable),
		strings.Join(where, " AND "),
		direction,
		limit,
	)
	sql = withMaxThreads(sql, s.cfg.AggregateThreads)

	results := make([]sampleResult, 0, limit)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var metricName string
		var serviceName string
		var resourceAttrs map[string]string
		var attrs map[string]string
		var ts int64
		var value float64
		if err := row.Scan(&metricName, &serviceName, &resourceAttrs, &attrs, &ts, &value); err != nil {
			return err
		}
		metric := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
		if !matchesAll(metric, selector.LabelMatchers) {
			return nil
		}
		results = append(results, sampleResult{
			Metric: metric,
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

func (s *Server) tryLabelIndexTopK(ctx context.Context, selector *parser.VectorSelector, window selectorWindow, evalTime time.Time, limit int, op parser.ItemType) (queryData, bool, error) {
	selectedSeries, ok := selectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return queryData{}, false, nil
	}

	direction := "DESC"
	if op == parser.BOTTOMK {
		direction = "ASC"
	}
	latestSource := latestSamplesForSelectedSeriesSQL(s.cfg, selector.LabelMatchers, window.mint, window.maxt)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), top_series AS (SELECT id, value FROM (%s) ORDER BY value %s LIMIT %d) SELECT selected_series.metric_name AS metric_name, selected_series.labels_json AS labels_json, toInt64(%d) AS ts, top_series.value AS value FROM top_series ANY INNER JOIN selected_series USING id ORDER BY value %s",
		selectedSeries,
		latestSource,
		direction,
		limit,
		evalTime.UnixMilli(),
		direction,
	)

	results := make([]sampleResult, 0, limit)
	err := s.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var metricName string
		var labelsJSON string
		var ts int64
		var value float64
		if err := row.Scan(&metricName, &labelsJSON, &ts, &value); err != nil {
			return err
		}
		metric, _, err := metricLabelMap(metricName, []byte(labelsJSON), nil)
		if err != nil {
			return err
		}
		results = append(results, sampleResult{
			Metric: metric,
			Value:  []any{float64(ts) / 1000, formatSample(value)},
		})
		return nil
	})
	if err != nil {
		return queryData{}, true, err
	}
	return queryData{ResultType: string(parser.ValueTypeVector), Result: results}, true, nil
}

type selectorWindow struct {
	mint int64
	maxt int64
}

func selectorWindowFor(selector *parser.VectorSelector, evalTime time.Time, lookback time.Duration) (selectorWindow, bool) {
	if selector.StartOrEnd != 0 {
		return selectorWindow{}, false
	}
	ref := evalTime.UnixMilli()
	if selector.Timestamp != nil {
		ref = *selector.Timestamp
	}
	offset := selector.OriginalOffset
	if offset == 0 {
		offset = selector.Offset
	}
	ref -= offset.Milliseconds()
	return selectorWindow{mint: ref - lookback.Milliseconds(), maxt: ref}, true
}

type instantSeriesExprKind int

const (
	instantSeriesSelector instantSeriesExprKind = iota
	instantSeriesRangeFunction
)

type instantSeriesExprBranch struct {
	kind        instantSeriesExprKind
	selector    *parser.VectorSelector
	window      selectorWindow
	matrixRange time.Duration
	fn          rangeFunction
	transform   scalarTransform
}

func (b instantSeriesExprBranch) equivalentTo(other instantSeriesExprBranch) bool {
	return b.kind == other.kind &&
		b.window == other.window &&
		b.matrixRange == other.matrixRange &&
		b.fn == other.fn &&
		b.transform.signature() == other.transform.signature()
}

type scalarTransformOp struct {
	op         parser.ItemType
	scalar     float64
	scalarLeft bool
}

type scalarTransform struct {
	ops []scalarTransformOp
}

func (t scalarTransform) identity() bool {
	return len(t.ops) == 0
}

func (t scalarTransform) signature() string {
	if len(t.ops) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t.ops))
	for _, op := range t.ops {
		side := "right"
		if op.scalarLeft {
			side = "left"
		}
		parts = append(parts, fmt.Sprintf("%s:%s:%s", op.op.String(), side, strconv.FormatFloat(op.scalar, 'g', -1, 64)))
	}
	return strings.Join(parts, "|")
}

func (t scalarTransform) append(op parser.ItemType, scalar float64, scalarLeft bool) scalarTransform {
	t.ops = append(t.ops, scalarTransformOp{op: op, scalar: scalar, scalarLeft: scalarLeft})
	return t
}

func (t scalarTransform) apply(value string) string {
	for _, op := range t.ops {
		scalar := strconv.FormatFloat(op.scalar, 'g', -1, 64)
		if op.scalarLeft {
			value = scalarBinarySQL(op.op, scalar, value)
		} else {
			value = scalarBinarySQL(op.op, value, scalar)
		}
	}
	return value
}

func scalarBinarySQL(op parser.ItemType, left, right string) string {
	switch op {
	case parser.ADD:
		return "(" + left + " + " + right + ")"
	case parser.SUB:
		return "(" + left + " - " + right + ")"
	case parser.MUL:
		return "(" + left + " * " + right + ")"
	case parser.DIV:
		return "(" + left + " / " + right + ")"
	default:
		return left
	}
}

func scalarArithmeticOperator(op parser.ItemType) bool {
	switch op {
	case parser.ADD, parser.SUB, parser.MUL, parser.DIV:
		return true
	default:
		return false
	}
}

type seriesExprKind int

const (
	seriesExprSelector seriesExprKind = iota
	seriesExprRangeFunction
)

// seriesExprBranch is one operand of a chain of plain `or` operators: a vector
// selector or a single-argument range function over one, with any scalar
// arithmetic folded into transform. node keeps the selector/call expression
// with the scalar arithmetic stripped.
type seriesExprBranch struct {
	node        parser.Expr
	kind        seriesExprKind
	selector    *parser.VectorSelector
	matrixRange time.Duration
	funcName    string
	transform   scalarTransform
}

func (b seriesExprBranch) equivalentTo(other seriesExprBranch) bool {
	return b.kind == other.kind &&
		b.funcName == other.funcName &&
		b.matrixRange == other.matrixRange &&
		b.transform.signature() == other.transform.signature()
}

func seriesExprBranches(expr parser.Expr) ([]seriesExprBranch, bool) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return seriesExprBranches(e.Expr)
	case *parser.BinaryExpr:
		if e.Op == parser.LOR {
			// `or on(...)` / `or ignoring(...)` deduplicate on a label subset,
			// which a series-id union cannot express
			if e.VectorMatching == nil || e.VectorMatching.Card != parser.CardManyToMany || e.VectorMatching.On || len(e.VectorMatching.MatchingLabels) != 0 {
				return nil, false
			}
			left, ok := seriesExprBranches(e.LHS)
			if !ok {
				return nil, false
			}
			right, ok := seriesExprBranches(e.RHS)
			if !ok {
				return nil, false
			}
			return append(left, right...), true
		}
	}
	branch, ok := seriesExprBranchFor(expr)
	if !ok {
		return nil, false
	}
	return []seriesExprBranch{branch}, true
}

func seriesExprBranchFor(expr parser.Expr) (seriesExprBranch, bool) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return seriesExprBranchFor(e.Expr)
	case *parser.VectorSelector:
		if e.Anchored || e.Smoothed {
			return seriesExprBranch{}, false
		}
		return seriesExprBranch{node: e, kind: seriesExprSelector, selector: e}, true
	case *parser.Call:
		if e.Func == nil || len(e.Args) != 1 {
			return seriesExprBranch{}, false
		}
		matrix, ok := e.Args[0].(*parser.MatrixSelector)
		if !ok || matrix.Range <= 0 {
			return seriesExprBranch{}, false
		}
		selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
		if !ok || selector.Anchored || selector.Smoothed {
			return seriesExprBranch{}, false
		}
		return seriesExprBranch{node: e, kind: seriesExprRangeFunction, selector: selector, matrixRange: matrix.Range, funcName: e.Func.Name}, true
	case *parser.BinaryExpr:
		if !scalarArithmeticOperator(e.Op) {
			return seriesExprBranch{}, false
		}
		if scalar, ok := finiteNumber(e.RHS); ok {
			branch, ok := seriesExprBranchFor(e.LHS)
			if !ok {
				return seriesExprBranch{}, false
			}
			branch.transform = branch.transform.append(e.Op, scalar, false)
			return branch, true
		}
		if scalar, ok := finiteNumber(e.LHS); ok {
			branch, ok := seriesExprBranchFor(e.RHS)
			if !ok {
				return seriesExprBranch{}, false
			}
			branch.transform = branch.transform.append(e.Op, scalar, true)
			return branch, true
		}
	}
	return seriesExprBranch{}, false
}

func (s *Server) instantSeriesExprBranches(expr parser.Expr, evalTime time.Time) ([]instantSeriesExprBranch, bool) {
	structural, ok := seriesExprBranches(expr)
	if !ok {
		return nil, false
	}
	branches := make([]instantSeriesExprBranch, 0, len(structural))
	for _, branch := range structural {
		converted, ok := s.instantSeriesExprBranchFor(branch, evalTime)
		if !ok {
			return nil, false
		}
		branches = append(branches, converted)
	}
	return branches, true
}

func (s *Server) instantSeriesExprBranchFor(branch seriesExprBranch, evalTime time.Time) (instantSeriesExprBranch, bool) {
	switch branch.kind {
	case seriesExprSelector:
		window, ok := selectorWindowFor(branch.selector, evalTime, s.cfg.LookbackDelta)
		if !ok {
			return instantSeriesExprBranch{}, false
		}
		return instantSeriesExprBranch{kind: instantSeriesSelector, selector: branch.selector, window: window, transform: branch.transform}, true
	case seriesExprRangeFunction:
		fn, ok := rangeFunctionMode(branch.funcName)
		if !ok {
			return instantSeriesExprBranch{}, false
		}
		window, ok := selectorWindowFor(branch.selector, evalTime, branch.matrixRange+rangeFunctionPreviousSampleLookback(s.cfg, fn))
		if !ok {
			return instantSeriesExprBranch{}, false
		}
		return instantSeriesExprBranch{kind: instantSeriesRangeFunction, selector: branch.selector, window: window, matrixRange: branch.matrixRange, fn: fn, transform: branch.transform}, true
	default:
		return instantSeriesExprBranch{}, false
	}
}

func (s *Server) instantSeriesExprSourceSQL(branch instantSeriesExprBranch, selectors []*parser.VectorSelector) (string, string) {
	union := len(selectors) > 1
	switch branch.kind {
	case instantSeriesSelector:
		var latest string
		if union {
			latest = latestSamplesForSelectedSeriesUnionSQL(s.cfg, exactMetricNamesForSelectors(selectors), branch.window.mint, branch.window.maxt)
		} else {
			latest = latestSamplesForSelectedSeriesSQL(s.cfg, branch.selector.LabelMatchers, branch.window.mint, branch.window.maxt)
		}
		return transformValueSQL(latest, branch.transform), "latest"
	case instantSeriesRangeFunction:
		var perSeries string
		if union {
			perSeries = rangeFunctionPerSeriesUnionSQL(s.cfg, branch.window, exactMetricNamesForSelectors(selectors), branch.fn)
		} else {
			perSeries = rangeFunctionPerSeriesSQL(s.cfg, branch.window, branch.selector.LabelMatchers, branch.fn)
		}
		if branch.fn.increase {
			perSeries = fmt.Sprintf("SELECT id, value * %s AS value FROM (%s)", formatDurationSeconds(branch.matrixRange), perSeries)
		}
		return transformValueSQL(perSeries, branch.transform), "per_series"
	default:
		return "", ""
	}
}

func transformValueSQL(source string, transform scalarTransform) string {
	if transform.identity() {
		return source
	}
	return fmt.Sprintf("SELECT id, %s AS value FROM (%s)", transform.apply("value"), source)
}

func transformGridValsSQL(gridExpr string, transform scalarTransform) string {
	if transform.identity() {
		return gridExpr
	}
	return fmt.Sprintf("arrayMap(x -> if(isNull(x), NULL, %s), %s)", transform.apply("x"), gridExpr)
}

type rangeFunction struct {
	counter            bool
	rate               bool
	instant            bool
	increase           bool
	usesPreviousSample bool
}

func rangeFunctionMode(name string) (rangeFunction, bool) {
	switch name {
	case "rate":
		return rangeFunction{counter: true, rate: true}, true
	case "irate":
		return rangeFunction{counter: true, rate: true, instant: true}, true
	case "increase":
		return rangeFunction{counter: true, rate: true, increase: true}, true
	case "delta":
		return rangeFunction{counter: false, rate: false, usesPreviousSample: true}, true
	case "idelta":
		return rangeFunction{counter: false, rate: false, instant: true, usesPreviousSample: true}, true
	default:
		return rangeFunction{}, false
	}
}

func rangeFunctionPreviousSampleLookback(cfg Config, fn rangeFunction) time.Duration {
	if !fn.usesPreviousSample || cfg.RemoteWriteInterval <= 0 {
		return 0
	}
	return cfg.RemoteWriteInterval
}

func rangeFunctionSampleSourceSQL(cfg Config, window selectorWindow, matchers []*labels.Matcher) string {
	source := samplesForSelectedSeriesSQL(cfg, matchers, window.mint, window.maxt)
	return rangeFunctionSampleSourceFromSamplesSQL(source)
}

func rangeFunctionSampleSourceFromSamplesSQL(source string) string {
	return "SELECT * FROM (" + sampleArraySourceFromSamplesSQL(source) + ") WHERE length(vals) > 1"
}

func sampleArraySourceFromSamplesSQL(source string) string {
	return fmt.Sprintf(
		"SELECT id, arrayMap(x -> x.1, pts) AS ts, arrayMap(x -> x.2, pts) AS vals FROM (SELECT id, arraySort(x -> x.1, groupArray((toUnixTimestamp64Milli(timestamp), value))) AS pts FROM (%s) GROUP BY id)",
		source,
	)
}

func rangeFunctionPerSeriesSQL(cfg Config, window selectorWindow, matchers []*labels.Matcher, fn rangeFunction) string {
	source := rangeFunctionSampleSourceSQL(cfg, window, matchers)
	return rangeFunctionPerSeriesFromSourceSQL(window, source, fn)
}

func rangeFunctionPerSeriesUnionSQL(cfg Config, window selectorWindow, metricNames []string, fn rangeFunction) string {
	source := samplesForSelectedSeriesUnionSQL(cfg, metricNames, window.mint, window.maxt)
	return rangeFunctionPerSeriesFromSourceSQL(window, rangeFunctionSampleSourceFromSamplesSQL(source), fn)
}

func rangeFunctionPerSeriesFromSourceSQL(window selectorWindow, source string, fn rangeFunction) string {
	if fn.instant {
		result := "if(last_v < prev_v, last_v, last_v - prev_v)"
		if !fn.counter {
			result = "last_v - prev_v"
		}
		if fn.rate {
			result = "(" + result + ") / ((last_t - prev_t) / 1000.0)"
		}
		return fmt.Sprintf(
			"SELECT id, %s AS value FROM (SELECT id, vals[-2] AS prev_v, vals[-1] AS last_v, ts[-2] AS prev_t, ts[-1] AS last_t FROM (%s) WHERE length(vals) >= 2 AND last_t > prev_t)",
			result,
			source,
		)
	}
	rangeSeconds := float64(window.maxt-window.mint) / 1000.0
	resetCorrection := "0.0"
	durationToStart := "duration_to_start1"
	if fn.counter {
		resetCorrection = "arraySum(arrayMap((prev, curr) -> if(curr < prev, prev, 0.0), arrayPopBack(vals), arrayPopFront(vals)))"
		durationToStart = "duration_to_start2"
	}
	result := "raw_delta + reset_correction"
	factor := "(sampled_interval + " + durationToStart + " + duration_to_end2) / sampled_interval"
	if fn.rate {
		factor += " / " + strconv.FormatFloat(rangeSeconds, 'f', -1, 64)
	}
	return fmt.Sprintf(
		"SELECT id, ((%s) * (%s)) AS value FROM (SELECT id, vals[1] AS first_v, vals[-1] AS last_v, ts[1] AS first_t, ts[-1] AS last_t, length(vals)-1 AS num_minus_one, (last_v - first_v) AS raw_delta, %s AS reset_correction, (last_t - first_t) / 1000.0 AS sampled_interval, (first_t - %d) / 1000.0 AS duration_to_start0, (%d - last_t) / 1000.0 AS duration_to_end0, sampled_interval / num_minus_one AS avg_between, avg_between * 1.1 AS threshold, if(duration_to_start0 >= threshold, avg_between / 2, duration_to_start0) AS duration_to_start1, if(raw_delta + reset_correction > 0 AND first_v >= 0, sampled_interval * (first_v / (raw_delta + reset_correction)), duration_to_start1) AS duration_to_zero, if(duration_to_zero < duration_to_start1, duration_to_zero, duration_to_start1) AS duration_to_start2, if(duration_to_end0 >= threshold, avg_between / 2, duration_to_end0) AS duration_to_end2 FROM (%s))",
		result,
		factor,
		resetCorrection,
		window.mint,
		window.maxt,
		source,
	)
}

func aggregateSQL(expr *parser.AggregateExpr, valueColumn string) (string, bool) {
	switch expr.Op {
	case parser.SUM:
		return "sum(" + valueColumn + ")", true
	case parser.AVG:
		return "avg(" + valueColumn + ")", true
	case parser.COUNT:
		return "toFloat64(count())", true
	case parser.MIN:
		return "min(" + valueColumn + ")", true
	case parser.MAX:
		return "max(" + valueColumn + ")", true
	case parser.GROUP:
		return "max(toFloat64(1))", true
	case parser.QUANTILE:
		quantile, ok := finiteNumber(expr.Param)
		if !ok || quantile < 0 || quantile > 1 {
			return "", false
		}
		return fmt.Sprintf("quantileExact(%s)(%s)", strconv.FormatFloat(quantile, 'f', -1, 64), valueColumn), true
	default:
		return "", false
	}
}

func finiteNumber(expr parser.Expr) (float64, bool) {
	switch e := expr.(type) {
	case *parser.NumberLiteral:
		if math.IsNaN(e.Val) || math.IsInf(e.Val, 0) {
			return 0, false
		}
		return e.Val, true
	case *parser.UnaryExpr:
		value, ok := finiteNumber(e.Expr)
		if !ok {
			return 0, false
		}
		switch e.Op {
		case parser.ADD:
			return value, true
		case parser.SUB:
			return -value, true
		default:
			return 0, false
		}
	default:
		return 0, false
	}
}

func withMaxThreads(sql string, maxThreads int) string {
	if maxThreads <= 0 {
		return sql
	}
	return sql + " SETTINGS max_threads = " + strconv.Itoa(maxThreads)
}

func aggregateLimit(expr parser.Expr) (int, bool) {
	value, ok := finiteNumber(expr)
	if !ok || value < 0 {
		return 0, false
	}
	return int(value), true
}

func exprHasFunction(expr parser.Expr, name string) bool {
	found := false
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if found {
			return errors.New("done")
		}
		call, ok := node.(*parser.Call)
		if ok && call.Func != nil && call.Func.Name == name {
			found = true
			return errors.New("done")
		}
		return nil
	})
	return found
}

func matchersPushdownSafe(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if matcherIsNoop(m) {
			continue
		}
		switch m.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if m.Name != labels.MetricName && m.Matches("") {
				return false
			}
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			if m.Name != labels.MetricName && !m.Matches("") {
				return false
			}
		default:
			return false
		}
	}
	return len(matchers) > 0
}

func matcherIsNoop(m *labels.Matcher) bool {
	if m.Type != labels.MatchRegexp {
		return false
	}
	switch strings.TrimSpace(m.Value) {
	case ".*", "(.*)", "(?:.*)", ".*|", "|.*", "(.*)|", "|(.*)", "(?:.*)|", "|(?:.*)":
		return true
	default:
		return false
	}
}

func selectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, selectParts []string) (string, bool) {
	if !matchersPushdownSafe(matchers) {
		return "", false
	}
	if len(selectParts) == 0 {
		selectParts = []string{"id"}
	}
	if selectPartsIDOnly(selectParts) {
		if sql, ok := selectedSeriesIDsFromLabelIndexSQL(cfg, matchers); ok {
			return sql, true
		}
	}
	where := []string{
		teamFilter(cfg),
	}
	if selectedSeriesNeedsBounds(selectParts) {
		where = append(where, seriesTimeFilters(cfg, matchers, mint, maxt)...)
	}
	where = append(where, seriesPreFilters(cfg, matchers)...)
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT %s FROM %s WHERE %s) GROUP BY id",
		strings.Join(selectedSeriesProjection(selectParts), ", "),
		selectedSeriesSourceColumns(selectParts),
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	), true
}

func selectedSeriesIDsFromLabelIndexSQL(cfg Config, matchers []*labels.Matcher) (string, bool) {
	metric := exactMetricName(matchers)
	if metric == "" || cfg.LabelIndexTable == "" {
		return "", false
	}
	base := -1
	for i, matcher := range matchers {
		membership, _, ok := labelIndexMembershipCondition(matcher)
		if ok && membership == "IN" && (base < 0 || matcher.Type == labels.MatchEqual) {
			base = i
			if matcher.Type == labels.MatchEqual {
				break
			}
		}
	}
	if base < 0 {
		return "", false
	}

	table := tableName(cfg.CHDatabase, cfg.LabelIndexTable)
	filters := []string{teamFilter(cfg), "metric_name = " + sqlString(metric)}
	for i, matcher := range matchers {
		if matcherIsNoop(matcher) || matcher.Name == labels.MetricName {
			continue
		}
		membership, condition, ok := labelIndexMembershipCondition(matcher)
		if !ok {
			return "", false
		}
		predicate := "label_name = " + sqlString(matcher.Name) + " AND " + condition
		if i == base {
			filters = append(filters, predicate)
			continue
		}
		filters = append(filters, fmt.Sprintf(
			"id %s (SELECT id FROM %s WHERE %s AND metric_name = %s AND %s)",
			membership, table, teamFilter(cfg), sqlString(metric), predicate,
		))
	}
	return fmt.Sprintf("SELECT id FROM %s WHERE %s GROUP BY id", table, strings.Join(filters, " AND ")), true
}

func selectedSeriesForInstantBranchesSQL(cfg Config, branches []instantSeriesExprBranch, selectors []*parser.VectorSelector, selectParts []string) (string, bool) {
	windows := make([]selectorWindow, 0, len(branches))
	for _, branch := range branches {
		windows = append(windows, branch.window)
	}
	return selectedSeriesUnionSQL(cfg, selectors, windows, selectParts)
}

func selectedSeriesUnionSQL(cfg Config, selectors []*parser.VectorSelector, windows []selectorWindow, selectParts []string) (string, bool) {
	if len(selectors) == 0 || len(selectors) != len(windows) {
		return "", false
	}
	if len(selectors) > 1 {
		if sql, ok := selectedSeriesExactMatcherUnionSQL(cfg, selectors, selectParts); ok {
			return sql, true
		}
	}

	selectedParts := make([]string, 0, len(selectors))
	for i, selector := range selectors {
		selectedSeries, ok := selectedSeriesSQL(cfg, selector.LabelMatchers, windows[i].mint, windows[i].maxt, selectParts)
		if !ok {
			return "", false
		}
		selectedParts = append(selectedParts, selectedSeries)
	}
	if len(selectedParts) == 1 {
		return selectedParts[0], true
	}
	return strings.Join(selectedParts, " UNION DISTINCT "), true
}

type exactLabelMatcher struct {
	name  string
	value string
}

type exactSelectorMatcherSet struct {
	metric string
	labels []exactLabelMatcher
}

func selectedSeriesExactMatcherUnionSQL(cfg Config, selectors []*parser.VectorSelector, selectParts []string) (string, bool) {
	if len(selectors) < 2 || selectedSeriesNeedsBounds(selectParts) {
		return "", false
	}
	sets := make([]exactSelectorMatcherSet, 0, len(selectors))
	metricNames := make([]string, 0, len(selectors))
	seenMetrics := make(map[string]struct{}, len(selectors))
	for _, selector := range selectors {
		set, ok := exactSelectorMatcherSetFor(selector.LabelMatchers)
		if !ok {
			return "", false
		}
		sets = append(sets, set)
		if _, ok := seenMetrics[set.metric]; !ok {
			seenMetrics[set.metric] = struct{}{}
			metricNames = append(metricNames, set.metric)
		}
	}

	matchedIDs := exactMatcherUnionIDsSQL(cfg, sets)
	if selectPartsIDOnly(selectParts) {
		return "SELECT id FROM (" + matchedIDs + ") GROUP BY id", true
	}

	where := []string{teamFilter(cfg), "id IN (" + matchedIDs + ")"}
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	return fmt.Sprintf(
		"SELECT %s FROM (SELECT %s FROM %s WHERE %s) GROUP BY id",
		strings.Join(selectedSeriesProjection(selectParts), ", "),
		selectedSeriesSourceColumns(selectParts),
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		strings.Join(where, " AND "),
	), true
}

func selectedSeriesExactMatcherUnionBranchesSQL(cfg Config, selectors []*parser.VectorSelector, grouping []string) (string, [][]string, bool) {
	if len(selectors) < 2 || len(grouping) == 0 {
		return "", nil, false
	}
	sets := make([]exactSelectorMatcherSet, 0, len(selectors))
	groupValues := make([][]string, len(grouping))
	for _, selector := range selectors {
		set, ok := exactSelectorMatcherSetFor(selector.LabelMatchers)
		if !ok || len(set.labels) < 2 {
			return "", nil, false
		}
		sets = append(sets, set)
		for i, name := range grouping {
			value := set.metric
			found := name == labels.MetricName
			if !found {
				for _, matcher := range set.labels {
					if matcher.name == name {
						value = matcher.value
						found = true
						break
					}
				}
			}
			if !found {
				return "", nil, false
			}
			groupValues[i] = append(groupValues[i], value)
		}
	}
	return "SELECT id, min(branch) AS branch FROM (" + exactMatcherUnionBranchIDsSQL(cfg, sets) + ") GROUP BY id", groupValues, true
}

func selectPartsIDOnly(selectParts []string) bool {
	return len(selectParts) == 1 && selectParts[0] == "id"
}

func exactSelectorMatcherSetFor(matchers []*labels.Matcher) (exactSelectorMatcherSet, bool) {
	if len(matchers) == 0 {
		return exactSelectorMatcherSet{}, false
	}
	var metric string
	seenLabels := make(map[string]struct{}, len(matchers))
	labelMatchers := make([]exactLabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) {
			continue
		}
		if matcher.Name == labels.MetricName {
			if matcher.Type != labels.MatchEqual || matcher.Value == "" {
				return exactSelectorMatcherSet{}, false
			}
			if metric != "" && metric != matcher.Value {
				return exactSelectorMatcherSet{}, false
			}
			metric = matcher.Value
			continue
		}
		if matcher.Name == "" || matcher.Type != labels.MatchEqual || matcher.Matches("") {
			return exactSelectorMatcherSet{}, false
		}
		if _, ok := seenLabels[matcher.Name]; ok {
			return exactSelectorMatcherSet{}, false
		}
		seenLabels[matcher.Name] = struct{}{}
		labelMatchers = append(labelMatchers, exactLabelMatcher{name: matcher.Name, value: matcher.Value})
	}
	// One bit per matcher tracks branch completeness in SQL, so a branch can
	// carry at most 64 of them.
	if metric == "" || len(labelMatchers) == 0 || len(labelMatchers) > 64 {
		return exactSelectorMatcherSet{}, false
	}
	return exactSelectorMatcherSet{metric: metric, labels: labelMatchers}, true
}

func exactMatcherUnionIDsSQL(cfg Config, sets []exactSelectorMatcherSet) string {
	if sql, ok := singleExactLabelUnionIDsSQL(cfg, sets); ok {
		return sql
	}
	return "SELECT id FROM (" + exactMatcherUnionBranchIDsSQL(cfg, sets) + ") GROUP BY id"
}

func exactMatcherUnionBranchIDsSQL(cfg Config, sets []exactSelectorMatcherSet) string {
	matcherRows := make([]string, 0, len(sets))
	metricNames := make([]string, 0, len(sets))
	labelNames := make([]string, 0, len(sets))
	labelValues := make([]string, 0, len(sets))
	seenMetricNames := make(map[string]struct{}, len(sets))
	seenLabelNames := make(map[string]struct{}, len(sets))
	seenLabelValues := make(map[string]struct{}, len(sets))
	for branch, set := range sets {
		if _, ok := seenMetricNames[set.metric]; !ok {
			seenMetricNames[set.metric] = struct{}{}
			metricNames = append(metricNames, set.metric)
		}
		for position, matcher := range set.labels {
			if _, ok := seenLabelNames[matcher.name]; !ok {
				seenLabelNames[matcher.name] = struct{}{}
				labelNames = append(labelNames, matcher.name)
			}
			if _, ok := seenLabelValues[matcher.value]; !ok {
				seenLabelValues[matcher.value] = struct{}{}
				labelValues = append(labelValues, matcher.value)
			}
			matcherRows = append(matcherRows, fmt.Sprintf(
				"(%d, %d, %d, %s, %s, %s)",
				branch,
				fullBranchMask(len(set.labels)),
				uint64(1)<<uint(position),
				sqlString(set.metric),
				sqlString(matcher.name),
				sqlString(matcher.value),
			))
		}
	}
	where := []string{teamFilter(cfg)}
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	where = append(where, inCondition("label_name", labelNames))
	where = append(where, inCondition("label_value", labelValues))
	// A series matches a branch once it has hit every one of that branch's
	// label matchers. Counting distinct label names for that check builds a
	// string hash set per (branch, id) group, which dominated dashboard CPU;
	// OR-ing one bit per matcher is exact, duplicate-safe, and a plain UInt64.
	return fmt.Sprintf(
		"SELECT mr.branch AS branch, li.id AS id FROM %s AS li INNER JOIN %s AS mr ON li.metric_name = mr.metric_name AND li.label_name = mr.label_name AND li.label_value = mr.label_value WHERE %s GROUP BY branch, id, full_mask HAVING groupBitOr(mr.match_bit) = full_mask",
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		"values('branch UInt32, full_mask UInt64, match_bit UInt64, metric_name String, label_name String, label_value String', "+strings.Join(matcherRows, ", ")+")",
		strings.Join(where, " AND "),
	)
}

// fullBranchMask is the set of matcher bits a series must hit to match a branch.
func fullBranchMask(matchers int) uint64 {
	return uint64(1)<<uint(matchers) - 1
}

func singleExactLabelUnionIDsSQL(cfg Config, sets []exactSelectorMatcherSet) (string, bool) {
	if len(sets) == 0 {
		return "", false
	}
	metric := sets[0].metric
	labelName := ""
	values := make([]string, 0, len(sets))
	seenValues := make(map[string]struct{}, len(sets))
	for _, set := range sets {
		if set.metric != metric || len(set.labels) != 1 {
			return "", false
		}
		label := set.labels[0]
		if labelName == "" {
			labelName = label.name
		}
		if label.name != labelName {
			return "", false
		}
		if _, ok := seenValues[label.value]; ok {
			continue
		}
		seenValues[label.value] = struct{}{}
		values = append(values, label.value)
	}
	where := []string{
		teamFilter(cfg),
		"metric_name = " + sqlString(metric),
		"label_name = " + sqlString(labelName),
		inCondition("label_value", values),
	}
	return fmt.Sprintf(
		"SELECT id FROM %s WHERE %s GROUP BY id",
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		strings.Join(where, " AND "),
	), true
}

func inCondition(column string, values []string) string {
	if len(values) == 1 {
		return column + " = " + sqlString(values[0])
	}
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, sqlString(value))
	}
	return column + " IN (" + strings.Join(quoted, ",") + ")"
}

func selectedSeriesNeedsBounds(selectParts []string) bool {
	for _, part := range selectParts {
		if part == "min_time" || part == "max_time" {
			return true
		}
	}
	return false
}

func selectedSeriesProjection(selectParts []string) []string {
	projected := make([]string, 0, len(selectParts))
	for _, part := range selectParts {
		switch part {
		case "id":
			projected = append(projected, "id")
		case "metric_name":
			projected = append(projected, "any(metric_name) AS metric_name")
		case "labels_json":
			projected = append(projected, "any(labels_json) AS labels_json")
		case "min_time":
			projected = append(projected, "min(min_time) AS min_time")
		case "max_time":
			projected = append(projected, "max(max_time) AS max_time")
		default:
			projected = append(projected, part)
		}
	}
	return projected
}

func selectedSeriesSourceColumns(selectParts []string) string {
	all := "id, metric_name, labels_json, min_time, max_time"
	columns := []string{"id"}
	add := func(column string) {
		for _, existing := range columns {
			if existing == column {
				return
			}
		}
		columns = append(columns, column)
	}
	for _, part := range selectParts {
		switch part {
		case "id":
		case "metric_name", "labels_json", "min_time", "max_time":
			add(part)
		default:
			return all
		}
	}
	return strings.Join(columns, ", ")
}

func latestSamplesForSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64) string {
	where := sampleBaseFilters(cfg, matchers, mint, maxt)
	if !metricOnlyMatchers(matchers) {
		where = append(where, sampleSelectedSeriesFilters(cfg)...)
	}
	source := rawSamplesSourceSQL(cfg, strings.Join(where, " AND "))
	latest := fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value, max(timestamp) AS ts_col FROM (%s) GROUP BY id",
		source,
	)
	return fmt.Sprintf("SELECT id, value, ts_col FROM (%s) WHERE %s", latest, nonStaleSampleSQL("value"))
}

func samplesForSelectedSeriesUnionSQL(cfg Config, metricNames []string, mint, maxt int64) string {
	where := []string{teamFilter(cfg)}
	where = append(where, sampleTimeFilters(cfg, mint, maxt)...)
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	where = append(where, sampleSelectedSeriesFilters(cfg)...)
	return rawSamplesSourceSQL(cfg, strings.Join(where, " AND "))
}

func latestSamplesForSelectedSeriesUnionSQL(cfg Config, metricNames []string, mint, maxt int64) string {
	source := samplesForSelectedSeriesUnionSQL(cfg, metricNames, mint, maxt)
	latest := fmt.Sprintf(
		"SELECT id, argMax(value, timestamp) AS value, max(timestamp) AS ts_col FROM (%s) GROUP BY id",
		source,
	)
	return fmt.Sprintf("SELECT id, value, ts_col FROM (%s) WHERE %s", latest, nonStaleSampleSQL("value"))
}

func exactMetricNamesForSelectors(selectors []*parser.VectorSelector) []string {
	names := make([]string, 0, len(selectors))
	seen := make(map[string]struct{}, len(selectors))
	for _, selector := range selectors {
		name := exactMetricName(selector.LabelMatchers)
		if name == "" {
			return nil
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func commonExactMetricMatchers(selectors []*parser.VectorSelector) []*labels.Matcher {
	if len(selectors) == 0 {
		return nil
	}
	names := exactMetricNamesForSelectors(selectors)
	if len(names) != 1 {
		return nil
	}
	return selectors[0].LabelMatchers
}

func metricNamesCondition(metricNames []string) string {
	switch len(metricNames) {
	case 0:
		return ""
	case 1:
		return "metric_name = " + sqlString(metricNames[0])
	default:
		quoted := make([]string, 0, len(metricNames))
		for _, name := range metricNames {
			quoted = append(quoted, sqlString(name))
		}
		return "metric_name IN (" + strings.Join(quoted, ",") + ")"
	}
}

func metricGroupProjection(grouping []string) []string {
	for _, name := range grouping {
		if name == labels.MetricName {
			return []string{"metric_name"}
		}
	}
	return nil
}

func labelIndexGroupSQL(grouping []string) ([]string, []string) {
	selects := make([]string, 0, len(grouping))
	groupBy := make([]string, 0, len(grouping))
	for i, name := range grouping {
		alias := groupAlias(i)
		if name == labels.MetricName {
			selects = append(selects, "selected_series.metric_name AS "+quoteIdent(alias))
		} else {
			selects = append(selects, "ifNull(group_labels."+quoteIdent(alias)+", '') AS "+quoteIdent(alias))
		}
		groupBy = append(groupBy, quoteIdent(alias))
	}
	return selects, groupBy
}

func exactMatcherGroupSQL(matchers []*labels.Matcher, grouping []string) ([]string, bool) {
	set, ok := exactSelectorMatcherSetFor(matchers)
	if !ok {
		return nil, false
	}
	selects := make([]string, 0, len(grouping))
	for i, name := range grouping {
		value := set.metric
		found := name == labels.MetricName
		if !found {
			for _, matcher := range set.labels {
				if matcher.name == name {
					value = matcher.value
					found = true
					break
				}
			}
		}
		if !found {
			return nil, false
		}
		selects = append(selects, sqlString(value)+" AS "+quoteIdent(groupAlias(i)))
	}
	return selects, true
}

func postHogSampleGroupSQL(grouping []string) ([]string, []string, []string, bool) {
	selects := make([]string, 0, len(grouping))
	groupBy := make([]string, 0, len(grouping))
	perIDSelects := make([]string, 0, len(grouping))
	for i, name := range grouping {
		alias := quoteIdent(groupAlias(i))
		expr, ok := postHogSampleGroupExpr(name)
		if !ok {
			return nil, nil, nil, false
		}
		perIDSelects = append(perIDSelects, "argMax("+expr+", timestamp) AS "+alias)
		selects = append(selects, alias)
		groupBy = append(groupBy, alias)
	}
	return selects, groupBy, perIDSelects, true
}

func postHogRangeGroupSQL(grouping []string) ([]string, []string, bool) {
	selects := make([]string, 0, len(grouping))
	groupBy := make([]string, 0, len(grouping))
	for i, name := range grouping {
		alias := quoteIdent(groupAlias(i))
		expr, ok := postHogSampleGroupExpr(name)
		if !ok {
			return nil, nil, false
		}
		selects = append(selects, expr+" AS "+alias)
		groupBy = append(groupBy, alias)
	}
	return selects, groupBy, true
}

func postHogSampleGroupExprs(grouping []string) ([]string, bool) {
	exprs := make([]string, 0, len(grouping))
	for _, name := range grouping {
		expr, ok := postHogSampleGroupExpr(name)
		if !ok {
			return nil, false
		}
		exprs = append(exprs, expr)
	}
	return exprs, true
}

func postHogUniqGroupExpr(exprs []string) string {
	if len(exprs) == 1 {
		return exprs[0]
	}
	return "tuple(" + strings.Join(exprs, ", ") + ")"
}

func postHogSampleGroupExpr(name string) (string, bool) {
	switch name {
	case labels.MetricName:
		return "metric_name", true
	case "service_name":
		return "service_name", true
	case "":
		return "", false
	default:
		return postHogLabelValueExpr(name), true
	}
}

func aggregateSourceSQL(base string, grouping []string, groupJoin string) string {
	source := base
	if groupingHasMetricName(grouping) {
		source += " ANY INNER JOIN selected_series USING id"
	}
	return source + groupJoin
}

func groupingHasMetricName(grouping []string) bool {
	for _, name := range grouping {
		if name == labels.MetricName {
			return true
		}
	}
	return false
}

func labelIndexGroupLabelsSQL(cfg Config, matchers []*labels.Matcher, grouping []string) (string, string) {
	return labelIndexGroupLabelsSQLWithSelectedFilter(cfg, matchers, grouping, true)
}

func labelIndexGroupLabelsSQLWithSelectedFilter(cfg Config, matchers []*labels.Matcher, grouping []string, filterToSelected bool) (string, string) {
	labelGroups := make([]struct {
		index int
		name  string
	}, 0, len(grouping))
	seen := make(map[string]struct{})
	for i, name := range grouping {
		if name == labels.MetricName {
			continue
		}
		key := name
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		labelGroups = append(labelGroups, struct {
			index int
			name  string
		}{index: i, name: name})
	}
	if len(labelGroups) == 0 {
		return "", ""
	}

	selects := []string{"id"}
	names := make([]string, 0, len(labelGroups))
	for _, group := range labelGroups {
		names = append(names, sqlString(group.name))
		selects = append(selects, fmt.Sprintf(
			"maxIf(label_value, label_name = %s) AS %s",
			sqlString(group.name),
			quoteIdent(groupAlias(group.index)),
		))
	}
	where := []string{
		teamFilter(cfg),
		"label_name IN (" + strings.Join(names, ", ") + ")",
	}
	metric := exactMetricName(matchers)
	if metric != "" {
		where = append(where, "metric_name = "+sqlString(metric))
	}
	if filterToSelected && metric == "" {
		where = append(where, "id IN (SELECT id FROM selected_series)")
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s GROUP BY id",
		strings.Join(selects, ", "),
		tableName(cfg.CHDatabase, cfg.LabelIndexTable),
		strings.Join(where, " AND "),
	), " ANY LEFT JOIN group_labels USING id"
}

func groupAlias(index int) string {
	return fmt.Sprintf("__group_%d", index)
}
