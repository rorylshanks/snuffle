package snuffle

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type CHQueryable struct {
	client *ClickHouseClient
	cfg    Config
}

func NewCHQueryable(client *ClickHouseClient, cfg Config) *CHQueryable {
	return &CHQueryable{client: client, cfg: cfg}
}

func (q *CHQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &CHQuerier{queryable: q, mint: mint, maxt: maxt}, nil
}

type CHQuerier struct {
	queryable *CHQueryable
	mint      int64
	maxt      int64
	selects   map[string]*selectFuture
}

func (q *CHQuerier) Close() error {
	return nil
}

func (q *CHQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if matchersUnsatisfiable(matchers) {
		return storage.EmptySeriesSet()
	}
	sortResult := shouldSortSeries(sortSeries, hints)
	cacheKey := selectCacheKey(sortSeries, hints, matchers)
	if future, ok := q.selects[cacheKey]; ok {
		return &futureSeriesSet{future: future, sortSeries: sortResult}
	}
	if q.selects == nil {
		q.selects = make(map[string]*selectFuture)
	}
	future := &selectFuture{done: make(chan struct{})}
	q.selects[cacheKey] = future
	go func() {
		future.series, future.err = q.selectSeriesMeta(ctx, hints, matchers...)
		close(future.done)
	}()
	return &futureSeriesSet{future: future, sortSeries: sortResult}
}

func (q *CHQuerier) selectSeriesMeta(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	if q.queryable.cfg.postHogSchemaLayout() {
		latestOnly := hints != nil && hints.Range == 0 && hints.Step == 0 && q.queryable.cfg.HistogramsTable == ""
		series, err := q.selectPostHogSeriesSamples(ctx, q.mint, q.maxt, latestOnly, matchers...)
		if err != nil {
			return nil, err
		}
		if hints != nil && hints.Limit > 0 && len(series) > hints.Limit {
			series = series[:hints.Limit]
		}
		return series, nil
	}

	latestOnly := hints != nil && hints.Range == 0 && hints.Step == 0 && q.queryable.cfg.HistogramsTable == ""
	if latestOnly {
		series, ok, err := q.selectLatestSeriesSamples(ctx, q.mint, q.maxt, matchers...)
		if err != nil {
			return nil, err
		}
		if ok {
			if hints != nil && hints.Limit > 0 && len(series) > hints.Limit {
				series = series[:hints.Limit]
			}
			return series, nil
		}
	}

	series, err := q.selectSeries(ctx, q.mint, q.maxt, matchers...)
	if err != nil {
		return nil, err
	}
	if hints != nil && hints.Limit > 0 && len(series) > hints.Limit {
		series = series[:hints.Limit]
	}

	if err := q.loadSamples(ctx, series, q.mint, q.maxt, latestOnly, matchers); err != nil {
		return nil, err
	}
	if err := q.loadHistograms(ctx, series, q.mint, q.maxt, matchers); err != nil {
		return nil, err
	}
	return series, nil
}

func matchersUnsatisfiable(matchers []*labels.Matcher) bool {
	byName := make(map[string][]*labels.Matcher)
	for _, matcher := range matchers {
		byName[matcher.Name] = append(byName[matcher.Name], matcher)
	}
	for _, matcher := range matchers {
		var candidates []string
		switch matcher.Type {
		case labels.MatchEqual:
			candidates = []string{matcher.Value}
		case labels.MatchRegexp:
			candidates = matcher.SetMatches()
		}
		if len(candidates) == 0 {
			continue
		}
		for _, candidate := range candidates {
			matchesAll := true
			for _, other := range byName[matcher.Name] {
				if !other.Matches(candidate) {
					matchesAll = false
					break
				}
			}
			if matchesAll {
				candidates = nil
				break
			}
		}
		if len(candidates) > 0 {
			return true
		}
	}
	return false
}

func selectCacheKey(sortSeries bool, hints *storage.SelectHints, matchers []*labels.Matcher) string {
	var key strings.Builder
	fmt.Fprintf(&key, "%t|%#v", sortSeries, hints)
	for _, matcher := range matchers {
		fmt.Fprintf(&key, "|%d:%q:%q", matcher.Type, matcher.Name, matcher.Value)
	}
	return key.String()
}

func shouldSortSeries(sortSeries bool, hints *storage.SelectHints) bool {
	return sortSeries || (hints != nil && hints.Step == 0)
}

type selectFuture struct {
	done   chan struct{}
	series []*seriesMeta
	err    error
}

type futureSeriesSet struct {
	future     *selectFuture
	sortSeries bool
	set        storage.SeriesSet
}

func (s *futureSeriesSet) wait() {
	if s.set != nil {
		return
	}
	<-s.future.done
	if s.future.err != nil {
		s.set = storage.ErrSeriesSet(s.future.err)
		return
	}
	s.set = seriesSetFromMeta(s.future.series, s.sortSeries)
}

func (s *futureSeriesSet) Next() bool {
	s.wait()
	return s.set.Next()
}

func (s *futureSeriesSet) At() storage.Series {
	s.wait()
	return s.set.At()
}

func (s *futureSeriesSet) Err() error {
	s.wait()
	return s.set.Err()
}

func (s *futureSeriesSet) Warnings() annotations.Annotations {
	s.wait()
	return s.set.Warnings()
}

func seriesSetFromMeta(series []*seriesMeta, sortSeries bool) storage.SeriesSet {
	if len(series) == 0 {
		return storage.EmptySeriesSet()
	}
	if sortSeries {
		series = append([]*seriesMeta(nil), series...)
		sort.Slice(series, func(i, j int) bool {
			return labels.Compare(series[i].labels, series[j].labels) < 0
		})
	}
	result := make([]storage.Series, len(series))
	for i := range series {
		result[i] = series[i]
	}
	return &seriesSet{series: result, idx: -1}
}

func (q *CHQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	if q.queryable.cfg.postHogSchemaLayout() {
		names, err := q.postHogLabelNames(ctx, limit, matchers...)
		return names, nil, err
	}
	if len(matchers) > 0 {
		if names, ok, err := q.labelNamesFromIndex(ctx, limit, matchers...); ok || err != nil {
			return names, nil, err
		}
		series, err := q.selectSeries(ctx, q.mint, q.maxt, matchers...)
		if err != nil {
			return nil, nil, err
		}
		names := map[string]struct{}{labels.MetricName: {}}
		for _, batch := range idBatches(seriesIDs(series), q.queryable.cfg.IDChunkSize) {
			sql := fmt.Sprintf(
				"SELECT DISTINCT label_name FROM %s WHERE %s AND id IN (%s)",
				tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.LabelIndexTable),
				teamFilter(q.queryable.cfg),
				joinUint64(batch),
			)
			if err := q.addStringRows(ctx, names, sql); err != nil {
				return nil, nil, err
			}
		}
		return sortedLimited(names, limit), nil, nil
	}

	names := map[string]struct{}{labels.MetricName: {}}
	sql := fmt.Sprintf(
		"SELECT DISTINCT label_name FROM %s WHERE %s ORDER BY label_name%s",
		tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.LabelIndexTable),
		teamFilter(q.queryable.cfg),
		sqlLimit(limit),
	)
	if err := q.addStringRows(ctx, names, sql); err != nil {
		return nil, nil, err
	}
	return sortedLimited(names, limit), nil, nil
}

func (q *CHQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	if q.queryable.cfg.postHogSchemaLayout() {
		values, err := q.postHogLabelValues(ctx, name, limit, matchers...)
		return values, nil, err
	}
	if len(matchers) > 0 {
		if values, ok, err := q.labelValuesFromIndex(ctx, name, limit, matchers...); ok || err != nil {
			return values, nil, err
		}
		series, err := q.selectSeries(ctx, q.mint, q.maxt, matchers...)
		if err != nil {
			return nil, nil, err
		}
		values := make(map[string]struct{})
		if name == labels.MetricName {
			for _, s := range series {
				values[s.metricName] = struct{}{}
			}
			return sortedLimited(values, limit), nil, nil
		}
		for _, batch := range idBatches(seriesIDs(series), q.queryable.cfg.IDChunkSize) {
			sql := fmt.Sprintf(
				"SELECT DISTINCT label_value FROM %s WHERE %s AND label_name = %s AND id IN (%s)",
				tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.LabelIndexTable),
				teamFilter(q.queryable.cfg),
				sqlString(name),
				joinUint64(batch),
			)
			if err := q.addStringRows(ctx, values, sql); err != nil {
				return nil, nil, err
			}
		}
		return sortedLimited(values, limit), nil, nil
	}

	values := make(map[string]struct{})
	var sql string
	if name == labels.MetricName {
		sql = fmt.Sprintf(
			"SELECT DISTINCT metric_name AS label_value FROM %s WHERE %s ORDER BY label_value%s",
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.SeriesTable),
			teamFilter(q.queryable.cfg),
			sqlLimit(limit),
		)
	} else {
		sql = fmt.Sprintf(
			"SELECT DISTINCT label_value FROM %s WHERE %s AND label_name = %s ORDER BY label_value%s",
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.LabelIndexTable),
			teamFilter(q.queryable.cfg),
			sqlString(name),
			sqlLimit(limit),
		)
	}
	if err := q.addStringRows(ctx, values, sql); err != nil {
		return nil, nil, err
	}
	return sortedLimited(values, limit), nil, nil
}

func (q *CHQuerier) addStringRows(ctx context.Context, values map[string]struct{}, sql string) error {
	return q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var value string
		if err := row.Scan(&value); err != nil {
			return err
		}
		values[value] = struct{}{}
		return nil
	})
}

type seriesMeta struct {
	id         uint64
	metricName string
	labels     labels.Labels
	labelMap   map[string]string
	samples    []samplePoint
	histograms []histogramPoint
	exemplars  []exemplarPoint
}

type seriesJSONRow struct {
	id       uint64
	labels   string
	labelSet labels.Labels
}

func (q *CHQuerier) selectSeries(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	if q.queryable.cfg.postHogSchemaLayout() {
		return q.selectPostHogSeries(ctx, mint, maxt, matchers...)
	}
	return q.selectSeriesMatching(ctx, mint, maxt, false, matchers...)
}

func (q *CHQuerier) selectActiveSeries(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	if q.queryable.cfg.postHogSchemaLayout() {
		return q.selectPostHogSeries(ctx, mint, maxt, matchers...)
	}
	return q.selectSeriesMatching(ctx, mint, maxt, true, matchers...)
}

func (q *CHQuerier) selectSeriesMatching(ctx context.Context, mint, maxt int64, activeOnly bool, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	where := []string{
		teamFilter(q.queryable.cfg),
	}
	if activeOnly {
		where = append(where, seriesTimeFilters(q.queryable.cfg, matchers, mint, maxt)...)
	}
	where = append(where, seriesPreFilters(q.queryable.cfg, matchers)...)

	sql := fmt.Sprintf(
		"SELECT id, any(metric_name) AS metric_name, any(labels_json) AS labels_json FROM (SELECT id, metric_name, labels_json FROM %s WHERE %s) GROUP BY id LIMIT %d",
		tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.SeriesTable),
		strings.Join(where, " AND "),
		q.queryable.cfg.MaxSeries,
	)

	series := make([]*seriesMeta, 0, 1024)
	err := q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var labelsJSON string
		if err := row.Scan(&id, &metricName, &labelsJSON); err != nil {
			return err
		}
		labelMap, ok, err := metricLabelMap(metricName, json.RawMessage(labelsJSON), matchers)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		series = append(series, &seriesMeta{
			id:         id,
			metricName: metricName,
			labelMap:   labelMap,
			labels:     labels.FromMap(labelMap),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(series) >= q.queryable.cfg.MaxSeries {
		return nil, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return series, nil
}

func (q *CHQuerier) selectActiveSeriesJSON(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]seriesJSONRow, bool, error) {
	if q.queryable.cfg.postHogSchemaLayout() {
		series, err := q.selectPostHogSeries(ctx, mint, maxt, matchers...)
		if err != nil {
			return nil, true, err
		}
		rows := make([]seriesJSONRow, 0, len(series))
		for _, s := range series {
			raw, err := json.Marshal(s.labelMap)
			if err != nil {
				return nil, true, err
			}
			rows = append(rows, seriesJSONRow{id: s.id, labels: string(raw), labelSet: s.labels})
		}
		return rows, true, nil
	}
	if !matchersPushdownSafe(matchers) {
		return nil, false, nil
	}
	where := []string{teamFilter(q.queryable.cfg)}
	where = append(where, seriesTimeFilters(q.queryable.cfg, matchers, mint, maxt)...)
	where = append(where, seriesPreFilters(q.queryable.cfg, matchers)...)
	labelExpr := "if(labels_json = '{}' OR labels_json = '', concat('{\"__name__\":', toJSONString(metric_name), '}'), concat('{\"__name__\":', toJSONString(metric_name), ',', substring(labels_json, 2)))"
	sql := fmt.Sprintf(
		"SELECT id, any(%s) AS labels_json FROM (SELECT id, metric_name, labels_json FROM %s WHERE %s) GROUP BY id LIMIT %d",
		labelExpr,
		tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.SeriesTable),
		strings.Join(where, " AND "),
		q.queryable.cfg.MaxSeries,
	)
	rows := make([]seriesJSONRow, 0, 1024)
	err := q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var out seriesJSONRow
		if err := row.Scan(&out.id, &out.labels); err != nil {
			return err
		}
		labelMap, err := parseLabelsJSON(json.RawMessage(out.labels))
		if err != nil {
			return err
		}
		out.labelSet = labels.FromMap(labelMap)
		rows = append(rows, out)
		return nil
	})
	if err != nil {
		return nil, true, err
	}
	if len(rows) >= q.queryable.cfg.MaxSeries {
		return nil, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return rows, true, nil
}

func seriesTimeFilters(cfg Config, matchers []*labels.Matcher, mint, maxt int64) []string {
	if metric := exactMetricName(matchers); metric != "" && cfg.SamplesTable != "" {
		return []string{"id IN (" + activeSeriesIDsSQL(cfg, matchers, mint, maxt) + ")"}
	}
	if cfg.ActivityTable == "" {
		return []string{
			fmt.Sprintf("max_time >= %s", chTimeMillis(mint)),
			fmt.Sprintf("min_time <= %s", chTimeMillis(maxt)),
		}
	}
	return []string{"id IN (" + activeSeriesIDsSQL(cfg, matchers, mint, maxt) + ")"}
}

func activeSeriesIDsSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64) string {
	if metric := exactMetricName(matchers); metric != "" && cfg.SamplesTable != "" {
		sql, _ := activeSeriesIDsForMetricsSQL(cfg, []string{metric}, mint, maxt)
		return sql
	}
	where := []string{
		teamFilter(cfg),
		fmt.Sprintf("bucket >= %s", chTimeMillis(mint)),
		fmt.Sprintf("bucket <= %s", chTimeMillis(maxt)),
	}
	where = append(where, metricNameConstraints(matchers)...)
	return fmt.Sprintf(
		"SELECT DISTINCT arrayJoin(ids) AS id FROM %s WHERE %s",
		tableName(cfg.CHDatabase, cfg.ActivityTable),
		strings.Join(where, " AND "),
	)
}

// activeSeriesIDsForMetricsSQL selects the series of the given metrics that
// carry a data point in [mint, maxt]. A metric can have orders of magnitude
// more series in the label index than are alive in any one query window, so
// intersecting with this keeps selector resolution proportional to the answer
// rather than to the metric's lifetime cardinality.
func activeSeriesIDsForMetricsSQL(cfg Config, metricNames []string, mint, maxt int64) (string, bool) {
	if cfg.SamplesTable == "" || len(metricNames) == 0 {
		return "", false
	}
	sources := []string{activeSeriesIDsFromTimedTableSQL(cfg, cfg.SamplesTable, metricNames, mint, maxt)}
	if cfg.HistogramsTable != "" {
		sources = append(sources, activeSeriesIDsFromTimedTableSQL(cfg, cfg.HistogramsTable, metricNames, mint, maxt))
	}
	return strings.Join(sources, " UNION DISTINCT "), true
}

func activeSeriesIDsFromTimedTableSQL(cfg Config, table string, metricNames []string, mint, maxt int64) string {
	where := []string{teamFilter(cfg)}
	if table == cfg.SamplesTable {
		where = append(where, sampleTimeFilters(cfg, mint, maxt)...)
	} else {
		where = append(where,
			"timestamp >= "+chTimeMillis(mint),
			"timestamp <= "+chTimeMillis(maxt),
		)
	}
	if condition := metricNamesCondition(metricNames); condition != "" {
		where = append(where, condition)
	}
	return fmt.Sprintf(
		"SELECT id FROM %s WHERE %s GROUP BY id",
		tableName(cfg.CHDatabase, table),
		strings.Join(where, " AND "),
	)
}

func (q *CHQuerier) selectLatestSeriesSamples(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, bool, error) {
	if !matchersPushdownSafe(matchers) {
		return nil, false, nil
	}
	selectedSeries, ok := selectedSeriesSQL(q.queryable.cfg, matchers, mint, maxt, []string{"id", "metric_name", "labels_json"})
	if !ok {
		return nil, false, nil
	}
	selectedSeries += fmt.Sprintf(" LIMIT %d", q.queryable.cfg.MaxSeries)
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s), latest AS (%s) SELECT id, metric_name, labels_json, toUnixTimestamp64Milli(latest.ts_col) AS ts, latest.value AS value FROM latest ANY INNER JOIN selected_series USING id ORDER BY id",
		selectedSeries,
		latestSamplesForSelectedSeriesSQL(q.queryable.cfg, matchers, mint, maxt),
	)

	series := make([]*seriesMeta, 0, 1024)
	err := q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var labelsJSON string
		var ts int64
		var value float64
		if err := row.Scan(&id, &metricName, &labelsJSON, &ts, &value); err != nil {
			return err
		}
		labelMap, ok, err := metricLabelMap(metricName, json.RawMessage(labelsJSON), matchers)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		series = append(series, &seriesMeta{
			id:         id,
			metricName: metricName,
			labelMap:   labelMap,
			labels:     labels.FromMap(labelMap),
			samples:    []samplePoint{{t: ts, v: value}},
		})
		return nil
	})
	if err != nil {
		return nil, true, err
	}
	if len(series) >= q.queryable.cfg.MaxSeries {
		return nil, true, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return series, true, nil
}

func metricOnlyMatchers(matchers []*labels.Matcher) bool {
	if len(matchers) == 0 {
		return false
	}
	for _, matcher := range matchers {
		if matcher.Name != labels.MetricName {
			return false
		}
	}
	return true
}

func (q *CHQuerier) labelNamesFromIndex(ctx context.Context, limit int, matchers ...*labels.Matcher) ([]string, bool, error) {
	if !matchersPushdownSafe(matchers) {
		return nil, false, nil
	}
	selectedSeries, ok := selectedSeriesSQL(q.queryable.cfg, matchers, q.mint, q.maxt, []string{"id"})
	if !ok {
		return nil, false, nil
	}
	sql := fmt.Sprintf(
		"WITH selected_series AS (%s) SELECT DISTINCT label_name FROM %s WHERE %s AND id IN (SELECT id FROM selected_series) ORDER BY label_name%s",
		selectedSeries,
		tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.LabelIndexTable),
		teamFilter(q.queryable.cfg),
		sqlLimit(limit),
	)
	names := map[string]struct{}{labels.MetricName: {}}
	if err := q.addStringRows(ctx, names, sql); err != nil {
		return nil, true, err
	}
	return sortedLimited(names, limit), true, nil
}

func (q *CHQuerier) labelValuesFromIndex(ctx context.Context, name string, limit int, matchers ...*labels.Matcher) ([]string, bool, error) {
	if !matchersPushdownSafe(matchers) {
		return nil, false, nil
	}
	selectedParts := []string{"id"}
	if name == labels.MetricName {
		selectedParts = []string{"metric_name"}
	}
	selectedSeries, ok := selectedSeriesSQL(q.queryable.cfg, matchers, q.mint, q.maxt, selectedParts)
	if !ok {
		return nil, false, nil
	}

	var sql string
	if name == labels.MetricName {
		sql = fmt.Sprintf(
			"WITH selected_series AS (%s) SELECT DISTINCT metric_name AS label_value FROM selected_series ORDER BY label_value%s",
			selectedSeries,
			sqlLimit(limit),
		)
	} else {
		sql = fmt.Sprintf(
			"WITH selected_series AS (%s) SELECT DISTINCT label_value FROM %s WHERE %s AND label_name = %s AND id IN (SELECT id FROM selected_series) ORDER BY label_value%s",
			selectedSeries,
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.LabelIndexTable),
			teamFilter(q.queryable.cfg),
			sqlString(name),
			sqlLimit(limit),
		)
	}
	values := make(map[string]struct{})
	if err := q.addStringRows(ctx, values, sql); err != nil {
		return nil, true, err
	}
	return sortedLimited(values, limit), true, nil
}

func seriesPreFilters(cfg Config, matchers []*labels.Matcher) []string {
	var filters []string
	metricConstraints := metricNameConstraints(matchers)
	for _, c := range metricConstraints {
		filters = append(filters, c)
	}

	for _, m := range matchers {
		if m.Name == labels.MetricName {
			continue
		}
		membership, condition, ok := labelIndexMembershipCondition(m)
		if !ok {
			continue
		}
		metricGuard := ""
		if metric := exactMetricName(matchers); metric != "" {
			metricGuard = " AND metric_name = " + sqlString(metric)
		}
		filters = append(filters, fmt.Sprintf(
			"id %s (SELECT id FROM %s WHERE %s AND label_name = %s%s AND %s)",
			membership,
			tableName(cfg.CHDatabase, cfg.LabelIndexTable),
			teamFilter(cfg),
			sqlString(m.Name),
			metricGuard,
			condition,
		))
	}
	return filters
}

func metricNameConstraints(matchers []*labels.Matcher) []string {
	var filters []string
	for _, m := range matchers {
		if m.Name != labels.MetricName || m.Matches("") {
			continue
		}
		if condition, ok := metricMatcherCondition(m); ok {
			filters = append(filters, condition)
		}
	}
	return filters
}

func metricMatcherCondition(m *labels.Matcher) (string, bool) {
	switch m.Type {
	case labels.MatchEqual:
		return "metric_name = " + sqlString(m.Value), true
	case labels.MatchNotEqual:
		return "metric_name != " + sqlString(m.Value), true
	case labels.MatchRegexp:
		if values := m.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return "metric_name IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := m.Prefix(); prefix != "" {
			return "startsWith(metric_name, " + sqlString(prefix) + ")", true
		}
		return "match(metric_name, " + sqlString(promRegexToCH(m.Value)) + ")", true
	case labels.MatchNotRegexp:
		if values := m.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return "metric_name NOT IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := m.Prefix(); prefix != "" {
			return "NOT startsWith(metric_name, " + sqlString(prefix) + ")", true
		}
		return "NOT match(metric_name, " + sqlString(promRegexToCH(m.Value)) + ")", true
	default:
		return "", false
	}
}

func exactMetricName(matchers []*labels.Matcher) string {
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual && m.Value != "" {
			return m.Value
		}
	}
	return ""
}

func labelIndexMembershipCondition(m *labels.Matcher) (string, string, bool) {
	if m.Name == labels.MetricName {
		return "", "", false
	}
	switch m.Type {
	case labels.MatchEqual, labels.MatchRegexp:
		return positiveLabelIndexMembershipCondition(m)
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		if !m.Matches("") {
			return "", "", false
		}
		condition, ok := negativeLabelIndexCondition(m)
		if !ok {
			return "", "", false
		}
		return "NOT IN", condition, true
	default:
		return "", "", false
	}
}

func positiveLabelIndexMembershipCondition(m *labels.Matcher) (string, string, bool) {
	if m.Matches("") {
		return "", "", false
	}
	switch m.Type {
	case labels.MatchEqual:
		return "IN", "label_value = " + sqlString(m.Value), true
	case labels.MatchRegexp:
		if values := m.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return "IN", "label_value IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := m.Prefix(); prefix != "" {
			return "IN", "startsWith(label_value, " + sqlString(prefix) + ")", true
		}
		return "IN", "match(label_value, " + sqlString(promRegexToCH(m.Value)) + ")", true
	default:
		return "", "", false
	}
}

func negativeLabelIndexCondition(m *labels.Matcher) (string, bool) {
	switch m.Type {
	case labels.MatchNotEqual:
		return "label_value = " + sqlString(m.Value), true
	case labels.MatchNotRegexp:
		if values := m.SetMatches(); len(values) > 0 {
			quoted := make([]string, 0, len(values))
			for _, value := range values {
				quoted = append(quoted, sqlString(value))
			}
			return "label_value IN (" + strings.Join(quoted, ",") + ")", true
		}
		if prefix := m.Prefix(); prefix != "" {
			return "startsWith(label_value, " + sqlString(prefix) + ")", true
		}
		return "match(label_value, " + sqlString(promRegexToCH(m.Value)) + ")", true
	default:
		return "", false
	}
}

func promRegexToCH(value string) string {
	// Prometheus regex matchers are fully anchored. RE2 in ClickHouse is not,
	// so preserve correctness for broad storage-side pruning.
	if strings.HasPrefix(value, "^(?:") && strings.HasSuffix(value, ")$") {
		return value
	}
	return "^(?:" + value + ")$"
}

func matchesAll(labelMap map[string]string, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(labelMap[m.Name]) {
			return false
		}
	}
	return true
}

func parseLabelsJSON(raw json.RawMessage) (map[string]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]string{}, nil
	}
	var labelsMap map[string]string
	if err := json.Unmarshal(raw, &labelsMap); err == nil {
		if labelsMap == nil {
			return map[string]string{}, nil
		}
		return labelsMap, nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return nil, err
	}
	if encoded == "" {
		return map[string]string{}, nil
	}
	if err := json.Unmarshal([]byte(encoded), &labelsMap); err != nil {
		return nil, err
	}
	if labelsMap == nil {
		return map[string]string{}, nil
	}
	return labelsMap, nil
}

func cloneLabelMap(labelMap map[string]string) map[string]string {
	cloned := make(map[string]string, len(labelMap)+1)
	for k, v := range labelMap {
		cloned[k] = v
	}
	return cloned
}

func metricLabelMap(metricName string, raw json.RawMessage, matchers []*labels.Matcher) (map[string]string, bool, error) {
	parsedLabels, err := parseLabelsJSON(raw)
	if err != nil {
		return nil, false, err
	}
	labelMap := cloneLabelMap(parsedLabels)
	labelMap[labels.MetricName] = metricName
	return labelMap, matchesAll(labelMap, matchers), nil
}

func (q *CHQuerier) loadSamples(ctx context.Context, series []*seriesMeta, mint, maxt int64, latestOnly bool, matchers []*labels.Matcher) error {
	if q.queryable.cfg.postHogSchemaLayout() {
		return q.loadPostHogSamples(ctx, series, mint, maxt, latestOnly, matchers)
	}
	byID, ids := seriesIndex(series)

	if len(series) > q.queryable.cfg.IDChunkSize {
		if sql, ok := samplesSQLFromMatchers(q.queryable.cfg, matchers, mint, maxt, latestOnly); ok {
			return q.queryable.client.QueryRows(ctx, sql, sampleRowHandler(byID))
		}
	}

	for _, batch := range idBatches(ids, q.queryable.cfg.IDChunkSize) {
		sql := samplesSQL(q.queryable.cfg, batch, mint, maxt, latestOnly, matchers)
		err := q.queryable.client.QueryRows(ctx, sql, sampleRowHandler(byID))
		if err != nil {
			return err
		}
	}
	return nil
}

func sampleRowHandler(byID map[uint64]*seriesMeta) func(clickHouseRow) error {
	return func(row clickHouseRow) error {
		var id uint64
		var ts int64
		var value float64
		if err := row.Scan(&id, &ts, &value); err != nil {
			return err
		}
		if s := byID[id]; s != nil {
			s.samples = append(s.samples, samplePoint{t: ts, v: value})
		}
		return nil
	}
}

func (q *CHQuerier) loadHistograms(ctx context.Context, series []*seriesMeta, mint, maxt int64, matchers []*labels.Matcher) error {
	if q.queryable.cfg.HistogramsTable == "" || len(series) == 0 {
		return nil
	}
	byID, ids := seriesIndex(series)

	if len(series) > q.queryable.cfg.IDChunkSize {
		if sql, ok := histogramsSQLFromMatchers(q.queryable.cfg, matchers, mint, maxt); ok {
			return q.queryable.client.QueryRows(ctx, sql, histogramRowHandler(byID))
		}
	}

	for _, batch := range idBatches(ids, q.queryable.cfg.IDChunkSize) {
		sql := histogramsSQL(q.queryable.cfg, batch, mint, maxt)
		if err := q.queryable.client.QueryRows(ctx, sql, histogramRowHandler(byID)); err != nil {
			return err
		}
	}
	return nil
}

func histogramRowHandler(byID map[uint64]*seriesMeta) func(clickHouseRow) error {
	return func(row clickHouseRow) error {
		var id uint64
		var ts int64
		var payload []byte
		if err := row.Scan(&id, &ts, &payload); err != nil {
			return err
		}
		var h prompb.Histogram
		if err := h.Unmarshal(payload); err != nil {
			return err
		}
		if s := byID[id]; s != nil {
			s.histograms = append(s.histograms, histogramPoint{t: ts, h: h})
		}
		return nil
	}
}

func histogramsSQLFromMatchers(cfg Config, matchers []*labels.Matcher, mint, maxt int64) (string, bool) {
	where := []string{
		teamFilter(cfg),
		fmt.Sprintf("timestamp >= %s", chTimeMillis(mint)),
		fmt.Sprintf("timestamp <= %s", chTimeMillis(maxt)),
	}
	where = append(where, metricNameConstraints(matchers)...)
	idFilters, ok := sampleIDFiltersFromMatchers(cfg, matchers, mint, maxt)
	if !ok {
		return "", false
	}
	where = append(where, idFilters...)
	return fmt.Sprintf(
		"SELECT id, toUnixTimestamp64Milli(timestamp) AS ts, argMax(histogram, version) AS histogram FROM %s WHERE %s GROUP BY id, timestamp ORDER BY id, timestamp",
		tableName(cfg.CHDatabase, cfg.HistogramsTable),
		strings.Join(where, " AND "),
	), true
}

func histogramsSQL(cfg Config, ids []uint64, mint, maxt int64) string {
	return fmt.Sprintf(
		"SELECT id, toUnixTimestamp64Milli(timestamp) AS ts, argMax(histogram, version) AS histogram FROM %s WHERE %s AND id IN (%s) AND timestamp >= %s AND timestamp <= %s GROUP BY id, timestamp ORDER BY id, timestamp",
		tableName(cfg.CHDatabase, cfg.HistogramsTable),
		teamFilter(cfg),
		joinUint64(ids),
		chTimeMillis(mint),
		chTimeMillis(maxt),
	)
}

func (q *CHQuerier) loadExemplars(ctx context.Context, series []*seriesMeta, mint, maxt int64) error {
	if q.queryable.cfg.ExemplarsTable == "" || len(series) == 0 {
		return nil
	}
	byID, ids := seriesIndex(series)
	for _, batch := range idBatches(ids, q.queryable.cfg.IDChunkSize) {
		sql := fmt.Sprintf(
			"SELECT id, toUnixTimestamp64Milli(timestamp) AS ts, any(value) AS value, any(labels_json) AS labels_json FROM %s WHERE %s AND id IN (%s) AND timestamp >= %s AND timestamp <= %s GROUP BY id, timestamp ORDER BY id, timestamp",
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.ExemplarsTable),
			teamFilter(q.queryable.cfg),
			joinUint64(batch),
			chTimeMillis(mint),
			chTimeMillis(maxt),
		)
		if err := q.queryable.client.QueryRows(ctx, sql, exemplarRowHandler(byID)); err != nil {
			return err
		}
	}
	return nil
}

func exemplarRowHandler(byID map[uint64]*seriesMeta) func(clickHouseRow) error {
	return func(row clickHouseRow) error {
		var id uint64
		var ts int64
		var value float64
		var labelsJSON string
		if err := row.Scan(&id, &ts, &value, &labelsJSON); err != nil {
			return err
		}
		parsedLabels, err := parseLabelsJSON(json.RawMessage(labelsJSON))
		if err != nil {
			return err
		}
		lbls := labels.FromMap(parsedLabels)
		if s := byID[id]; s != nil {
			s.exemplars = append(s.exemplars, exemplarPoint{
				t:      ts,
				value:  value,
				labels: lbls,
				pb: prompb.Exemplar{
					Labels:    labelsToPrompb(lbls),
					Value:     value,
					Timestamp: ts,
				},
			})
		}
		return nil
	}
}

func samplesSQLFromMatchers(cfg Config, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) (string, bool) {
	where := sampleBaseFilters(cfg, matchers, mint, maxt)
	idFilters, ok := sampleIDFiltersFromMatchers(cfg, matchers, mint, maxt)
	if !ok {
		return "", false
	}
	where = append(where, idFilters...)
	return sampleRowsSQL(cfg, strings.Join(where, " AND "), latestOnly), true
}

func sampleRowsSQL(cfg Config, where string, latestOnly bool) string {
	source := rawSamplesSourceSQL(cfg, where)
	if latestOnly {
		latest := fmt.Sprintf(
			"SELECT id, max(timestamp) AS ts_col, argMax(value, timestamp) AS value FROM (%s) GROUP BY id",
			source,
		)
		return fmt.Sprintf(
			"SELECT id, toUnixTimestamp64Milli(ts_col) AS ts, value FROM (%s) WHERE %s ORDER BY id",
			latest,
			nonStaleSampleSQL("value"),
		)
	}
	return fmt.Sprintf(
		"SELECT id, toUnixTimestamp64Milli(timestamp) AS ts, value FROM (%s) ORDER BY id, timestamp",
		source,
	)
}

func rawSamplesSourceSQL(cfg Config, where string) string {
	return fmt.Sprintf(
		"SELECT id, timestamp, value FROM %s WHERE %s",
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		where,
	)
}

func samplesForSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64) string {
	where := sampleBaseFilters(cfg, matchers, mint, maxt)
	where = append(where, sampleSelectedSeriesFilters(cfg)...)
	return rawSamplesSourceSQL(cfg, strings.Join(where, " AND "))
}

func sampleIDFiltersFromMatchers(cfg Config, matchers []*labels.Matcher, mint, maxt int64) ([]string, bool) {
	var filters []string
	for _, m := range matchers {
		if matcherIsNoop(m) {
			continue
		}
		if m.Name == labels.MetricName {
			continue
		}
		membership, condition, ok := labelIndexMembershipCondition(m)
		if !ok {
			return nil, false
		}
		source := fmt.Sprintf(
			"SELECT id FROM %s WHERE %s AND label_name = %s AND %s",
			tableName(cfg.CHDatabase, cfg.LabelIndexTable),
			teamFilter(cfg),
			sqlString(m.Name),
			condition,
		)
		filters = append(filters, sampleIDMembershipFilters(cfg, membership, source)...)
	}
	return filters, true
}

func samplesSQL(cfg Config, ids []uint64, mint, maxt int64, latestOnly bool, matchers []*labels.Matcher) string {
	where := sampleBaseFilters(cfg, matchers, mint, maxt)
	where = append(where, sampleExplicitIDFilters(cfg, ids)...)
	return sampleRowsSQL(cfg, strings.Join(where, " AND "), latestOnly)
}

func idBatches(ids []uint64, size int) [][]uint64 {
	if len(ids) == 0 {
		return nil
	}
	if size <= 0 {
		size = 5000
	}
	batches := make([][]uint64, 0, (len(ids)+size-1)/size)
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		batches = append(batches, ids[start:end])
	}
	return batches
}

func joinUint64(ids []uint64) string {
	if len(ids) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(len(ids) * 21)
	b.WriteString(strconv.FormatUint(ids[0], 10))
	for _, id := range ids[1:] {
		b.WriteByte(',')
		b.WriteString(strconv.FormatUint(id, 10))
	}
	return b.String()
}

func seriesIDs(series []*seriesMeta) []uint64 {
	ids := make([]uint64, len(series))
	for i, s := range series {
		ids[i] = s.id
	}
	return ids
}

func seriesIndex(series []*seriesMeta) (map[uint64]*seriesMeta, []uint64) {
	byID := make(map[uint64]*seriesMeta, len(series))
	ids := make([]uint64, 0, len(series))
	for _, s := range series {
		byID[s.id] = s
		ids = append(ids, s.id)
	}
	return byID, ids
}

func sortedLimited(values map[string]struct{}, limit int) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	if limit > 0 && len(result) > limit {
		return result[:limit]
	}
	return result
}

func sqlLimit(limit int) string {
	if limit <= 0 {
		return ""
	}
	return " LIMIT " + strconv.Itoa(limit)
}

type seriesSet struct {
	series []storage.Series
	idx    int
	err    error
}

func (s *seriesSet) Next() bool {
	if s.err != nil {
		return false
	}
	s.idx++
	return s.idx < len(s.series)
}

func (s *seriesSet) At() storage.Series {
	if s.idx < 0 || s.idx >= len(s.series) {
		return nil
	}
	return s.series[s.idx]
}

func (s *seriesSet) Err() error {
	return s.err
}

func (s *seriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *seriesMeta) Labels() labels.Labels {
	return s.labels
}

func (s *seriesMeta) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	points := make([]seriesPoint, 0, len(s.samples)+len(s.histograms))
	for _, sample := range s.samples {
		points = append(points, seriesPoint{t: sample.t, f: sample.v, typ: chunkenc.ValFloat})
	}
	for _, h := range s.histograms {
		valueType := chunkenc.ValHistogram
		if h.h.IsFloatHistogram() {
			valueType = chunkenc.ValFloatHistogram
		}
		points = append(points, seriesPoint{t: h.t, h: h.h, typ: valueType})
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].t == points[j].t {
			return points[i].typ < points[j].typ
		}
		return points[i].t < points[j].t
	})
	return &sampleIterator{points: points, idx: -1}
}

type samplePoint struct {
	t int64
	v float64
}

type histogramPoint struct {
	t int64
	h prompb.Histogram
}

type exemplarPoint struct {
	t      int64
	value  float64
	labels labels.Labels
	pb     prompb.Exemplar
}

type seriesPoint struct {
	t   int64
	f   float64
	h   prompb.Histogram
	typ chunkenc.ValueType
}

type sampleIterator struct {
	points []seriesPoint
	idx    int
}

func (it *sampleIterator) Next() chunkenc.ValueType {
	if it.idx >= len(it.points) {
		return chunkenc.ValNone
	}
	it.idx++
	if it.idx >= len(it.points) {
		return chunkenc.ValNone
	}
	return it.points[it.idx].typ
}

func (it *sampleIterator) Seek(t int64) chunkenc.ValueType {
	if it.idx >= 0 && it.idx < len(it.points) && it.points[it.idx].t >= t {
		return it.points[it.idx].typ
	}
	start := it.idx + 1
	if start < 0 {
		start = 0
	}
	pos := sort.Search(len(it.points)-start, func(i int) bool {
		return it.points[start+i].t >= t
	})
	it.idx = start + pos
	if it.idx >= len(it.points) {
		return chunkenc.ValNone
	}
	return it.points[it.idx].typ
}

func (it *sampleIterator) At() (int64, float64) {
	point, ok := it.current()
	if !ok {
		return math.MinInt64, math.NaN()
	}
	return point.t, point.f
}

func (it *sampleIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	point, ok := it.current()
	if !ok {
		return math.MinInt64, nil
	}
	if point.typ != chunkenc.ValHistogram {
		return point.t, nil
	}
	return point.t, point.h.ToIntHistogram()
}

func (it *sampleIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	point, ok := it.current()
	if !ok {
		return math.MinInt64, nil
	}
	if point.typ != chunkenc.ValFloatHistogram {
		return point.t, nil
	}
	return point.t, point.h.ToFloatHistogram()
}

func (it *sampleIterator) AtT() int64 {
	point, ok := it.current()
	if !ok {
		return math.MinInt64
	}
	return point.t
}

func (it *sampleIterator) AtST() int64 {
	return 0
}

func (it *sampleIterator) Err() error {
	return nil
}

func (it *sampleIterator) current() (seriesPoint, bool) {
	if it.idx < 0 || it.idx >= len(it.points) {
		return seriesPoint{}, false
	}
	return it.points[it.idx], true
}
