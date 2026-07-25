package snuffle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func (s *Server) handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	status := "error"
	var compressedBytes int
	var decodedBytes int
	var batch remoteWriteBatch
	defer func() {
		s.metrics.observeRemoteWrite(status, time.Since(started), compressedBytes, decodedBytes, batch)
	}()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeHTTPError(w, http.StatusMethodNotAllowed, "bad_method", errors.New("method not allowed"))
		return
	}

	decoded, compressedBytes, err := readSnappyBody(r, "remote write")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	decodedBytes = len(decoded)

	ok := false
	if !s.cfg.SampleAttributes && !s.cfg.postHogSchemaLayout() {
		batch, ok, err = buildRemoteWriteBatchFromProto(decoded, s.cfg.RemoteWriteInterval, s.cfg.TeamID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "bad_data", err)
			return
		}
	}
	if !ok {
		var req prompb.WriteRequest
		if err := req.Unmarshal(decoded); err != nil {
			writeAPIError(w, http.StatusBadRequest, "bad_data", fmt.Errorf("decode remote write protobuf: %w", err))
			return
		}
		batch, err = buildRemoteWriteBatch(&req, s.cfg.RemoteWriteInterval, s.cfg.TeamID, s.cfg.SampleAttributes || s.cfg.postHogSchemaLayout())
	}
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.CHTimeout)
	defer cancel()
	if err := s.insertRemoteWriteBatch(ctx, batch); err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "execution", err)
		return
	}
	runtime.KeepAlive(decoded)

	status = "ok"
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRemoteRead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	status := "error"
	var compressedBytes int
	var decodedBytes int
	var responseBytes int
	var queryCount int
	var returnedRows remoteReadRows
	defer func() {
		s.metrics.observeRemoteRead(status, time.Since(started), compressedBytes, decodedBytes, responseBytes, queryCount, returnedRows)
	}()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeHTTPError(w, http.StatusMethodNotAllowed, "bad_method", errors.New("method not allowed"))
		return
	}

	var req prompb.ReadRequest
	var err error
	decodedBytes, compressedBytes, err = readSnappyProto(r, &req, "remote read")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err)
		return
	}
	queryCount = len(req.GetQueries())
	if !remoteReadAcceptsSamples(&req) {
		writeAPIError(w, http.StatusBadRequest, "bad_data", errors.New("remote read streamed chunk responses are not supported"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	resp, err := s.remoteReadSamples(ctx, &req)
	if err != nil {
		writeAPIError(w, http.StatusUnprocessableEntity, "execution", err)
		return
	}
	payload, err := resp.Marshal()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "execution", err)
		return
	}
	returnedRows = countRemoteReadRows(resp)
	encoded := snappy.Encode(nil, payload)
	responseBytes = len(encoded)

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	status = "ok"
	_, _ = w.Write(encoded)
}

func readSnappyProto(r *http.Request, dst interface{ Unmarshal([]byte) error }, name string) (int, int, error) {
	decoded, compressedBytes, err := readSnappyBody(r, name)
	if err != nil {
		return 0, compressedBytes, err
	}
	if err := dst.Unmarshal(decoded); err != nil {
		return len(decoded), compressedBytes, fmt.Errorf("decode %s protobuf: %w", name, err)
	}
	return len(decoded), compressedBytes, nil
}

func readSnappyBody(r *http.Request, name string) ([]byte, int, error) {
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, 0, err
	}
	decoded, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, len(compressed), fmt.Errorf("decode snappy %s request: %w", name, err)
	}
	return decoded, len(compressed), nil
}

type remoteWriteBatch struct {
	sampleRows     []remoteWriteSampleRow
	sampleColumns  remoteWriteSampleColumns
	histogramRows  []remoteWriteHistogramRow
	exemplarRows   []remoteWriteExemplarRow
	metadataRows   []remoteWriteMetadataRow
	seriesRecords  []remoteWriteSeriesRow
	fastProto      []byte
	seriesCount    int
	sampleCount    int
	histogramCount int
	exemplarCount  int
	metadataCount  int
}

type remoteWriteSeriesRow struct {
	TeamID     uint64
	ID         uint64
	MetricName string
	LabelsJSON string
	Labels     []remoteWriteLabel
	MinMS      int64
	MaxMS      int64
}

type remoteWriteLabel struct {
	Name  string
	Value string
}

type remoteWriteSampleRow struct {
	TeamID      uint64
	MetricName  string
	TimestampMS int64
	ID          uint64
	Value       float64
	ServiceName string
	Attributes  map[string]string
}

type remoteWriteSampleColumns struct {
	TeamIDs      []uint64
	MetricNames  []string
	Timestamps   []int64
	IDs          []uint64
	Values       []float64
	ServiceNames []string
	Attributes   []map[string]string
}

func (c remoteWriteSampleColumns) count() int {
	return len(c.IDs)
}

type remoteWriteHistogramRow struct {
	TeamID      uint64
	MetricName  string
	TimestampMS int64
	ID          uint64
	Histogram   []byte
	Version     uint64
}

type remoteWriteExemplarRow struct {
	TeamID      uint64
	TimestampMS int64
	ID          uint64
	Value       float64
	LabelsJSON  string
}

type remoteWriteMetadataRow struct {
	TeamID           uint64
	MetricFamilyName string
	Type             string
	Unit             string
	Help             string
	UpdatedAtMS      int64
}

func buildRemoteWriteBatch(req *prompb.WriteRequest, sampleInterval time.Duration, teamID uint64, includeSampleAttributes bool) (remoteWriteBatch, error) {
	timeseriesCount := len(req.GetTimeseries())
	sampleIntervalMS := sampleInterval.Milliseconds()
	sampleCountHint := 0
	histogramCountHint := 0
	exemplarCountHint := 0
	for _, ts := range req.GetTimeseries() {
		sampleCountHint += len(ts.GetSamples())
		histogramCountHint += len(ts.GetHistograms())
		exemplarCountHint += len(ts.GetExemplars())
	}
	batch := remoteWriteBatch{
		sampleRows:    make([]remoteWriteSampleRow, 0, sampleCountHint),
		histogramRows: make([]remoteWriteHistogramRow, 0, histogramCountHint),
		exemplarRows:  make([]remoteWriteExemplarRow, 0, exemplarCountHint),
		metadataRows:  make([]remoteWriteMetadataRow, 0, len(req.GetMetadata())),
		seriesRecords: make([]remoteWriteSeriesRow, 0, timeseriesCount),
	}
	seriesIndexByID := make(map[uint64]int, timeseriesCount)
	updatedAtMS := time.Now().UnixMilli()

	for _, metadata := range req.GetMetadata() {
		if metadata.GetMetricFamilyName() == "" {
			continue
		}
		batch.metadataRows = append(batch.metadataRows, remoteWriteMetadataRow{
			TeamID:           teamID,
			MetricFamilyName: metadata.GetMetricFamilyName(),
			Type:             remoteMetadataType(metadata.GetType()),
			Unit:             metadata.GetUnit(),
			Help:             metadata.GetHelp(),
			UpdatedAtMS:      updatedAtMS,
		})
	}

	for _, ts := range req.GetTimeseries() {
		if len(ts.GetSamples()) == 0 && len(ts.GetHistograms()) == 0 && len(ts.GetExemplars()) == 0 {
			continue
		}

		rowLabels := remoteWriteLabelsFromPrompb(ts.GetLabels())
		id, metricName, err := remoteWriteSeriesIdentityLabels(rowLabels)
		if err != nil {
			return remoteWriteBatch{}, err
		}
		var sampleAttributes map[string]string
		var serviceName string
		if includeSampleAttributes {
			sampleAttributes = sampleAttributesFromRemoteWriteLabels(rowLabels)
			serviceName = serviceNameFromRemoteWriteLabels(rowLabels)
		}

		var minMS int64
		var maxMS int64
		var haveTime bool
		observeTime := func(timestamp int64) {
			if !haveTime {
				minMS = timestamp
				maxMS = timestamp
				haveTime = true
				return
			}
			if timestamp < minMS {
				minMS = timestamp
			}
			if timestamp > maxMS {
				maxMS = timestamp
			}
		}

		for _, sample := range ts.GetSamples() {
			if isRemoteWriteSampleDroppable(sample.Value) {
				continue
			}
			bucketMS := bucketTimestampForStepMS(sample.Timestamp, sampleIntervalMS)
			observeTime(bucketMS)
			if len(batch.sampleRows) > 0 {
				last := &batch.sampleRows[len(batch.sampleRows)-1]
				if last.ID == id && last.TimestampMS == bucketMS {
					last.Value = sample.Value
					continue
				}
			}
			batch.sampleRows = append(batch.sampleRows, remoteWriteSampleRow{
				TeamID:      teamID,
				MetricName:  metricName,
				TimestampMS: bucketMS,
				ID:          id,
				Value:       sample.Value,
				ServiceName: serviceName,
				Attributes:  sampleAttributes,
			})
		}
		for _, histogram := range ts.GetHistograms() {
			originalTimestamp := histogram.Timestamp
			bucketMS := bucketTimestampForStepMS(originalTimestamp, sampleIntervalMS)
			observeTime(bucketMS)
			histogram.Timestamp = bucketMS
			payload, err := histogram.Marshal()
			if err != nil {
				return remoteWriteBatch{}, err
			}
			batch.histogramRows = append(batch.histogramRows, remoteWriteHistogramRow{
				TeamID:      teamID,
				MetricName:  metricName,
				TimestampMS: bucketMS,
				ID:          id,
				Histogram:   payload,
				Version:     remoteWriteVersion(originalTimestamp),
			})
		}
		for _, exemplar := range ts.GetExemplars() {
			if math.IsNaN(exemplar.Value) {
				continue
			}
			observeTime(exemplar.Timestamp)
			exemplarLabels, err := remoteWriteAuxLabelMap(exemplar.GetLabels())
			if err != nil {
				return remoteWriteBatch{}, err
			}
			labelsJSON, err := json.Marshal(exemplarLabels)
			if err != nil {
				return remoteWriteBatch{}, err
			}
			batch.exemplarRows = append(batch.exemplarRows, remoteWriteExemplarRow{
				TeamID:      teamID,
				TimestampMS: exemplar.Timestamp,
				ID:          id,
				Value:       exemplar.Value,
				LabelsJSON:  string(labelsJSON),
			})
		}
		if !haveTime {
			continue
		}
		addRemoteWriteSeriesRecord(&batch, seriesIndexByID, remoteWriteSeriesRow{
			TeamID:     teamID,
			ID:         id,
			MetricName: metricName,
			Labels:     rowLabels,
			MinMS:      minMS,
			MaxMS:      maxMS,
		})
	}
	batch.seriesCount = len(batch.seriesRecords)
	batch.sampleCount = len(batch.sampleRows)
	batch.histogramCount = len(batch.histogramRows)
	batch.exemplarCount = len(batch.exemplarRows)
	batch.metadataCount = len(batch.metadataRows)
	return batch, nil
}

func addRemoteWriteSeriesRecord(batch *remoteWriteBatch, indexByID map[uint64]int, row remoteWriteSeriesRow) {
	if index, ok := indexByID[row.ID]; ok {
		existing := &batch.seriesRecords[index]
		if row.MinMS < existing.MinMS {
			existing.MinMS = row.MinMS
		}
		if row.MaxMS > existing.MaxMS {
			existing.MaxMS = row.MaxMS
		}
		return
	}
	indexByID[row.ID] = len(batch.seriesRecords)
	batch.seriesRecords = append(batch.seriesRecords, row)
}

func remoteMetadataType(input prompb.MetricMetadata_MetricType) string {
	switch input {
	case prompb.MetricMetadata_COUNTER:
		return "counter"
	case prompb.MetricMetadata_GAUGE:
		return "gauge"
	case prompb.MetricMetadata_HISTOGRAM:
		return "histogram"
	case prompb.MetricMetadata_GAUGEHISTOGRAM:
		return "gaugehistogram"
	case prompb.MetricMetadata_SUMMARY:
		return "summary"
	case prompb.MetricMetadata_INFO:
		return "info"
	case prompb.MetricMetadata_STATESET:
		return "stateset"
	default:
		return "unknown"
	}
}

func stableSeriesID(lbls labels.Labels) uint64 {
	var hash xxhash.Digest
	hash.Reset()
	var length [8]byte
	lbls.Range(func(label labels.Label) {
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Name)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Name)
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Value)
	})
	return hash.Sum64()
}

func remoteWriteSeriesIdentity(input []prompb.Label) (uint64, string, error) {
	return remoteWriteSeriesIdentityLabels(remoteWriteLabelsFromPrompb(input))
}

func remoteWriteLabelsFromPrompb(input []prompb.Label) []remoteWriteLabel {
	if len(input) == 0 {
		return nil
	}
	output := make([]remoteWriteLabel, len(input))
	for i, label := range input {
		output[i] = remoteWriteLabel{Name: label.Name, Value: label.Value}
	}
	return output
}

func remoteWriteSeriesIdentityLabels(input []remoteWriteLabel) (uint64, string, error) {
	if len(input) == 0 {
		return 0, "", errors.New("remote write time series has no labels")
	}
	lbls := input
	if !remoteWriteLabelsSorted(input) {
		sortRemoteWriteLabelsByName(lbls)
	}

	var length [8]byte
	var stack [512]byte
	encoded := stack[:0]
	useStackHash := true
	var metricName string
	var previous string
	for i, label := range lbls {
		if label.Name == "" {
			return 0, "", errors.New("remote write label name must not be empty")
		}
		if i > 0 && label.Name == previous {
			return 0, "", fmt.Errorf("duplicate remote write label %q", label.Name)
		}
		previous = label.Name
		if label.Name == labels.MetricName {
			metricName = label.Value
		}
		if useStackHash {
			needed := 16 + len(label.Name) + len(label.Value)
			if len(encoded)+needed <= cap(encoded) {
				binary.LittleEndian.PutUint64(length[:], uint64(len(label.Name)))
				encoded = append(encoded, length[:]...)
				encoded = append(encoded, label.Name...)
				binary.LittleEndian.PutUint64(length[:], uint64(len(label.Value)))
				encoded = append(encoded, length[:]...)
				encoded = append(encoded, label.Value...)
			} else {
				useStackHash = false
			}
		}
	}
	if metricName == "" {
		return 0, "", errors.New("remote write time series is missing __name__")
	}
	if useStackHash {
		return xxhash.Sum64(encoded), metricName, nil
	}
	return remoteWriteSeriesDigestHash(lbls), metricName, nil
}

func remoteWriteSeriesDigestHash(input []remoteWriteLabel) uint64 {
	var hash xxhash.Digest
	hash.Reset()
	var length [8]byte
	for _, label := range input {
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Name)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Name)
		binary.LittleEndian.PutUint64(length[:], uint64(len(label.Value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.WriteString(label.Value)
	}
	return hash.Sum64()
}

func remoteWriteLabelsSorted(input []remoteWriteLabel) bool {
	for i := 1; i < len(input); i++ {
		if input[i-1].Name > input[i].Name {
			return false
		}
	}
	return true
}

func sortRemoteWriteLabelsByName(input []remoteWriteLabel) {
	for i := 1; i < len(input); i++ {
		label := input[i]
		j := i - 1
		for ; j >= 0 && input[j].Name > label.Name; j-- {
			input[j+1] = input[j]
		}
		input[j+1] = label
	}
}

func labelsJSONFromRemoteWriteLabels(input []remoteWriteLabel) (string, error) {
	if len(input) == 0 {
		return "{}", nil
	}
	buf := make([]byte, 0, len(input)*32)
	buf = append(buf, '{')
	first := true
	for _, label := range input {
		if label.Name == labels.MetricName {
			continue
		}
		if label.Name == "" {
			return "", errors.New("remote write label name must not be empty")
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = strconv.AppendQuote(buf, label.Name)
		buf = append(buf, ':')
		buf = strconv.AppendQuote(buf, label.Value)
	}
	buf = append(buf, '}')
	return string(buf), nil
}

func sampleAttributesFromRemoteWriteLabels(input []remoteWriteLabel) map[string]string {
	attributes := make(map[string]string, len(input))
	for _, label := range input {
		if label.Name == labels.MetricName || label.Name == "" {
			continue
		}
		attributes[label.Name+"__str"] = label.Value
	}
	return attributes
}

func serviceNameFromRemoteWriteLabels(input []remoteWriteLabel) string {
	for _, label := range input {
		if label.Name == "service.name" || label.Name == "service_name" {
			return label.Value
		}
	}
	return ""
}

func bucketTimestampMS(timestamp int64, interval time.Duration) int64 {
	return bucketTimestampForStepMS(timestamp, interval.Milliseconds())
}

func bucketTimestampForStepMS(timestamp, step int64) int64 {
	if step <= 0 {
		return timestamp
	}
	remainder := timestamp % step
	if remainder < 0 {
		remainder += step
	}
	return timestamp - remainder
}

func remoteWriteVersion(timestamp int64) uint64 {
	if timestamp < 0 {
		return 0
	}
	return uint64(timestamp)
}

func remoteWriteAuxLabelMap(input []prompb.Label) (map[string]string, error) {
	return prompbLabelMap(input, "remote write auxiliary label")
}

func prompbLabelMap(input []prompb.Label, kind string) (map[string]string, error) {
	labelMap := make(map[string]string, len(input))
	for _, label := range input {
		if label.Name == "" {
			return nil, errors.New(kind + " name must not be empty")
		}
		if _, exists := labelMap[label.Name]; exists {
			return nil, fmt.Errorf("duplicate %s %q", kind, label.Name)
		}
		labelMap[label.Name] = label.Value
	}
	return labelMap, nil
}

func (s *Server) insertRemoteWriteBatch(ctx context.Context, batch remoteWriteBatch) error {
	if batch.seriesCount == 0 && batch.sampleCount == 0 && batch.histogramCount == 0 && batch.exemplarCount == 0 && batch.metadataCount == 0 {
		return nil
	}
	batchSummary := remoteWriteBatchSummary(batch)
	if s.cfg.postHogSchemaLayout() {
		batch.seriesRecords = nil
		batch.seriesCount = 0
		batchSummary = remoteWriteBatchSummary(batch)
	} else if batch.seriesCount > 0 {
		inserted, err := s.insertMissingRemoteSeriesRows(ctx, batch, batchSummary)
		if err != nil {
			return err
		}
		batch.seriesRecords = nil
		batch.seriesCount = inserted
		batchSummary = remoteWriteBatchSummary(batch)
	}
	started := time.Now()
	if err := s.insertRemoteMetadataRows(ctx, batch.metadataRows); err != nil {
		return remoteWritePhaseError("insert metadata", s.cfg.MetricsTable, len(batch.metadataRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	started = time.Now()
	if err := s.insertRemoteSampleRows(ctx, batch); err != nil {
		return remoteWritePhaseError("insert samples", s.cfg.SamplesTable, batch.sampleCount, s.cfg.CHTimeout, batchSummary, started, err)
	}
	started = time.Now()
	if err := s.insertRemoteHistogramRows(ctx, batch.histogramRows); err != nil {
		return remoteWritePhaseError("insert histograms", s.cfg.HistogramsTable, len(batch.histogramRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	started = time.Now()
	if err := s.insertRemoteExemplarRows(ctx, batch.exemplarRows); err != nil {
		return remoteWritePhaseError("insert exemplars", s.cfg.ExemplarsTable, len(batch.exemplarRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	return nil
}

func (s *Server) insertMissingRemoteSeriesRows(ctx context.Context, batch remoteWriteBatch, batchSummary string) (int, error) {
	rows := batch.seriesRecords
	started := time.Now()
	newRows, err := s.filterNewSeriesRows(ctx, rows)
	if err != nil {
		return 0, remoteWritePhaseError("lookup existing series", s.cfg.SeriesTable, len(rows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	if len(newRows) == 0 {
		return 0, nil
	}

	if s.seriesMu != nil {
		s.seriesMu.Lock()
		defer s.seriesMu.Unlock()
	}

	started = time.Now()
	newRows, err = s.filterNewSeriesRows(ctx, newRows)
	if err != nil {
		return 0, remoteWritePhaseError("recheck existing series", s.cfg.SeriesTable, len(newRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	if len(newRows) == 0 {
		return 0, nil
	}

	started = time.Now()
	if len(batch.fastProto) > 0 {
		err = populateSeriesLabelsJSONFromProto(batch.fastProto, newRows)
	} else {
		err = populateSeriesLabelsJSON(newRows)
	}
	if err != nil {
		return 0, remoteWritePhaseError("encode new series labels", s.cfg.SeriesTable, len(newRows), s.cfg.CHTimeout, batchSummary, started, err)
	}

	started = time.Now()
	if err := s.insertRemoteSeriesRows(ctx, newRows); err != nil {
		return 0, remoteWritePhaseError("insert series", s.cfg.SeriesTable, len(newRows), s.cfg.CHTimeout, batchSummary, started, err)
	}
	return len(newRows), nil
}

func remoteWriteBatchSummary(batch remoteWriteBatch) string {
	return fmt.Sprintf(
		"series=%d samples=%d histograms=%d exemplars=%d metadata=%d",
		batch.seriesCount,
		batch.sampleCount,
		batch.histogramCount,
		batch.exemplarCount,
		batch.metadataCount,
	)
}

func remoteWritePhaseError(phase, table string, rows int, timeout time.Duration, batchSummary string, started time.Time, err error) error {
	if err == nil {
		return nil
	}
	elapsed := time.Since(started).Round(time.Millisecond)
	details := fmt.Sprintf("after %s (clickhouse_timeout=%s table=%s rows=%d batch=%s)", elapsed, timeout, table, rows, batchSummary)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("remote write %s timed out %s: %w", phase, details, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("remote write %s canceled %s: %w", phase, details, err)
	default:
		return fmt.Errorf("remote write %s failed %s: %w", phase, details, err)
	}
}

func populateSeriesLabelsJSON(rows []remoteWriteSeriesRow) error {
	for i := range rows {
		if rows[i].LabelsJSON != "" {
			continue
		}
		labelsJSON, err := labelsJSONFromRemoteWriteLabels(rows[i].Labels)
		if err != nil {
			return err
		}
		rows[i].LabelsJSON = labelsJSON
	}
	return nil
}

func (s *Server) filterNewSeriesRows(ctx context.Context, rows []remoteWriteSeriesRow) ([]remoteWriteSeriesRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	ids := make([]uint64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	known := make(map[uint64]struct{}, len(rows))
	sql := knownSeriesIDsSQL(s.cfg, "remote_write_series_ids")
	if err := s.client.QueryRowsWithExternalUInt64s(ctx, "remote_write_series_ids", "id", ids, sql, func(row clickHouseRow) error {
		var id uint64
		if err := row.Scan(&id); err != nil {
			return err
		}
		known[id] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(known) == 0 {
		return rows, nil
	}
	if len(known) == len(rows) {
		return nil, nil
	}
	newRows := rows[:0]
	for _, row := range rows {
		if _, ok := known[row.ID]; ok {
			continue
		}
		newRows = append(newRows, row)
	}
	return newRows, nil
}

// knownSeriesIDsSQL reports which of this batch's series already exist.
//
// Asking the inverse -- which batch ids are NOT IN (SELECT id FROM series) --
// makes ClickHouse materialise every series id the tenant has ever written to
// answer a question about a few thousand of them. Against 51M series that read
// 394MB and took 2.9s on every remote-write request; driving the lookup from
// the batch's ids instead reads ~3MB and takes ~12ms. The caller inverts the
// result, which costs one map lookup per row.
func knownSeriesIDsSQL(cfg Config, lookupTable string) string {
	return fmt.Sprintf(
		"SELECT id FROM %s WHERE team_id = %d AND id IN (SELECT id FROM %s)",
		tableName(cfg.CHDatabase, cfg.SeriesTable),
		cfg.TeamID,
		quoteIdent(lookupTable),
	)
}

func (s *Server) insertRemoteSeriesRows(ctx context.Context, rows []remoteWriteSeriesRow) error {
	if len(rows) == 0 {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, id, metric_name, labels_json, min_time, max_time)", tableName(s.cfg.CHDatabase, s.cfg.SeriesTable))
	return s.client.InsertColumnsSync(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		ids := make([]uint64, len(rows))
		metricNames := make([]string, len(rows))
		labelsJSON := make([]string, len(rows))
		minTimes := make([]int64, len(rows))
		maxTimes := make([]int64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			ids[i] = row.ID
			metricNames[i] = row.MetricName
			labelsJSON[i] = row.LabelsJSON
			minTimes[i] = row.MinMS
			maxTimes[i] = row.MaxMS
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(metricNames); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(labelsJSON); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(minTimes); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(maxTimes); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteSampleRows(ctx context.Context, batchRows remoteWriteBatch) error {
	if batchRows.sampleCount == 0 {
		return nil
	}
	if s.cfg.postHogSchemaLayout() {
		return s.insertPostHogRemoteSampleRows(ctx, batchRows)
	}
	insertAttributes := s.cfg.SampleAttributes
	columnsSQL := "team_id, metric_name, timestamp, id, value"
	if insertAttributes {
		columnsSQL += ", attributes_map_str"
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s)", tableName(s.cfg.CHDatabase, s.cfg.SamplesTable), columnsSQL)
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		columns := batchRows.sampleColumns
		if columns.count() == 0 {
			columns = remoteWriteSampleColumns{
				TeamIDs:     make([]uint64, len(batchRows.sampleRows)),
				MetricNames: make([]string, len(batchRows.sampleRows)),
				Timestamps:  make([]int64, len(batchRows.sampleRows)),
				IDs:         make([]uint64, len(batchRows.sampleRows)),
				Values:      make([]float64, len(batchRows.sampleRows)),
			}
			if insertAttributes {
				columns.Attributes = make([]map[string]string, len(batchRows.sampleRows))
			}
			for i, row := range batchRows.sampleRows {
				columns.TeamIDs[i] = row.TeamID
				columns.MetricNames[i] = row.MetricName
				columns.Timestamps[i] = row.TimestampMS
				columns.IDs[i] = row.ID
				columns.Values[i] = row.Value
				if insertAttributes {
					columns.Attributes[i] = row.Attributes
				}
			}
		}
		return appendRemoteSampleColumns(batch, columns)
	})
}

func (s *Server) insertPostHogRemoteSampleRows(ctx context.Context, batchRows remoteWriteBatch) error {
	sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_name, timestamp, observed_timestamp, value, service_name, resource_attributes, attributes_map_str)", tableName(s.cfg.CHDatabase, s.cfg.SamplesTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]int32, len(batchRows.sampleRows))
		metricNames := make([]string, len(batchRows.sampleRows))
		timestamps := make([]int64, len(batchRows.sampleRows))
		values := make([]float64, len(batchRows.sampleRows))
		serviceNames := make([]string, len(batchRows.sampleRows))
		resourceAttributes := make([]map[string]string, len(batchRows.sampleRows))
		attributes := make([]map[string]string, len(batchRows.sampleRows))
		for i, row := range batchRows.sampleRows {
			teamIDs[i] = int32(row.TeamID)
			metricNames[i] = row.MetricName
			timestamps[i] = row.TimestampMS
			values[i] = row.Value
			serviceNames[i] = row.ServiceName
			resourceAttributes[i] = map[string]string{}
			attributes[i] = row.Attributes
			if attributes[i] == nil {
				attributes[i] = map[string]string{}
			}
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(metricNames); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(values); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(serviceNames); err != nil {
			return 0, err
		}
		if err := batch.Column(6).Append(resourceAttributes); err != nil {
			return 0, err
		}
		if err := batch.Column(7).Append(attributes); err != nil {
			return 0, err
		}
		return len(batchRows.sampleRows), nil
	})
}

func appendRemoteSampleColumns(batch clickHouseBatch, columns remoteWriteSampleColumns) (int, error) {
	if err := batch.Column(0).Append(columns.TeamIDs); err != nil {
		return 0, err
	}
	if err := batch.Column(1).Append(columns.MetricNames); err != nil {
		return 0, err
	}
	if err := batch.Column(2).Append(columns.Timestamps); err != nil {
		return 0, err
	}
	if err := batch.Column(3).Append(columns.IDs); err != nil {
		return 0, err
	}
	if err := batch.Column(4).Append(columns.Values); err != nil {
		return 0, err
	}
	nextColumn := 5
	if len(columns.ServiceNames) > 0 {
		if err := batch.Column(nextColumn).Append(columns.ServiceNames); err != nil {
			return 0, err
		}
		nextColumn++
	}
	if len(columns.Attributes) > 0 {
		if err := batch.Column(nextColumn).Append(columns.Attributes); err != nil {
			return 0, err
		}
	}
	return columns.count(), nil
}

func (s *Server) insertRemoteHistogramRows(ctx context.Context, rows []remoteWriteHistogramRow) error {
	if len(rows) == 0 || s.cfg.HistogramsTable == "" {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_name, timestamp, id, histogram, version)", tableName(s.cfg.CHDatabase, s.cfg.HistogramsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		metricNames := make([]string, len(rows))
		timestamps := make([]int64, len(rows))
		ids := make([]uint64, len(rows))
		histograms := make([][]byte, len(rows))
		versions := make([]uint64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			metricNames[i] = row.MetricName
			timestamps[i] = row.TimestampMS
			ids[i] = row.ID
			histograms[i] = row.Histogram
			versions[i] = row.Version
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(metricNames); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(histograms); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(versions); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteExemplarRows(ctx context.Context, rows []remoteWriteExemplarRow) error {
	if len(rows) == 0 || s.cfg.ExemplarsTable == "" {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, timestamp, id, value, labels_json)", tableName(s.cfg.CHDatabase, s.cfg.ExemplarsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		timestamps := make([]int64, len(rows))
		ids := make([]uint64, len(rows))
		values := make([]float64, len(rows))
		labelsJSON := make([]string, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			timestamps[i] = row.TimestampMS
			ids[i] = row.ID
			values[i] = row.Value
			labelsJSON[i] = row.LabelsJSON
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(timestamps); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(ids); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(values); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(labelsJSON); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func (s *Server) insertRemoteMetadataRows(ctx context.Context, rows []remoteWriteMetadataRow) error {
	if len(rows) == 0 || s.cfg.MetricsTable == "" {
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s (team_id, metric_family_name, type, unit, help, updated_at)", tableName(s.cfg.CHDatabase, s.cfg.MetricsTable))
	return s.client.InsertColumns(ctx, sql, func(batch clickHouseBatch) (int, error) {
		teamIDs := make([]uint64, len(rows))
		names := make([]string, len(rows))
		types := make([]string, len(rows))
		units := make([]string, len(rows))
		helps := make([]string, len(rows))
		updatedAt := make([]int64, len(rows))
		for i, row := range rows {
			teamIDs[i] = row.TeamID
			names[i] = row.MetricFamilyName
			types[i] = row.Type
			units[i] = row.Unit
			helps[i] = row.Help
			updatedAt[i] = row.UpdatedAtMS
		}
		if err := batch.Column(0).Append(teamIDs); err != nil {
			return 0, err
		}
		if err := batch.Column(1).Append(names); err != nil {
			return 0, err
		}
		if err := batch.Column(2).Append(types); err != nil {
			return 0, err
		}
		if err := batch.Column(3).Append(units); err != nil {
			return 0, err
		}
		if err := batch.Column(4).Append(helps); err != nil {
			return 0, err
		}
		if err := batch.Column(5).Append(updatedAt); err != nil {
			return 0, err
		}
		return len(rows), nil
	})
}

func remoteReadAcceptsSamples(req *prompb.ReadRequest) bool {
	accepted := req.GetAcceptedResponseTypes()
	if len(accepted) == 0 {
		return true
	}
	for _, responseType := range accepted {
		if responseType == prompb.ReadRequest_SAMPLES {
			return true
		}
	}
	return false
}

func (s *Server) remoteReadSamples(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resp := &prompb.ReadResponse{Results: make([]*prompb.QueryResult, 0, len(req.GetQueries()))}
	for _, query := range req.GetQueries() {
		matchers, err := remoteReadMatchers(query.GetMatchers())
		if err != nil {
			return nil, err
		}
		q := &CHQuerier{queryable: s.queryable, mint: query.GetStartTimestampMs(), maxt: query.GetEndTimestampMs()}
		series, err := q.selectSeries(ctx, query.GetStartTimestampMs(), query.GetEndTimestampMs(), matchers...)
		if err != nil {
			return nil, err
		}
		if err := q.loadSamples(ctx, series, query.GetStartTimestampMs(), query.GetEndTimestampMs(), false, matchers); err != nil {
			return nil, err
		}
		if err := q.loadHistograms(ctx, series, query.GetStartTimestampMs(), query.GetEndTimestampMs(), matchers); err != nil {
			return nil, err
		}
		if err := q.loadExemplars(ctx, series, query.GetStartTimestampMs(), query.GetEndTimestampMs()); err != nil {
			return nil, err
		}

		result := &prompb.QueryResult{Timeseries: make([]*prompb.TimeSeries, 0, len(series))}
		for _, seriesMeta := range series {
			if len(seriesMeta.samples) == 0 && len(seriesMeta.histograms) == 0 && len(seriesMeta.exemplars) == 0 {
				continue
			}
			timeSeries := &prompb.TimeSeries{
				Labels:     labelsToPrompb(seriesMeta.labels),
				Samples:    make([]prompb.Sample, 0, len(seriesMeta.samples)),
				Histograms: make([]prompb.Histogram, 0, len(seriesMeta.histograms)),
				Exemplars:  make([]prompb.Exemplar, 0, len(seriesMeta.exemplars)),
			}
			for _, sample := range seriesMeta.samples {
				timeSeries.Samples = append(timeSeries.Samples, prompb.Sample{
					Timestamp: sample.t,
					Value:     sample.v,
				})
			}
			for _, histogram := range seriesMeta.histograms {
				timeSeries.Histograms = append(timeSeries.Histograms, histogram.h)
			}
			for _, exemplar := range seriesMeta.exemplars {
				timeSeries.Exemplars = append(timeSeries.Exemplars, exemplar.pb)
			}
			result.Timeseries = append(result.Timeseries, timeSeries)
		}
		resp.Results = append(resp.Results, result)
	}
	return resp, nil
}

func remoteReadMatchers(input []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(input))
	for _, matcher := range input {
		matchType, err := remoteReadMatcherType(matcher.GetType())
		if err != nil {
			return nil, err
		}
		converted, err := labels.NewMatcher(matchType, matcher.GetName(), matcher.GetValue())
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, nil
}

func remoteReadMatcherType(input prompb.LabelMatcher_Type) (labels.MatchType, error) {
	switch input {
	case prompb.LabelMatcher_EQ:
		return labels.MatchEqual, nil
	case prompb.LabelMatcher_NEQ:
		return labels.MatchNotEqual, nil
	case prompb.LabelMatcher_RE:
		return labels.MatchRegexp, nil
	case prompb.LabelMatcher_NRE:
		return labels.MatchNotRegexp, nil
	default:
		return labels.MatchEqual, fmt.Errorf("unsupported remote read matcher type %s", input.String())
	}
}

func labelsToPrompb(input labels.Labels) []prompb.Label {
	output := make([]prompb.Label, 0, input.Len())
	input.Range(func(label labels.Label) {
		output = append(output, prompb.Label{Name: label.Name, Value: label.Value})
	})
	return output
}
