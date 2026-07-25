package snuffle

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseClient struct {
	conn    clickhouse.Conn
	connErr error
	cfg     Config
	metrics *bridgeMetrics
}

// ponytail: request-scoped pools bound credential lifetime; add a bounded per-user pool only if handshake latency matters.
type requestClickHouseConnection struct {
	username string
	password string
	once     sync.Once
	conn     clickhouse.Conn
	err      error
}

type requestClickHouseConnectionKey struct{}

func NewClickHouseClient(cfg Config, metrics ...*bridgeMetrics) *ClickHouseClient {
	var bridgeMetrics *bridgeMetrics
	if len(metrics) > 0 {
		bridgeMetrics = metrics[0]
	}
	conn, err := openClickHouse(cfg, cfg.CHUser, cfg.CHPassword)
	return &ClickHouseClient{
		conn:    conn,
		connErr: err,
		cfg:     cfg,
		metrics: bridgeMetrics,
	}
}

func openClickHouse(cfg Config, username, password string) (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     clickHouseAddrs(cfg),
		Auth: clickhouse.Auth{
			Database: cfg.CHDatabase,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"allow_experimental_time_series_aggregate_functions": 1,
			// Every selector filters the label index on its sort-key prefix
			// (team_id, metric_name, label_name, label_value), which no
			// projection can improve on. ClickHouse still scans each
			// projection's primary index to decide that, and the label index
			// has ~900k marks, so the rejected analysis costs ~65ms per query.
			"optimize_use_projections": 0,
			// Remote write arrives pre-batched, so the async insert buffer is
			// there to coalesce writers, not to build a batch. ClickHouse's 10MB
			// default is far above anything one request contributes, so every
			// insert waited out the busy timeout (50-200ms) instead of flushing
			// on size. Bounding it flushes once a request's worth of data has
			// landed; small writers still coalesce on the timer as before.
			"async_insert_max_data_size": 1048576,
		},
		DialTimeout:     cfg.CHTimeout,
		MaxOpenConns:    32,
		MaxIdleConns:    8,
		ConnMaxLifetime: time.Hour,
	})
}

func (c *ClickHouseClient) withRequestCredentials(ctx context.Context, username, password string) (context.Context, func()) {
	requestConn := &requestClickHouseConnection{username: username, password: password}
	return context.WithValue(ctx, requestClickHouseConnectionKey{}, requestConn), func() {
		if requestConn.conn != nil {
			_ = requestConn.conn.Close()
		}
	}
}

func (c *ClickHouseClient) connection(ctx context.Context) (clickhouse.Conn, error) {
	requestConn, ok := ctx.Value(requestClickHouseConnectionKey{}).(*requestClickHouseConnection)
	if !ok {
		return c.conn, c.connErr
	}
	requestConn.once.Do(func() {
		requestConn.conn, requestConn.err = openClickHouse(c.cfg, requestConn.username, requestConn.password)
	})
	return requestConn.conn, requestConn.err
}

func clickHouseAddrs(cfg Config) []string {
	addr := strings.TrimSpace(cfg.CHAddr)
	if addr == "" {
		addr = "localhost:9000"
	}
	parts := strings.Split(addr, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			addrs = append(addrs, part)
		}
	}
	if len(addrs) == 0 {
		return []string{"localhost:9000"}
	}
	return addrs
}

type clickHouseRow interface {
	Scan(dest ...any) error
}

func (c *ClickHouseClient) QueryRows(ctx context.Context, sql string, handle func(clickHouseRow) error) error {
	return c.queryRows(ctx, sql, handle)
}

func (c *ClickHouseClient) QueryRowsWithExternalUInt64s(ctx context.Context, table, column string, values []uint64, sql string, handle func(clickHouseRow) error) error {
	external, err := ext.NewTable(table, ext.Column(column, "UInt64"))
	if err != nil {
		return err
	}
	for _, value := range values {
		if err := external.Append(value); err != nil {
			return err
		}
	}
	return c.queryRows(clickhouse.Context(ctx, clickhouse.WithExternalTable(external)), sql, handle)
}

func (c *ClickHouseClient) queryRows(ctx context.Context, sql string, handle func(clickHouseRow) error) (err error) {
	started := time.Now()
	var rowCount int64
	var scannedRows atomic.Uint64
	var readBytes atomic.Uint64
	if c.metrics != nil {
		c.metrics.clickHouseQueryInflight.Inc()
		defer c.metrics.clickHouseQueryInflight.Dec()
	}
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		scanned := int64(scannedRows.Load())
		read := int64(readBytes.Load())
		recordClickHouseRead(ctx, rowCount, scanned, read)
		c.metrics.observeClickHouseQuery(status, time.Since(started), rowCount, scanned, read)
	}()
	ctx = clickhouse.Context(ctx, clickhouse.WithProgress(func(progress *clickhouse.Progress) {
		scannedRows.Add(progress.Rows)
		readBytes.Add(progress.Bytes)
	}))
	conn, err := c.connection(ctx)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	result, err := conn.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("start clickhouse query: %w", err)
	}
	defer result.Close()

	for result.Next() {
		rowCount++
		if err := handle(result); err != nil {
			return fmt.Errorf("handle clickhouse query row %d: %w", rowCount, err)
		}
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("read clickhouse query result after %d rows: %w", rowCount, err)
	}
	return nil
}

func (c *ClickHouseClient) Exec(ctx context.Context, sql string) error {
	conn, err := c.connection(ctx)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("execute clickhouse statement: %w", err)
	}
	return nil
}

type clickHouseBatch = driver.Batch

func (c *ClickHouseClient) InsertColumns(ctx context.Context, sql string, appendColumns func(clickHouseBatch) (int, error)) error {
	return c.insertColumns(ctx, sql, true, appendColumns)
}

func (c *ClickHouseClient) InsertColumnsSync(ctx context.Context, sql string, appendColumns func(clickHouseBatch) (int, error)) error {
	return c.insertColumns(ctx, sql, false, appendColumns)
}

func (c *ClickHouseClient) insertColumns(ctx context.Context, sql string, async bool, appendColumns func(clickHouseBatch) (int, error)) (err error) {
	started := time.Now()
	table := insertTableName(sql)
	mode := "sync"
	if async {
		mode = "async"
	}
	var count int
	var writtenBytes atomic.Uint64
	if c.metrics != nil {
		c.metrics.clickHouseInsertInflight.Inc()
		defer c.metrics.clickHouseInsertInflight.Dec()
	}
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		c.metrics.observeClickHouseInsert(table, mode, status, time.Since(started), count, writtenBytes.Load())
	}()
	conn, err := c.connection(ctx)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	insertMode := "native insert"
	if async {
		ctx = clickhouse.Context(ctx, clickhouse.WithAsync(true))
		insertMode = "native async insert (async_insert=1 wait_for_async_insert=1)"
	}
	ctx = clickhouse.Context(ctx, clickhouse.WithProgress(func(progress *clickhouse.Progress) {
		writtenBytes.Add(progress.WroteBytes)
	}))
	batch, err := conn.PrepareBatch(ctx, sql)
	if err != nil {
		return fmt.Errorf("prepare %s: %w", insertMode, err)
	}
	sent := false
	defer func() {
		if !sent {
			_ = batch.Close()
		}
	}()
	count, err = appendColumns(batch)
	if err != nil {
		return fmt.Errorf("append native insert columns: %w", err)
	}
	if count == 0 {
		sent = true
		if err := batch.Close(); err != nil {
			return fmt.Errorf("close empty %s batch: %w", insertMode, err)
		}
		return nil
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send %s rows=%d: %w", insertMode, count, err)
	}
	sent = true
	recordClickHouseWrite(ctx, int64(count))
	return nil
}

func insertTableName(sql string) string {
	upper := strings.ToUpper(sql)
	const marker = "INSERT INTO "
	index := strings.Index(upper, marker)
	if index < 0 {
		return "unknown"
	}
	rest := strings.TrimSpace(sql[index+len(marker):])
	if rest == "" {
		return "unknown"
	}
	table := rest
	if cut := strings.IndexAny(rest, " \t\r\n("); cut >= 0 {
		table = rest[:cut]
	}
	table = strings.ReplaceAll(table, "`", "")
	if table == "" {
		return "unknown"
	}
	return table
}

func (c *ClickHouseClient) Ping(ctx context.Context) error {
	conn, err := c.connection(ctx)
	if err != nil {
		return err
	}
	return conn.Ping(ctx)
}

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func quoteIdent(ident string) string {
	if !identifierRE.MatchString(ident) {
		panic(fmt.Sprintf("invalid ClickHouse identifier %q", ident))
	}
	return "`" + ident + "`"
}

func tableName(database, table string) string {
	if strings.Contains(table, ".") {
		parts := strings.Split(table, ".")
		quoted := make([]string, 0, len(parts))
		for _, part := range parts {
			quoted = append(quoted, quoteIdent(part))
		}
		return strings.Join(quoted, ".")
	}
	if database == "" {
		return quoteIdent(table)
	}
	return quoteIdent(database) + "." + quoteIdent(table)
}

func sqlString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return "'" + value + "'"
}

func chTimeMillis(ms int64) string {
	return "fromUnixTimestamp64Milli(" + strconv.FormatInt(ms, 10) + ", 'UTC')"
}

func chTimeMillisExpr(expr string) string {
	return "fromUnixTimestamp64Milli(" + expr + ", 'UTC')"
}

func teamFilter(cfg Config) string {
	return "team_id = " + strconv.FormatUint(cfg.TeamID, 10)
}

func formatDurationSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
}
