// metrics_series.go 请求统计的**时间维度**（/v1/stats?range=... 数据源）。
//
// 与 metrics.go 的分工：那边是「累计值」（进程/统计起点至今一个数），这边是
// 「按小时分桶的时间序列」，回答「昨天怎么样」「这周比上周如何」。
//
// 设计要点：
//   - **按小时分桶，而不是直接按天**：按天回答不了「今天下午为什么慢」。
//     日/周只是查询期从小时桶滚动求和的**视图**，不预物化 —— 小时桶是唯一真相源，
//     避免「日表与小时表不一致」这类经典双写问题；代价是聚合时多遍历几十个桶。
//   - **累计与桶共用 modelMetrics.add**（见 metrics.go）：两处各写一份累加逻辑，
//     日后改一处漏一处就会出现「总览与趋势对不上」的诡异现象。测试
//     TestSeriesSumsMatchCumulative 锁死两者相等。
//   - **观测功能不阻断转发**：落盘失败、文件损坏、非法参数一律降级（打日志 /
//     从零开始 / 回落默认），绝不返回 5xx 或影响请求路径。
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultRetentionDays 小时桶默认保留天数。30 天 ≈ 720 桶。
	DefaultRetentionDays = 30
	// MaxRetentionDays 保留期上限（1 年）。配超了会被钳到这个值。
	//
	// 存在的意义是防手滑：metrics.retention_days 写成 3650（十年）不会报错，
	// 但会让桶数上限涨到 87600，内存和落盘文件都跟着涨一个数量级。
	MaxRetentionDays = 365
	// seriesFlushInterval 落盘节流间隔：只有 dirty 时才真正写文件。
	seriesFlushInterval = 60 * time.Second
)

// Interval 时间序列的聚合粒度。
type Interval string

const (
	// IntervalHour 按小时（原始桶，不聚合）。
	IntervalHour Interval = "hour"
	// IntervalDay 按自然日（本地时区）。
	IntervalDay Interval = "day"
	// IntervalWeek 按自然周（周一为起点，本地时区）。
	IntervalWeek Interval = "week"
)

// NormalizeInterval 规范化粒度参数，非法值回落 hour。
//
// 回落而非报错：统计是观测功能，面板传了个不认识的粒度不该让整页失败。
func NormalizeInterval(s string) Interval {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "day", "daily", "d":
		return IntervalDay
	case "week", "weekly", "w":
		return IntervalWeek
	default:
		return IntervalHour
	}
}

// RangeQuery 时间范围查询参数。零值 From/To 表示该侧不限制。
type RangeQuery struct {
	// From/To 查询区间（含 From、不含 To）。
	From time.Time
	To   time.Time
	// Interval 聚合粒度，空 = hour。
	Interval Interval
	// Model 非空时只看该模型。
	Model string
}

// RangePoint 时间序列上的一个数据点。
//
// Stats 与 Derived 都是完整的 ModelStatPayload（字段集一致）：面板的
// TrendChart 从 derived 取均值/比率类指标（首字延迟、吞吐、命中率），从 stats
// 取计数类指标（请求数、token、计费）——两者都需要完整字段，故不复用「原始累加
// 量」结构（那会让 stats.credit 之类的字段名对不上，趋势线恒为 0）。
type RangePoint struct {
	// Key 分组键："2026-09-14T15" / "2026-09-14" / "2026-W38"。
	Key string `json:"key"`
	// Start 分组起点（本地时区，含）。
	Start time.Time `json:"start"`
	// End 分组终点（本地时区，不含）。
	End time.Time `json:"end"`

	Stats   ModelStatPayload `json:"stats"`
	Derived ModelStatPayload `json:"derived"`
}

// RangeResult 时间范围聚合结果。
type RangeResult struct {
	Interval Interval     `json:"interval"`
	From     time.Time    `json:"from"`
	To       time.Time    `json:"to"`
	Points   []RangePoint `json:"points"`
	// Total 该范围内所有分组的汇总。
	Total ModelStatPayload `json:"total"`
	// Models 范围内出现过的模型（升序），供面板做模型筛选。
	Models []string `json:"models"`
}

// seriesStore 按小时分桶的时间序列。
type seriesStore struct {
	mu      sync.Mutex
	buckets map[int64]map[string]*modelMetrics // key = 小时起点的 Unix 秒
	// retention 桶保留期；<=0 时回落 DefaultRetentionDays。
	retention time.Duration
	// stateFile 落盘路径；空 = 纯内存（重启清零）。
	stateFile string
	dirty     bool
	lastPrune time.Time
	warned    bool // 单桶容量超限只告警一次，避免刷屏
}

var globalSeries = &seriesStore{
	buckets:   make(map[int64]map[string]*modelMetrics),
	retention: DefaultRetentionDays * 24 * time.Hour,
}

// ---------------------------------------------------------------------------
// 记录
// ---------------------------------------------------------------------------

// recordSeries 把一次请求记入当前小时桶。由 recordChatMetric 调用（与累计表同源）。
func (ss *seriesStore) record(model string, s *chatStat, total time.Duration, now time.Time) {
	key := hourStart(now).Unix()

	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.pruneLockedIfDue(now)

	bucket, ok := ss.buckets[key]
	if !ok {
		bucket = make(map[string]*modelMetrics)
		ss.buckets[key] = bucket
	}

	mm, ok := bucket[model]
	if !ok {
		if len(bucket) >= metricsCap {
			if !ss.warned {
				ss.warned = true
				logMetricsCapWarn(model)
			}
			return
		}
		mm = &modelMetrics{}
		bucket[model] = mm
	}
	mm.add(s, total, now)
	ss.dirty = true
}

// hourStart 返回时刻所在小时的起点（本地时区整点）。
//
// 不用 t.Truncate(time.Hour)：Truncate 以 UTC 零时为基准，在 UTC+05:45 这类
// 非整点偏移的时区上会截出偏移半小时的「小时」。
func hourStart(t time.Time) time.Time {
	t = t.Local()
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.Local)
}

// groupKey 把时刻按粒度归组，返回组键与组的起止时间（本地时区）。
func groupKey(t time.Time, iv Interval) (key string, start, end time.Time) {
	t = t.Local()
	switch iv {
	case IntervalDay:
		start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
		end = start.AddDate(0, 0, 1)
		return start.Format("2006-01-02"), start, end
	case IntervalWeek:
		// 以周一为一周起点（中文语境「这周」的直觉）。Go 的 Weekday()：Sunday=0，
		// 故先折算成「距本周一的天数」再回退。键用 ISO 周号（跨年周归属正确）。
		offset := (int(t.Weekday()) + 6) % 7
		start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, -offset)
		end = start.AddDate(0, 0, 7)
		isoYear, isoWeek := start.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", isoYear, isoWeek), start, end
	default: // hour
		start = hourStart(t)
		end = start.Add(time.Hour)
		return start.Format("2006-01-02T15"), start, end
	}
}

// ---------------------------------------------------------------------------
// 查询
// ---------------------------------------------------------------------------

// SeriesBuckets 返回当前保留的小时桶数（供 /v1/stats 透出，面板据此展示可回溯范围）。
func SeriesBuckets() int {
	ss := globalSeries
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.buckets)
}

// MetricsRange 按时间范围聚合查询。
func MetricsRange(q RangeQuery) RangeResult {
	return globalSeries.query(q)
}

func (ss *seriesStore) query(q RangeQuery) RangeResult {
	iv := q.Interval
	if iv == "" {
		iv = IntervalHour
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// 先把命中的小时桶按分组键归并。
	type group struct {
		start, end time.Time
		models     map[string]*modelMetrics
	}
	groups := make(map[string]*group)
	modelSet := make(map[string]bool)

	for hourKey, bucket := range ss.buckets {
		t := time.Unix(hourKey, 0)
		// 区间过滤：桶起点落在 [From, To) 内才计入。
		if !q.From.IsZero() && t.Before(q.From) {
			continue
		}
		if !q.To.IsZero() && !t.Before(q.To) {
			continue
		}
		for model, mm := range bucket {
			if q.Model != "" && model != q.Model {
				continue
			}
			modelSet[model] = true

			gk, gstart, gend := groupKey(t, iv)
			g, ok := groups[gk]
			if !ok {
				g = &group{start: gstart, end: gend, models: make(map[string]*modelMetrics)}
				groups[gk] = g
			}
			dst, ok := g.models[model]
			if !ok {
				dst = &modelMetrics{}
				g.models[model] = dst
			}
			dst.mergeFrom(mm)
		}
	}

	// 每个分组跨模型汇总成一个数据点。
	points := make([]RangePoint, 0, len(groups))
	var total modelMetrics
	for k, g := range groups {
		var merged modelMetrics
		for _, mm := range g.models {
			merged.mergeFrom(mm)
		}
		total.mergeFrom(&merged)

		// 数据点代表「该时段全部模型」，故 model 字段统一为 "(all)"。
		payload := deriveModelStat("(all)", &merged)
		points = append(points, RangePoint{
			Key:     k,
			Start:   g.start,
			End:     g.end,
			Stats:   payload,
			Derived: payload,
		})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Start.Before(points[j].Start) })

	models := make([]string, 0, len(modelSet))
	for m := range modelSet {
		models = append(models, m)
	}
	sort.Strings(models)

	out := RangeResult{
		Interval: iv,
		Points:   points,
		Total:    deriveModelStat("(all)", &total),
		Models:   models,
	}
	// 回填实际生效的区间边界（面板拿它显示「实际区间」）。
	// 无数据时回落查询参数本身：面板据此仍能显示「你选的是哪段，只是没数据」。
	if len(points) > 0 {
		out.From = points[0].Start
		out.To = points[len(points)-1].End
	} else {
		out.From = q.From
		out.To = q.To
	}
	return out
}

// ---------------------------------------------------------------------------
// 清理
// ---------------------------------------------------------------------------

// pruneLocked 删除超出保留期的小时桶。调用方必须已持锁。
func (ss *seriesStore) pruneLocked(now time.Time) {
	ret := ss.retention
	if ret <= 0 {
		ret = DefaultRetentionDays * 24 * time.Hour
	}
	cutoff := now.Add(-ret)
	for k := range ss.buckets {
		if time.Unix(k, 0).Before(cutoff) {
			delete(ss.buckets, k)
			ss.dirty = true
		}
	}
}

// pruneLockedIfDue 距上次清理满一小时才真正清理。调用方必须已持锁。
//
// 节流的原因：桶只按小时增长，每次请求都遍历整个 series 是纯浪费。
func (ss *seriesStore) pruneLockedIfDue(now time.Time) {
	if now.Sub(ss.lastPrune) < time.Hour {
		return
	}
	ss.pruneLocked(now)
	ss.lastPrune = now
}

// pruneIfDue 是 pruneLockedIfDue 的自行取锁版本，供后台刷盘循环调用。
//
// 为什么后台也要清：record 只在**有请求**时触发清理，网关空闲（夜里没人用、
// 或账号全挂了）时就永远不清——过期桶会一直躺在内存和磁盘里。刷盘循环本来就
// 每分钟醒一次，顺手清一下成本为零。
func (ss *seriesStore) pruneIfDue(now time.Time) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.pruneLockedIfDue(now)
}

// resetSeries 清空时间序列（/v1/stats/reset 与累计值一起清）。
func (ss *seriesStore) reset() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.buckets = make(map[int64]map[string]*modelMetrics)
	ss.warned = false
	ss.dirty = true
}

// ---------------------------------------------------------------------------
// 落盘
// ---------------------------------------------------------------------------

// diskState 统计落盘结构。累计值与时间序列同文件 —— 两者由同一个 add 写入，
// 分开存会在「一个恢复成功一个失败」时破坏「累计 == 各桶之和」的不变式。
type diskState struct {
	Since  time.Time                      `json:"since"`
	Models map[string]diskModel           `json:"models"`
	Series map[int64]map[string]diskModel `json:"series,omitempty"`
	// RetentionSec 落盘时的保留期（秒），供排查「桶为何被清理」。
	RetentionSec int64 `json:"retention_sec,omitempty"`
}

// diskModel 是 modelMetrics 的落盘镜像。
//
// 为什么不给 modelMetrics 直接加 json tag：累加器字段全是内部量（和与计数），
// 且都是非导出字段（encoding/json 根本不认）。用一份显式 DTO 把「磁盘格式」与
// 「内存结构」解耦：日后调整累加字段不会读不了旧文件。
type diskModel struct {
	Requests  int64 `json:"requests"`
	Success   int64 `json:"success"`
	Failed    int64 `json:"failed"`
	Streaming int64 `json:"streaming"`

	TTFBSumMS float64 `json:"ttfb_sum_ms"`
	TTFBCount int64   `json:"ttfb_count"`
	LatSumMS  float64 `json:"latency_sum_ms"`
	GenSecSum float64 `json:"gen_sec_sum"`

	PromptTok  int64 `json:"prompt_tokens"`
	CompTok    int64 `json:"completion_tokens"`
	CacheHit   int64 `json:"cache_hit_tokens"`
	CacheMiss  int64 `json:"cache_miss_tokens"`
	CacheWrite int64 `json:"cache_write_tokens"`

	Credit   float64   `json:"credit"`
	LastSeen time.Time `json:"last_seen"`
}

func (mm *modelMetrics) toDisk() diskModel {
	return diskModel{
		Requests: mm.requests, Success: mm.success, Failed: mm.failed, Streaming: mm.streaming,
		TTFBSumMS: mm.ttfbSumMS, TTFBCount: mm.ttfbCount,
		LatSumMS: mm.latSumMS, GenSecSum: mm.genSecSum,
		PromptTok: mm.promptTok, CompTok: mm.compTok,
		CacheHit: mm.cacheHit, CacheMiss: mm.cacheMiss, CacheWrite: mm.cacheWrite,
		Credit: mm.credit, LastSeen: mm.lastSeen,
	}
}

func (dm diskModel) toMem() *modelMetrics {
	return &modelMetrics{
		requests: dm.Requests, success: dm.Success, failed: dm.Failed, streaming: dm.Streaming,
		ttfbSumMS: dm.TTFBSumMS, ttfbCount: dm.TTFBCount,
		latSumMS: dm.LatSumMS, genSecSum: dm.GenSecSum,
		promptTok: dm.PromptTok, compTok: dm.CompTok,
		cacheHit: dm.CacheHit, cacheMiss: dm.CacheMiss, cacheWrite: dm.CacheWrite,
		credit: dm.Credit, lastSeen: dm.LastSeen,
	}
}

// InitMetrics 配置统计的落盘路径与保留期，并启动定期刷盘。由 main 启动时调用一次。
//
// file 为空 = 纯内存：不读也不写，重启清零。file 非空时尝试恢复历史（文件缺失/
// 损坏则静默从零开始，不阻断启动）。
func InitMetrics(file string, retentionDays int) {
	ss := globalSeries
	ss.mu.Lock()
	ss.stateFile = file
	if retentionDays > 0 {
		if retentionDays > MaxRetentionDays {
			retentionDays = MaxRetentionDays
		}
		ss.retention = time.Duration(retentionDays) * 24 * time.Hour
	}
	ss.mu.Unlock()

	if file != "" {
		ss.load()
	}
	go ss.flushLoop()
}

// FlushMetrics 立即落盘（进程退出前调用）。无变更时是空操作。
func FlushMetrics() { globalSeries.flush() }

// flushLoop 定期清理过期桶并落盘。
//
// 清理与落盘共用同一个循环：两者都是后台维护，且清理产生的删除同样要落盘
// （否则重启后过期桶又回来了）。拆两个 goroutine 只会多一份调度与锁竞争。
func (ss *seriesStore) flushLoop() {
	t := time.NewTicker(seriesFlushInterval)
	defer t.Stop()
	for range t.C {
		ss.pruneIfDue(time.Now())
		ss.flush()
	}
}

func (ss *seriesStore) flush() {
	ss.mu.Lock()
	dirty := ss.dirty
	ss.mu.Unlock()
	if !dirty {
		return
	}
	ss.save()
}

// cumulativeDiskSnapshot 取累计值的落盘镜像。调用方无需持任何锁。
func cumulativeDiskSnapshot() (map[string]diskModel, time.Time) {
	m := globalMetrics
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]diskModel, len(m.byModel))
	for name, mm := range m.byModel {
		out[name] = mm.toDisk()
	}
	return out, m.since
}

// save 原子落盘。
//
// 锁序：**先 metrics 后 series，且两次获取不嵌套**。两把锁若嵌套获取，会与
// MetricsSnapshotOf / ResetMetrics（同为 metrics → series 顺序）形成锁序反转。
// 代价是累计快照与桶快照之间可能夹进一次请求（两者差一条）——落盘文件是观测
// 记录而非事务账本，这个瞬时偏差可接受。
//
// 落盘失败只打日志不阻断：统计是观测功能，不该因为磁盘问题影响转发。
func (ss *seriesStore) save() {
	models, since := cumulativeDiskSnapshot()

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if !ss.dirty {
		return // 取累计快照期间已被另一次 save 刷过
	}
	ss.dirty = false
	if ss.stateFile == "" {
		return
	}

	state := diskState{
		Since:        since,
		Models:       models,
		Series:       make(map[int64]map[string]diskModel, len(ss.buckets)),
		RetentionSec: int64(ss.retention / time.Second),
	}
	for hour, bucket := range ss.buckets {
		bucketOut := make(map[string]diskModel, len(bucket))
		for name, mm := range bucket {
			bucketOut[name] = mm.toDisk()
		}
		state.Series[hour] = bucketOut
	}

	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Printf("WARN: [metrics] 统计序列化失败，本次不落盘: %v", err)
		return
	}
	if dir := filepath.Dir(ss.stateFile); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	// 先写临时文件再 rename：避免进程在写一半时被杀留下半截 JSON。
	tmp := ss.stateFile + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("WARN: [metrics] 统计落盘失败（%s）: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, ss.stateFile); err != nil {
		log.Printf("WARN: [metrics] 统计落盘改名失败（%s）: %v", ss.stateFile, err)
	}
}

// load 恢复累计值与时间序列。文件缺失/损坏一律静默从零开始 —— 统计不该因为
// 一个坏文件而让网关起不来。
//
// 锁序同 save：先 metrics 后 series，不嵌套。
func (ss *seriesStore) load() {
	raw, err := os.ReadFile(ss.stateFile)
	if err != nil {
		return // 首次运行（文件不存在）或权限问题：从零开始
	}
	var state diskState
	if err := json.Unmarshal(raw, &state); err != nil {
		log.Printf("WARN: [metrics] 统计文件损坏，从零开始（%s）: %v", ss.stateFile, err)
		return
	}

	globalMetrics.mu.Lock()
	if !state.Since.IsZero() {
		globalMetrics.since = state.Since
	}
	if state.Models != nil {
		byModel := make(map[string]*modelMetrics, len(state.Models))
		for name, dm := range state.Models {
			byModel[name] = dm.toMem()
		}
		globalMetrics.byModel = byModel
	}
	globalMetrics.mu.Unlock()

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if state.Series != nil {
		buckets := make(map[int64]map[string]*modelMetrics, len(state.Series))
		for hour, models := range state.Series {
			bucket := make(map[string]*modelMetrics, len(models))
			for name, dm := range models {
				bucket[name] = dm.toMem()
			}
			buckets[hour] = bucket
		}
		ss.buckets = buckets
	}

	// 启动时清一次过期桶：进程可能停机数天，期间没有请求触发清理。
	ss.pruneLocked(time.Now())
	ss.dirty = false
}
