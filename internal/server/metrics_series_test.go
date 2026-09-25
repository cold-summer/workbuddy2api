package server

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
)

// ── 测试脚手架 ────────────────────────────────────────────────────────────

// resetSeriesForTest 隔离用例间的全局统计状态（累计值 + 小时桶 + 落盘配置）。
func resetSeriesForTest(t *testing.T) {
	t.Helper()
	ResetMetrics() // 清累计值与桶
	ss := globalSeries
	ss.mu.Lock()
	ss.stateFile = ""
	ss.retention = DefaultRetentionDays * 24 * time.Hour
	ss.lastPrune = time.Time{}
	ss.warned = false
	ss.dirty = false
	ss.mu.Unlock()
	t.Cleanup(func() {
		ResetMetrics()
		ss.mu.Lock()
		ss.stateFile = ""
		ss.lastPrune = time.Time{}
		ss.dirty = false
		ss.mu.Unlock()
	})
}

// setSeriesFileForTest 把落盘路径指向临时文件。
func setSeriesFileForTest(t *testing.T, file string) {
	t.Helper()
	ss := globalSeries
	ss.mu.Lock()
	ss.stateFile = file
	ss.mu.Unlock()
}

// mkStat 造一次请求观测。toks<0 表示「观测缺失」哨兵（见 modelMetrics.add 注释）。
func mkStat(model string, status, toks int) *chatStat {
	return &chatStat{
		model: model, mode: "stream", status: status,
		ttfb: 1500 * time.Millisecond, toks: toks, hasUsage: true, prompt: 100,
		cacheHit: 800, cacheMiss: 200, cacheWr: 0,
		credit: 0.02, hasCredit: true,
	}
}

// assertSameCounters 只比对计数与和（不含均值/比率）。
//
// 用途：把「各数据点的计数相加」与累计值对比——数据点携带的是派生后的
// ModelStatPayload，相加出来的均值没有意义（均值不能相加），只有计数可比。
func assertSameCounters(t *testing.T, label string, want, got ModelStatPayload) {
	t.Helper()
	counters := []struct {
		name string
		w, g int64
	}{
		{"requests", want.Requests, got.Requests},
		{"success", want.Success, got.Success},
		{"failed", want.Failed, got.Failed},
		{"streaming", want.Streaming, got.Streaming},
		{"prompt_tokens", want.PromptTokens, got.PromptTokens},
		{"completion_tokens", want.CompletionTokens, got.CompletionTokens},
		{"total_tokens", want.TotalTokens, got.TotalTokens},
		{"cache_hit_tokens", want.CacheHitTokens, got.CacheHitTokens},
		{"cache_miss_tokens", want.CacheMissTokens, got.CacheMissTokens},
		{"cache_write_tokens", want.CacheWriteTokens, got.CacheWriteTokens},
	}
	for _, c := range counters {
		if c.w != c.g {
			t.Errorf("%s: %s = %d want %d", label, c.name, c.g, c.w)
		}
	}
	if math.Abs(want.Credit-got.Credit) > 1e-9 {
		t.Errorf("%s: credit = %v want %v", label, got.Credit, want.Credit)
	}
}

// assertSameStat 比对两份派生统计：计数与和精确相等，均值/比率按浮点容差。
func assertSameStat(t *testing.T, label string, want, got ModelStatPayload) {
	t.Helper()
	assertSameCounters(t, label, want, got)
	floats := []struct {
		name string
		w, g float64
	}{
		{"avg_ttfb_ms", want.AvgTTFBMS, got.AvgTTFBMS},
		{"avg_latency_ms", want.AvgLatencyMS, got.AvgLatencyMS},
		{"tokens_per_sec", want.TokensPerSec, got.TokensPerSec},
		{"cache_hit_rate", want.CacheHitRate, got.CacheHitRate},
		{"credit_per_req", want.CreditPerReq, got.CreditPerReq},
	}
	for _, f := range floats {
		if math.Abs(f.w-f.g) > 1e-9 {
			t.Errorf("%s: %s = %v want %v", label, f.name, f.g, f.w)
		}
	}
}

// putBucketForTest 直接放置一个桶，绕过 record 的清理节流。
//
// 保留期用例需要「同时存在一个过期桶和一个未过期桶」，而 record 每次跨小时都会
// 顺手清理——直接放置才能把清理逻辑单独拎出来测。
func putBucketForTest(t *testing.T, at time.Time, model string, requests int64) {
	t.Helper()
	ss := globalSeries
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.buckets[hourStart(at).Unix()] = map[string]*modelMetrics{
		model: {requests: requests, success: requests, lastSeen: at},
	}
}

// ── 不变式：累计 == 各桶之和 ──────────────────────────────────────────────

// TestSeriesSumsMatchCumulative 锁死「累计值与小时桶由同一个 add 写入」这条不变式。
//
// 这是本次改动最关键的一条：累计表与桶若各写一份累加逻辑，日后改一处漏一处就会
// 出现「总览与趋势对不上」——用户看到的是两个数字互相矛盾，且极难定位。
func TestSeriesSumsMatchCumulative(t *testing.T) {
	resetSeriesForTest(t)
	base := time.Date(2026, 9, 20, 10, 0, 0, 0, time.Local)

	// 跨 3 个小时桶、2 个模型，含一次失败与一次「观测缺失」。
	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, base)
	recordChatMetricAt(mkStat("m1", 200, 200), 6*time.Second, base.Add(30*time.Minute))
	recordChatMetricAt(mkStat("m2", 429, -1), time.Second, base.Add(2*time.Hour))
	recordChatMetricAt(mkStat("m1", 200, 300), 7*time.Second, base.Add(25*time.Hour))

	snap := MetricsSnapshotOf()
	if snap.SeriesBuckets != 3 {
		t.Fatalf("series_buckets = %d want 3", snap.SeriesBuckets)
	}

	// 全区间按小时聚合：每个桶一个点，且各桶之和必须等于累计值。
	res := MetricsRange(RangeQuery{Interval: IntervalHour})
	if len(res.Points) != 3 {
		t.Fatalf("points = %d want 3", len(res.Points))
	}
	assertSameStat(t, "range.total vs snapshot.total", snap.Total, res.Total)

	// 逐桶求和再与累计值比：即使分组正确，漏字段也会在这里暴露。
	var sum ModelStatPayload
	for _, p := range res.Points {
		sum.Requests += p.Stats.Requests
		sum.Success += p.Stats.Success
		sum.Failed += p.Stats.Failed
		sum.Streaming += p.Stats.Streaming
		sum.PromptTokens += p.Stats.PromptTokens
		sum.CompletionTokens += p.Stats.CompletionTokens
		sum.TotalTokens += p.Stats.TotalTokens
		sum.CacheHitTokens += p.Stats.CacheHitTokens
		sum.CacheMissTokens += p.Stats.CacheMissTokens
		sum.CacheWriteTokens += p.Stats.CacheWriteTokens
		sum.Credit += p.Stats.Credit
	}
	assertSameCounters(t, "sum(points) vs snapshot.total", snap.Total, sum)
}

// TestRangePointCarriesFullPayload 数据点必须同时给出计数与派生指标。
//
// 面板的 TrendChart 从 stats 取计数（请求数/token/计费）、从 derived 取均值
// （首字延迟/吞吐/命中率）——两者字段集都必须完整，否则对应那条趋势线恒为 0。
func TestRangePointCarriesFullPayload(t *testing.T) {
	resetSeriesForTest(t)
	at := time.Date(2026, 9, 20, 10, 0, 0, 0, time.Local)
	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, at)

	res := MetricsRange(RangeQuery{Interval: IntervalHour})
	if len(res.Points) != 1 {
		t.Fatalf("points = %d want 1", len(res.Points))
	}
	p := res.Points[0]

	// stats 侧：计数 + 计费（上游把计费塞进原始字段，面板读不到 → 趋势线恒 0）。
	if p.Stats.Requests != 1 || p.Stats.CompletionTokens != 100 {
		t.Errorf("stats 计数缺失: %+v", p.Stats)
	}
	if p.Stats.Credit <= 0 {
		t.Errorf("stats.credit = %v want > 0（面板「计费」趋势线依赖它）", p.Stats.Credit)
	}
	// derived 侧：均值。
	if p.Derived.AvgTTFBMS != 1500 {
		t.Errorf("derived.avg_ttfb_ms = %v want 1500", p.Derived.AvgTTFBMS)
	}
	if p.Derived.CacheHitRate != 0.8 {
		t.Errorf("derived.cache_hit_rate = %v want 0.8", p.Derived.CacheHitRate)
	}
}

// ── 分组键 ────────────────────────────────────────────────────────────────

func TestGroupKeyHour(t *testing.T) {
	at := time.Date(2026, 9, 20, 10, 30, 0, 0, time.Local)
	key, start, end := groupKey(at, IntervalHour)
	if key != "2026-09-20T10" {
		t.Errorf("key = %q want 2026-09-20T10", key)
	}
	if start.Hour() != 10 || start.Minute() != 0 {
		t.Errorf("start = %v want 10:00", start)
	}
	if end.Sub(start) != time.Hour {
		t.Errorf("end-start = %v want 1h", end.Sub(start))
	}
	// 同一小时内的两个时刻必须落进同一个桶。
	if k2, _, _ := groupKey(at.Add(29*time.Minute), IntervalHour); k2 != key {
		t.Errorf("同小时被分到不同桶: %q vs %q", k2, key)
	}
	// 下一个整点是新桶。
	if k3, _, _ := groupKey(at.Add(30*time.Minute), IntervalHour); k3 == key {
		t.Errorf("跨小时仍落在同一桶: %q", k3)
	}
}

func TestGroupKeyDay(t *testing.T) {
	key, start, end := groupKey(time.Date(2026, 9, 20, 23, 59, 0, 0, time.Local), IntervalDay)
	if key != "2026-09-20" {
		t.Errorf("key = %q want 2026-09-20", key)
	}
	if start.Hour() != 0 || start.Minute() != 0 || start.Day() != 20 {
		t.Errorf("start = %v want 2026-09-20 00:00", start)
	}
	if end.Day() != 21 {
		t.Errorf("end = %v want 2026-09-21", end)
	}
}

// TestGroupKeyWeekStartsMonday 周起点必须是周一（中文语境「这周」的直觉），
// 且周一至周日归同一周、跨年周归属正确。
func TestGroupKeyWeekStartsMonday(t *testing.T) {
	monday := time.Date(2026, 9, 21, 0, 0, 0, 0, time.Local)
	if monday.Weekday() != time.Monday {
		t.Fatalf("用例前提不成立：2026-09-21 是 %v，应为周一", monday.Weekday())
	}

	key, start, _ := groupKey(monday, IntervalWeek)
	if start.Weekday() != time.Monday {
		t.Errorf("周起点 = %v want 周一", start.Weekday())
	}
	// 周一到周日同桶。
	for i := 0; i < 7; i++ {
		k, _, _ := groupKey(monday.AddDate(0, 0, i), IntervalWeek)
		if k != key {
			t.Errorf("周一+%d 天落在不同周: %q vs %q", i, k, key)
		}
	}
	// 下周一进入新桶。
	if k, _, _ := groupKey(monday.AddDate(0, 0, 7), IntervalWeek); k == key {
		t.Errorf("下周一仍落在同一周: %q", k)
	}
	// 跨年：2026-12-31(周四) 与 2027-01-01(周五) 属同一 ISO 周。
	nyEve := time.Date(2026, 12, 31, 12, 0, 0, 0, time.Local)
	nyDay := time.Date(2027, 1, 1, 12, 0, 0, 0, time.Local)
	k1, _, _ := groupKey(nyEve, IntervalWeek)
	k2, _, _ := groupKey(nyDay, IntervalWeek)
	if k1 != k2 {
		t.Errorf("跨年同一 ISO 周被拆开: %q vs %q", k1, k2)
	}
}

func TestNormalizeIntervalFallsBackToHour(t *testing.T) {
	cases := map[string]Interval{
		"":       IntervalHour,
		"hour":   IntervalHour,
		"bogus":  IntervalHour,
		"d":      IntervalDay,
		"daily":  IntervalDay,
		"DAY":    IntervalDay,
		" day ":  IntervalDay,
		"w":      IntervalWeek,
		"weekly": IntervalWeek,
		"Week":   IntervalWeek,
	}
	for in, want := range cases {
		if got := NormalizeInterval(in); got != want {
			t.Errorf("NormalizeInterval(%q) = %q want %q", in, got, want)
		}
	}
}

// ── 区间与模型筛选 ────────────────────────────────────────────────────────

func TestRangeFiltersByWindowAndModel(t *testing.T) {
	resetSeriesForTest(t)
	day := time.Date(2026, 9, 20, 0, 0, 0, 0, time.Local)

	// 9/20 10:00 m1；9/21 10:00 m1；9/21 11:00 m2。
	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, day.Add(10*time.Hour))
	recordChatMetricAt(mkStat("m1", 200, 200), 5*time.Second, day.Add(34*time.Hour))
	recordChatMetricAt(mkStat("m2", 200, 300), 5*time.Second, day.Add(35*time.Hour))

	// 区间 [9/21 00:00, 9/22 00:00) 只含后两次。
	res := MetricsRange(RangeQuery{
		From:     day.Add(24 * time.Hour),
		To:       day.Add(48 * time.Hour),
		Interval: IntervalDay,
	})
	if len(res.Points) != 1 {
		t.Fatalf("points = %d want 1（只应命中 9/21）", len(res.Points))
	}
	if res.Points[0].Key != "2026-09-21" {
		t.Errorf("key = %q want 2026-09-21", res.Points[0].Key)
	}
	if res.Total.Requests != 2 {
		t.Errorf("requests = %d want 2", res.Total.Requests)
	}
	// 边界回填成实际生效区间（面板拿它显示「实际区间」）。
	if !res.From.Equal(day.Add(24*time.Hour)) || !res.To.Equal(day.Add(48*time.Hour)) {
		t.Errorf("from/to = %v → %v want 9/21 00:00 → 9/22 00:00", res.From, res.To)
	}

	// 模型筛选：只看 m2。
	resM2 := MetricsRange(RangeQuery{Interval: IntervalDay, Model: "m2"})
	if len(resM2.Models) != 1 || resM2.Models[0] != "m2" {
		t.Fatalf("models = %v want [m2]", resM2.Models)
	}
	if resM2.Total.Requests != 1 || resM2.Total.CompletionTokens != 300 {
		t.Errorf("m2 汇总 = %+v want 1 次 / 300 token", resM2.Total)
	}
}

func TestRangeEmptyResultIsEmptyArray(t *testing.T) {
	resetSeriesForTest(t)
	res := MetricsRange(RangeQuery{Interval: IntervalDay})

	// 面板用 points.length 判断有无数据；null 会让它多一层判空。
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back["points"] == nil {
		t.Errorf("空区间的 points 序列化成 null，want []")
	}
	if back["models"] == nil {
		t.Errorf("空区间的 models 序列化成 null，want []")
	}
}

// ── 保留期 ────────────────────────────────────────────────────────────────

func TestSeriesPruneDropsExpiredBuckets(t *testing.T) {
	resetSeriesForTest(t)
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.Local)

	// 一个刚过保留期的桶 + 一个保留期内的桶（直接放置，绕过 record 的清理节流）。
	putBucketForTest(t, now.Add(-31*24*time.Hour), "m1", 1)
	putBucketForTest(t, now.Add(-2*time.Hour), "m1", 1)

	if got := SeriesBuckets(); got != 2 {
		t.Fatalf("前置桶数 = %d want 2", got)
	}

	globalSeries.mu.Lock()
	globalSeries.pruneLocked(now)
	globalSeries.mu.Unlock()

	if got := SeriesBuckets(); got != 1 {
		t.Errorf("清理后桶数 = %d want 1（31 天前的桶应被删）", got)
	}
	res := MetricsRange(RangeQuery{Interval: IntervalHour})
	if len(res.Points) != 1 {
		t.Errorf("清理后点数 = %d want 1", len(res.Points))
	}
}

// TestSeriesPrunesOnRecord 记录跨小时的新请求时会顺手清理过期桶。
//
// 清理节流挂在 record 上（不是后台 goroutine）：桶只按小时增长，每次请求都遍历
// 整个 series 是浪费，所以用「距上次清理 ≥ 1 小时」跳过绝大多数调用。
func TestSeriesPrunesOnRecord(t *testing.T) {
	resetSeriesForTest(t)
	now := time.Now()

	putBucketForTest(t, now.Add(-31*24*time.Hour), "m1", 1)
	// 把 lastPrune 拨回过去，让下一次 record 触发清理。
	globalSeries.mu.Lock()
	globalSeries.lastPrune = time.Time{}
	globalSeries.mu.Unlock()

	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, now)

	if got := SeriesBuckets(); got != 1 {
		t.Errorf("桶数 = %d want 1（过期桶应被 record 顺手清掉，只留当前桶）", got)
	}
}

// ── 落盘 ──────────────────────────────────────────────────────────────────

func TestSeriesPersistenceRoundTrip(t *testing.T) {
	resetSeriesForTest(t)
	file := filepath.Join(t.TempDir(), "metrics.json")
	setSeriesFileForTest(t, file)

	base := time.Date(2026, 9, 20, 10, 0, 0, 0, time.Local)
	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, base)
	recordChatMetricAt(mkStat("m1", 200, 200), 6*time.Second, base.Add(3*time.Hour))
	recordChatMetricAt(mkStat("m2", 429, -1), time.Second, base.Add(3*time.Hour))

	want := MetricsSnapshotOf()
	FlushMetrics()

	if _, err := os.Stat(file); err != nil {
		t.Fatalf("落盘文件不存在: %v", err)
	}
	// 落盘必须是原子替换：临时文件不能留在原地。
	if _, err := os.Stat(file + ".tmp"); err == nil {
		t.Errorf("临时文件 %s.tmp 未被 rename 掉", file)
	}

	// 清空内存后恢复。
	ResetMetrics()
	if got := SeriesBuckets(); got != 0 {
		t.Fatalf("清空后桶数 = %d want 0", got)
	}
	globalSeries.load()

	got := MetricsSnapshotOf()
	if got.SeriesBuckets != want.SeriesBuckets {
		t.Errorf("恢复后桶数 = %d want %d", got.SeriesBuckets, want.SeriesBuckets)
	}
	assertSameStat(t, "恢复后的累计值", want.Total, got.Total)
	if len(got.Models) != len(want.Models) {
		t.Fatalf("恢复后模型数 = %d want %d", len(got.Models), len(want.Models))
	}
	for i := range want.Models {
		assertSameStat(t, "恢复后的模型 "+want.Models[i].Model, want.Models[i], got.Models[i])
	}

	// 趋势也要能跨重启读出来。
	res := MetricsRange(RangeQuery{Interval: IntervalHour})
	if len(res.Points) != 2 {
		t.Errorf("恢复后点数 = %d want 2", len(res.Points))
	}
	assertSameStat(t, "恢复后的区间汇总", want.Total, res.Total)
}

func TestSeriesLoadCorruptFileStartsEmpty(t *testing.T) {
	resetSeriesForTest(t)
	file := filepath.Join(t.TempDir(), "metrics.json")
	if err := os.WriteFile(file, []byte("{ 这不是 JSON"), 0o600); err != nil {
		t.Fatalf("写坏文件: %v", err)
	}
	setSeriesFileForTest(t, file)

	// 坏文件不该让网关起不来：静默从零开始。
	globalSeries.load()

	if got := SeriesBuckets(); got != 0 {
		t.Errorf("坏文件恢复后桶数 = %d want 0", got)
	}
	if got := MetricsSnapshotOf().Total.Requests; got != 0 {
		t.Errorf("坏文件恢复后请求数 = %d want 0", got)
	}
}

func TestSeriesLoadMissingFileStartsEmpty(t *testing.T) {
	resetSeriesForTest(t)
	setSeriesFileForTest(t, filepath.Join(t.TempDir(), "nope.json"))

	globalSeries.load() // 首次运行：文件不存在

	if got := SeriesBuckets(); got != 0 {
		t.Errorf("文件缺失时桶数 = %d want 0", got)
	}
}

// ── 重置 ──────────────────────────────────────────────────────────────────

func TestResetMetricsClearsSeries(t *testing.T) {
	resetSeriesForTest(t)
	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, time.Now())

	if got := SeriesBuckets(); got != 1 {
		t.Fatalf("前置桶数 = %d want 1", got)
	}
	ResetMetrics()

	// 只清累计值会让趋势图还留着旧数据，两张卡片对不上。
	if got := SeriesBuckets(); got != 0 {
		t.Errorf("重置后桶数 = %d want 0", got)
	}
	if got := MetricsSnapshotOf().Total.Requests; got != 0 {
		t.Errorf("重置后请求数 = %d want 0", got)
	}
}

// ── handler 参数解析（面板对接契约）───────────────────────────────────────

// TestParseRangeQuery 覆盖 /v1/stats 的时间参数解析。
//
// 这条路径决定响应里到底有没有 range 字段——面板正是按「有无 range」决定
// 画趋势图还是显示降级提示，所以参数解析是面板对接的实际契约面。
func TestParseRangeQuery(t *testing.T) {
	midnight := func(offsetDays int) time.Time {
		n := time.Now()
		return time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, offsetDays)
	}

	t.Run("无时间参数只返回累计统计", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats"))
		if ok {
			t.Errorf("ok = true want false（无参数时不该返回 range）")
		}
		if q.Interval != IntervalHour {
			t.Errorf("interval = %q want hour（缺省粒度）", q.Interval)
		}
	})

	t.Run("interval 单独出现不算时间查询", func(t *testing.T) {
		if _, ok := parseRangeQuery(newReq(t, "/v1/stats?interval=day")); ok {
			t.Errorf("ok = true want false（只有 interval 不足以构成时间查询）")
		}
	})

	t.Run("all 不限边界", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?range=all"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		if !q.From.IsZero() || !q.To.IsZero() {
			t.Errorf("from/to = %v/%v want 零值（全部保留期）", q.From, q.To)
		}
	})

	t.Run("today 覆盖当天零点到次日零点", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?range=today"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		if !q.From.Equal(midnight(0)) || !q.To.Equal(midnight(1)) {
			t.Errorf("from/to = %v/%v want %v/%v", q.From, q.To, midnight(0), midnight(1))
		}
	})

	t.Run("yesterday 覆盖昨天零点到今天零点", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?range=yesterday"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		if !q.From.Equal(midnight(-1)) || !q.To.Equal(midnight(0)) {
			t.Errorf("from/to = %v/%v want %v/%v", q.From, q.To, midnight(-1), midnight(0))
		}
	})

	t.Run("7d 是相对区间", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?range=7d"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		want := time.Now().AddDate(0, 0, -7)
		if diff := q.From.Sub(want); diff > time.Minute || diff < -time.Minute {
			t.Errorf("from = %v want ≈%v", q.From, want)
		}
		if !q.To.IsZero() {
			t.Errorf("to = %v want 零值（到现在为止）", q.To)
		}
	})

	t.Run("面板的自定义区间带毫秒", func(t *testing.T) {
		// 面板用 new Date(x).toISOString() 生成，形如 2026-09-20T02:00:00.000Z。
		// Go 的 time.RFC3339 解析允许秒后跟小数部分，这里锁住它不被改坏。
		q, ok := parseRangeQuery(newReq(t,
			"/v1/stats?from=2026-09-20T02:00:00.000Z&to=2026-09-21T02:00:00.000Z&interval=day"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		if q.From.UTC().Format(time.RFC3339) != "2026-09-20T02:00:00Z" {
			t.Errorf("from = %v want 2026-09-20T02:00:00Z", q.From.UTC())
		}
		if q.Interval != IntervalDay {
			t.Errorf("interval = %q want day", q.Interval)
		}
	})

	t.Run("绝对区间优先于相对区间", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t,
			"/v1/stats?range=today&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		if q.From.UTC().Year() != 2026 || q.From.UTC().Month() != time.January {
			t.Errorf("from = %v want 2026-01-01（显式区间应压过 range=today）", q.From.UTC())
		}
	})

	t.Run("非法时间静默忽略而不报错", func(t *testing.T) {
		// 统计是观测功能，宁可回落默认也不要 400 让面板整页失败。
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?from=不是时间"))
		if ok {
			t.Errorf("ok = true want false（非法 from 应被忽略）")
		}
		if !q.From.IsZero() {
			t.Errorf("from = %v want 零值", q.From)
		}
	})

	t.Run("非法粒度回落 hour", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?range=all&interval=每小时"))
		if !ok {
			t.Fatal("ok = false want true")
		}
		if q.Interval != IntervalHour {
			t.Errorf("interval = %q want hour", q.Interval)
		}
	})

	t.Run("只给 model 也构成时间查询", func(t *testing.T) {
		q, ok := parseRangeQuery(newReq(t, "/v1/stats?model=m1"))
		if !ok {
			t.Fatal("ok = false want true（面板切模型筛选时不该退化成累计统计）")
		}
		if q.Model != "m1" {
			t.Errorf("model = %q want m1", q.Model)
		}
	})
}

// newReq 造一个只带 URL 的请求（parseRangeQuery 只读 URL query）。
func newReq(t *testing.T, target string) *http.Request {
	t.Helper()
	return httptest.NewRequest(http.MethodGet, target, nil)
}

// ── 端到端：面板实际收到的 JSON ──────────────────────────────────────────

// TestStatsEndpointExposesRange 走完整 HTTP 路由，锁住面板看到的响应形状。
//
// 前面几个用例测的是内部函数；这一条测的是**面板真正拿到的那份 JSON**——
// 面板按「有无 range 字段」决定画趋势图还是显示降级提示，所以这个契约必须
// 端到端钉住，而不是只信内部结构体。
func TestStatsEndpointExposesRange(t *testing.T) {
	resetMetricsForTest(t)
	resetSeriesForTest(t)

	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, fullFieldsModelsBody, false
	})
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: up, GlobalEnabled: false, APIKey: "k"})

	recordChatMetricAt(mkStat("m1", 200, 100), 5*time.Second, time.Now())

	get := func(target string) map[string]any {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Header.Set("Authorization", "Bearer k")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s → %d: %s", target, rec.Code, rec.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("解析 GET %s 响应: %v", target, err)
		}
		return out
	}

	// 带时间参数：range + series_buckets 齐备，数据点带 stats 与 derived。
	withRange := get("/v1/stats?range=today&interval=hour")
	if _, ok := withRange["series_buckets"]; !ok {
		t.Errorf("响应缺 series_buckets（面板「可回溯 N 个时间桶」依赖它）")
	}
	rg, ok := withRange["range"].(map[string]any)
	if !ok {
		t.Fatalf("响应缺 range 字段（面板据此决定画图还是降级）: %v", withRange)
	}
	if rg["interval"] != "hour" {
		t.Errorf("range.interval = %v want hour", rg["interval"])
	}
	pts, ok := rg["points"].([]any)
	if !ok || len(pts) == 0 {
		t.Fatalf("range.points 为空: %v", rg["points"])
	}
	p0, ok := pts[0].(map[string]any)
	if !ok {
		t.Fatalf("数据点不是对象: %T", pts[0])
	}
	for _, k := range []string{"key", "start", "end", "stats", "derived"} {
		if _, ok := p0[k]; !ok {
			t.Errorf("数据点缺 %q 字段", k)
		}
	}

	// 不带时间参数：range 必须被 omitempty 省略，响应形状与旧版逐字段一致。
	plain := get("/v1/stats")
	if _, ok := plain["range"]; ok {
		t.Errorf("不带时间参数时不应返回 range（omitempty 失效）")
	}
	for _, k := range []string{"enabled", "since", "now", "uptime_sec", "total", "models"} {
		if _, ok := plain[k]; !ok {
			t.Errorf("累计统计响应缺 %q 字段", k)
		}
	}
}

// TestPruneIfDueRunsWhenIdle 后台循环也要清理过期桶。
//
// record 只在**有请求**时触发清理：网关空闲（夜里没人用、或账号全挂了）时
// 过期桶会一直躺在内存和磁盘里。刷盘循环每分钟醒一次，顺手清一下成本为零。
func TestPruneIfDueRunsWhenIdle(t *testing.T) {
	resetSeriesForTest(t)
	now := time.Now()

	putBucketForTest(t, now.Add(-31*24*time.Hour), "m1", 1)
	if got := SeriesBuckets(); got != 1 {
		t.Fatalf("前置桶数 = %d want 1", got)
	}

	// 距上次清理不足一小时 → 不动（节流）。
	setLastPrune(t, now.Add(-10*time.Minute))
	globalSeries.pruneIfDue(now)
	if got := SeriesBuckets(); got != 1 {
		t.Errorf("节流失效：10 分钟前刚清过又清了（桶数 = %d）", got)
	}

	// 距上次清理超过一小时 → 清理。
	setLastPrune(t, now.Add(-2*time.Hour))
	globalSeries.pruneIfDue(now)
	if got := SeriesBuckets(); got != 0 {
		t.Errorf("空闲清理未生效：桶数 = %d want 0", got)
	}
	// 清理必须标脏，否则删除不落盘，重启后过期桶又回来了。
	if !dirtyForTest() {
		t.Errorf("清理后 dirty = false，删除不会落盘")
	}
}

// TestInitMetricsClampsRetention InitMetrics 直接调用时也钳制保留期（不经过 config）。
func TestInitMetricsClampsRetention(t *testing.T) {
	resetSeriesForTest(t)
	InitMetrics("", 3650) // 十年：应被钳到 MaxRetentionDays

	globalSeries.mu.Lock()
	got := globalSeries.retention
	globalSeries.mu.Unlock()

	want := time.Duration(MaxRetentionDays) * 24 * time.Hour
	if got != want {
		t.Errorf("retention = %v want %v（钳到上限）", got, want)
	}
}

// setLastPrune 拨动清理节流的时间戳。
func setLastPrune(t *testing.T, at time.Time) {
	t.Helper()
	ss := globalSeries
	ss.mu.Lock()
	ss.lastPrune = at
	ss.mu.Unlock()
}

// dirtyForTest 读落盘脏标志。
func dirtyForTest() bool {
	ss := globalSeries
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.dirty
}
