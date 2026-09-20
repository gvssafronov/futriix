/*
 * Copyright 2026 Safronov Grigorii
 *
 * Licensed under the CDDL, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * https://opensource.org/licenses/CDDL-1.0
 */

// Файл: internal/metrics/prometheus.go
// Назначение: Экспорт метрик в формате Prometheus (text exposition format)
// без внешних зависимостей. Совместимо с Linux и OpenIndiana (illumos).
// Поддерживает типы: counter, gauge, histogram (упрощённый).
//
// ИСПРАВЛЕНО: вместо небезопасного приведения через unsafePointer
// используются стандартные math.Float64bits / math.Float64frombits,
// что полностью убирает зависимость от пакета unsafe и работает
// одинаково на всех платформах.

package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MetricType тип метрики
type MetricType string

const (
	CounterType   MetricType = "counter"
	GaugeType     MetricType = "gauge"
	HistogramType MetricType = "histogram"
	SummaryType   MetricType = "summary"
)

// LabelPair пара ключ-значение
type LabelPair struct {
	Name  string
	Value string
}

// MetricFamily семейство метрик с одинаковым именем
type MetricFamily struct {
	Name    string
	Help    string
	Type    MetricType
	mu      sync.RWMutex
	metrics map[string]*Metric // key: сериализованные labels
}

// Metric конкретная метрика с набором labels
type Metric struct {
	Labels  []LabelPair
	Value   atomic.Uint64 // для counter/gauge храним float64 как биты
	ValueF  atomic.Value  // float64
	Buckets []float64     // для histogram
	Counts  []atomic.Uint64
	Sum     atomic.Uint64 // float64 как биты
	Count   atomic.Uint64
}

// Registry реестр метрик
type Registry struct {
	families sync.Map // map[string]*MetricFamily
	start    time.Time
}

var defaultRegistry = NewRegistry()

// NewRegistry создаёт новый реестр метрик
func NewRegistry() *Registry {
	return &Registry{start: time.Now()}
}

// DefaultRegistry возвращает глобальный реестр
func DefaultRegistry() *Registry { return defaultRegistry }

// RegisterCounter регистрирует counter-метрику
func (r *Registry) RegisterCounter(name, help string) *MetricFamily {
	return r.register(name, help, CounterType)
}

// RegisterGauge регистрирует gauge-метрику
func (r *Registry) RegisterGauge(name, help string) *MetricFamily {
	return r.register(name, help, GaugeType)
}

// RegisterHistogram регистрирует histogram-метрику
func (r *Registry) RegisterHistogram(name, help string, buckets []float64) *MetricFamily {
	mf := r.register(name, help, HistogramType)
	if len(buckets) == 0 {
		buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	}
	mf.mu.Lock()
	mf.metrics["__buckets__"] = &Metric{Buckets: buckets}
	mf.mu.Unlock()
	return mf
}

func (r *Registry) register(name, help string, t MetricType) *MetricFamily {
	if mf, ok := r.families.Load(name); ok {
		return mf.(*MetricFamily)
	}
	mf := &MetricFamily{
		Name:    name,
		Help:    help,
		Type:    t,
		metrics: make(map[string]*Metric),
	}
	actual, _ := r.families.LoadOrStore(name, mf)
	return actual.(*MetricFamily)
}

func labelsKey(labels []LabelPair) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := make([]LabelPair, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	for i, l := range sorted {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
	}
	return b.String()
}

// WithLabels возвращает метрику с указанными labels (создаёт при необходимости)
func (mf *MetricFamily) WithLabels(labels ...LabelPair) *Metric {
	key := labelsKey(labels)
	mf.mu.RLock()
	if m, ok := mf.metrics[key]; ok {
		mf.mu.RUnlock()
		return m
	}
	mf.mu.RUnlock()

	mf.mu.Lock()
	defer mf.mu.Unlock()
	if m, ok := mf.metrics[key]; ok {
		return m
	}
	m := &Metric{Labels: labels}
	m.ValueF.Store(float64(0))
	if mf.Type == HistogramType {
		if bm, ok := mf.metrics["__buckets__"]; ok {
			m.Buckets = bm.Buckets
			m.Counts = make([]atomic.Uint64, len(m.Buckets))
		}
	}
	mf.metrics[key] = m
	return m
}

// Inc увеличивает counter на 1
func (m *Metric) Inc() { m.Add(1) }

// Add добавляет delta к значению
func (m *Metric) Add(delta float64) {
	for {
		old := m.ValueF.Load()
		var oldF float64
		if old != nil {
			oldF = old.(float64)
		}
		if m.ValueF.CompareAndSwap(old, oldF+delta) {
			return
		}
	}
}

// Set устанавливает значение (для gauge)
func (m *Metric) Set(v float64) {
	m.ValueF.Store(v)
}

// Observe добавляет наблюдение в histogram
func (m *Metric) Observe(v float64) {
	if m.Buckets == nil {
		return
	}
	for i, b := range m.Buckets {
		if v <= b {
			m.Counts[i].Add(1)
		}
	}
	m.Count.Add(1)
	for {
		old := m.Sum.Load()
		oldF := float64FromBits(old)
		newF := oldF + v
		if m.Sum.CompareAndSwap(old, float64ToBits(newF)) {
			break
		}
	}
}

// float64ToBits и float64FromBits — стандартные обёртки над math.
// Используются для атомарного хранения float64 в atomic.Uint64.
func float64ToBits(f float64) uint64 {
	return math.Float64bits(f)
}

func float64FromBits(b uint64) float64 {
	return math.Float64frombits(b)
}

// WriteTo записывает метрики в формате Prometheus text exposition
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	var total int64
	var writeErr error

	write := func(s string) {
		if writeErr != nil {
			return
		}
		n, err := io.WriteString(w, s)
		total += int64(n)
		if err != nil {
			writeErr = err
		}
	}

	// Process metrics
	write("# HELP futriis_up Whether futriis is up (1) or not (0)\n")
	write("# TYPE futriis_up gauge\n")
	write("futriis_up 1\n")

	write("# HELP futriis_uptime_seconds Uptime in seconds\n")
	write("# TYPE futriis_uptime_seconds gauge\n")
	write(fmt.Sprintf("futriis_uptime_seconds %.3f\n", time.Since(r.start).Seconds()))

	// Сортируем имена семейств для детерминированного вывода
	names := make([]string, 0)
	r.families.Range(func(k, v interface{}) bool {
		names = append(names, k.(string))
		return true
	})
	sort.Strings(names)

	for _, name := range names {
		mfVal, _ := r.families.Load(name)
		mf := mfVal.(*MetricFamily)

		write(fmt.Sprintf("# HELP %s %s\n", mf.Name, mf.Help))
		write(fmt.Sprintf("# TYPE %s %s\n", mf.Name, mf.Type))

		mf.mu.RLock()
		keys := make([]string, 0, len(mf.metrics))
		for k := range mf.metrics {
			if k == "__buckets__" {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			m := mf.metrics[k]
			labelStr := formatLabels(m.Labels)
			switch mf.Type {
			case CounterType, GaugeType:
				v := m.ValueF.Load()
				var f float64
				if v != nil {
					f = v.(float64)
				}
				write(fmt.Sprintf("%s%s %.6f\n", mf.Name, labelStr, f))
			case HistogramType:
				cum := uint64(0)
				for i, b := range m.Buckets {
					cum += m.Counts[i].Load()
					le := formatFloat(b)
					write(fmt.Sprintf("%s_bucket%s le=\"%s\"} %d\n", mf.Name, labelStrWithBrace(labelStr), le, cum))
				}
				write(fmt.Sprintf("%s_bucket%s le=\"+Inf\"} %d\n", mf.Name, labelStrWithBrace(labelStr), m.Count.Load()))
				sumF := float64FromBits(m.Sum.Load())
				write(fmt.Sprintf("%s_sum%s %.6f\n", mf.Name, labelStr, sumF))
				write(fmt.Sprintf("%s_count%s %d\n", mf.Name, labelStr, m.Count.Load()))
			}
		}
		mf.mu.RUnlock()
	}

	return total, writeErr
}

func formatLabels(labels []LabelPair) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteString("=\"")
		b.WriteString(escapeLabelValue(l.Value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// labelStrWithBrace превращает "{a=\"b\"}" в "{a=\"b\"," для добавления le="..."
func labelStrWithBrace(labelStr string) string {
	if labelStr == "" {
		return "{"
	}
	return labelStr[:len(labelStr)-1] + ","
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func formatFloat(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", f), "0"), ".")
}
