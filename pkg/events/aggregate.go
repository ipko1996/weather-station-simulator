package events

import (
	"errors"
	"fmt"
	"time"
)

// TopicWeatherAggregates carries windowed temperature summaries produced by
// the aggregation-service and fanned out to browsers by the
// notification-gateway. Aggregates are ephemeral display data — they
// regenerate every window, and TimescaleDB holds the durable history — which
// is why the topic's retention (see kafkax.AggregatesTopic) is short.
const TopicWeatherAggregates = "weather.aggregates"

// WindowAggregate summarizes one scope's temperature over one tumbling
// window. Scope is "national" for now; regional scopes (county codes) arrive
// with the map's clustering feature and need no schema change.
type WindowAggregate struct {
	Scope       string    `json:"scope"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	AvgTempC    float64   `json:"avg_temp_c"`
	MinTempC    float64   `json:"min_temp_c"`
	MaxTempC    float64   `json:"max_temp_c"`
	Count       int       `json:"count"` // readings in the window — 0-count windows are never emitted
}

var _ Event = WindowAggregate{}

// Sentinel errors, same design as the other events.
var (
	ErrMissingScope  = errors.New("scope is required")
	ErrInvalidWindow = errors.New("window end must be after start")
	ErrEmptyWindow   = errors.New("aggregate of zero readings")
)

// Key routes by scope: every "national" aggregate lands on the same
// partition, so any single consumer sees the national series in order — which
// is exactly what a browser redrawing a chart needs.
func (a WindowAggregate) Key() []byte {
	return []byte(a.Scope)
}

// Validate checks the aggregate's internal consistency before it's published.
func (a WindowAggregate) Validate() error {
	if a.Scope == "" {
		return ErrMissingScope
	}
	if !a.WindowEnd.After(a.WindowStart) {
		return fmt.Errorf("%w: start %s, end %s", ErrInvalidWindow, a.WindowStart, a.WindowEnd)
	}
	if a.Count <= 0 {
		return ErrEmptyWindow
	}
	if a.MinTempC > a.MaxTempC {
		return fmt.Errorf("min %.1f exceeds max %.1f", a.MinTempC, a.MaxTempC)
	}
	return nil
}
