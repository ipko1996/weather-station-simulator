package events

// Event is the contract every message type on our Kafka topics fulfills:
// it can vouch for itself (Validate) and say which partition key it belongs
// under (Key). The producer needs nothing more — it validates, keys and
// marshals; the payload's concrete shape is the consumer's business.
//
// Satisfaction is implicit, Go's structural typing: SensorReading never
// declares "implements Event" — having the two methods IS implementing it.
// The var _ lines below are compile-time assertions making that visible: if
// a type ever drifts away from the contract, the build breaks here, with a
// clear error, instead of at some distant call site.
type Event interface {
	Validate() error
	Key() []byte
}

var _ Event = SensorReading{}
