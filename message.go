package nsq

import (
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"time"
)

// The number of bytes for a Message.ID
const (
	MsgIDLength       = 16
	MsgMetaSizeLength = 2
)

// MessageID is the ASCII encoded hexadecimal message ID
type MessageID [MsgIDLength]byte

// Message is the fundamental data type containing
// the id, body, and metadata
type Message struct {
	ID        MessageID
	Meta      []byte
	Body      []byte
	Timestamp int64
	Attempts  uint16

	NSQDAddress string

	Delegate MessageDelegate

	autoResponseDisabled int32
	responded            int32
}

// NewMessage creates a Message, initializes some metadata,
// and returns a pointer
func NewMessage(id MessageID, meta, body []byte) *Message {
	return &Message{
		ID:        id,
		Meta:      meta,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// DisableAutoResponse disables the automatic response that
// would normally be sent when a handler.HandleMessage
// returns (FIN/REQ based on the error value returned).
//
// This is useful if you want to batch, buffer, or asynchronously
// respond to messages.
func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

// IsAutoResponseDisabled indicates whether or not this message
// will be responded to automatically
func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

// HasResponded indicates whether or not this message has been responded to
func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

// Finish sends a FIN command to the nsqd which
// sent this message
func (m *Message) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

// Touch sends a TOUCH command to the nsqd which
// sent this message
func (m *Message) Touch() {
	if m.HasResponded() {
		return
	}
	m.Delegate.OnTouch(m)
}

// Requeue sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// A delay of -1 will automatically calculate
// based on the number of attempts and the
// configured default_requeue_delay
func (m *Message) Requeue(delay time.Duration) {
	m.doRequeue(delay, true)
}

// RequeueWithoutBackoff sends a REQ command to the nsqd which
// sent this message, using the supplied delay.
//
// Notably, using this method to respond does not trigger a backoff
// event on the configured Delegate.
func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	m.doRequeue(delay, false)
}

func (m *Message) doRequeue(delay time.Duration, backoff bool) {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnRequeue(m, delay, backoff)
}

// WriteTo implements the WriterTo interface and serializes
// the message into the supplied producer.
//
// It is suggested that the target Writer is buffered to
// avoid performing many system calls.
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	// write meta size
	var metaSize [2]byte
	binary.BigEndian.PutUint16(metaSize[:], uint16(len(m.Meta)))
	n, err = w.Write(metaSize[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	if len(m.Meta) > 0 {
		// write meta body
		n, err = w.Write(m.Meta)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// DecodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (uint16) || (binary) || (binary)
// |       8-byte         ||    ||                 16-byte                      ||  2-byte  || (N-byte) ||  N-byte
// ---------------------------------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                      meta size    meta body    message body
//                        (uint16)
//                         2-byte
//                        attempts
func DecodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < 10+MsgIDLength {
		return nil, errors.New("not enough data to decode valid message")
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	metaSize := binary.BigEndian.Uint16(b[10+MsgIDLength : 10+MsgIDLength+MsgMetaSizeLength])
	if metaSize > 0 {
		msg.Meta = b[10+MsgIDLength+MsgMetaSizeLength : 10+MsgIDLength+MsgMetaSizeLength+metaSize]
	} else {
		msg.Meta = nil
	}
	msg.Body = b[10+MsgIDLength+MsgMetaSizeLength+metaSize:]

	return &msg, nil
}
