package checker

import (
	"sort"

	"github.com/juju/errors"
	"github.com/moiot/gravity/pkg/protocol/dcp"
	log "github.com/sirupsen/logrus"
)

type segment struct {
	tag         string
	startOffset uint64
	endOffset   uint64
	messages    map[string][]*dcp.Message //key is payload id
}

func newSegment(tag string, startOffset uint64, endOffset uint64, msgs []*dcp.Message) *segment {
	t := make(map[string][]*dcp.Message)

	for _, m := range msgs {
		payloadId := m.GetPayload().Id
		_, exists := t[payloadId]
		if !exists {
			t[payloadId] = make([]*dcp.Message, 0, 1)
		}
		t[payloadId] = append(t[payloadId], m)
	}

	for _, v := range t {
		sort.Slice(v, func(i, j int) bool {
			if v[i].Timestamp == v[j].Timestamp {
				return v[i].Id < v[j].Id
			}
			return v[i].Timestamp < v[j].Timestamp
		})
	}

	s := segment{
		tag,
		startOffset,
		endOffset,
		t,
	}

	log.Infof("new segment, tag=%s, startOffset=%d, msg cnt=%d", s.tag, s.startOffset, len(msgs))

	return &s
}

func (s *segment) equal(s2 *segment) bool {
	if s == s2 || s.tag == s2.tag || s.startOffset != s2.startOffset || s.endOffset != s2.endOffset {
		panic(errors.Errorf("can't compare segment %+v with %+v", s, s2))
	}

	if len(s.messages) != len(s2.messages) {
		return false
	}

	for id, m1 := range s.messages {
		m2, ok := s2.messages[id]
		if !ok || len(m1) != len(m2) {
			log.Error("length not equal ", m1, m2)
			return false
		}

		for i := 0; i < len(m1); i++ {
			if m1[i].GetChecksum() != m2[i].GetChecksum() {
				log.Error("checksum not equal, content: ", *m1[i].GetPayload(), *m2[i].GetPayload())
				return false
			}
		}
	}

	return true
}
