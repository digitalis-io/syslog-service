/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

/* Package parser implements RFC 3164 syslog message parsing for Golang with a little bit customizable timestamp parsing
behavior.

Simple usage example would be:

    import github.com/elodina/go-syslog-parser
    ...

    syslog := parser.New(parser.TimestampFormats)
	message, err := syslog.ParseString(`<165>Apr 14 14:26:12 master logger[1693]: foobar`)
	if err != nil {
		panic(err)
	}

Practice shows that sometimes you might want to slightly customize timestamp format yet be still able to parse messages.
To provide parser with timestamp formats you expect use the argument to `parser.New()`.
For example, if you expect format with micro or nanoseconds, you could use this:

    parser.New([]string{time.StampMicro, time.StampNano})

Please note that when providing multiple timestamp formats, they will be tried in the same order as provided, e.g.
if you expect most messages to have microsecond precision and some messages with nanosecond precision it makes sense
to list `time.StampMicro` first.
*/

package parser

import "time"

const (
	priStart = '<'
	priEnd   = '>'
	pidStart = '['
	pidEnd   = ']'
)

// TimestampFormats is a slice of timestamp formats used in the provided order to parse the syslog message timestamp.
// If contains more than one value, parser will try all formats one-by-one until successfully parses the timestamp or
// reaches the end of this slice.
var TimestampFormats = []string{
	time.Stamp,
}

// Parser is the actual RFC 3164 syslog message parser.
type Parser struct {
	timestampFormats []string
}

// New returns a new RFC 3164 syslog message parser configured with provided timestamp formats.
func New(timestampFormats []string) *Parser {
	return &Parser{
		timestampFormats: timestampFormats,
	}
}

// Parse tries to parse the given byte slice. Returns a parsed syslog message and an error if it occurs.
func (p *Parser) Parse(in []byte) (*Message, error) {
	return p.ParseString(string(in))
}

// ParseString tries to parse the given string. Returns a parsed syslog message and an error if it occurs.
func (p *Parser) ParseString(in string) (*Message, error) {
	if in == "" {
		return nil, ErrEmptyMessage
	}

	position := 0
	p.skipSpaces(in, &position)

	priority, err := p.parsePriority(in, &position)
	if err != nil {
		return nil, err
	}

	p.skipSpaces(in, &position)

	timestamp, err := p.parseTimestamp(in, &position)
	if err != nil {
		return nil, err
	}

	p.skipSpaces(in, &position)
	hostname := p.parseHostname(in, &position)
	p.skipSpaces(in, &position)

	tag, shouldParsePID := p.parseTag(in, &position)
	p.skipSpaces(in, &position)

	pid := -1
	if shouldParsePID {
		pid, err = p.parsePID(in, &position)
		if err != nil {
			return nil, err
		}
		p.skipSpaces(in, &position)
	}

	message := p.parseMessage(in, &position)

	return &Message{
		Priority:  priority.priority,
		Severity:  priority.severity,
		Facility:  priority.facility,
		Timestamp: timestamp,
		Hostname:  hostname,
		Tag:       tag,
		PID:       pid,
		Message:   message,
	}, err
}

func (p *Parser) skipSpaces(in string, position *int) {
	if len(in) <= *position {
		return
	}

	for in[*position] == ' ' {
		*position++
	}
}

func (p *Parser) parsePriority(in string, position *int) (*priority, error) {
	if in[*position] != priStart {
		return nil, ErrInvalidPriorityStart
	}

	relativePosition := 1 // 1 because we already validated start character
	priorityValue := 0
	for relativePosition < len(in) {
		// according to RFC 3164 - "The PRI part MUST have three, four, or five characters and will be
		// bound with angle brackets as the first and last characters."
		if relativePosition == 5 {
			return nil, ErrPriTooLong
		}

		current := in[*position+relativePosition]
		if current == priEnd {
			if relativePosition == 1 {
				return nil, ErrPriTooShort
			}

			*position += relativePosition + 1
			return newPriority(priorityValue), nil
		}

		if !isDigit(current) {
			return nil, ErrPriNotDigit
		}

		priorityValue = (priorityValue * 10) + int(current-'0')
		relativePosition++
	}

	return nil, ErrPriEndMissing
}

func (p *Parser) parseTimestamp(in string, position *int) (time.Time, error) {
	for _, format := range p.timestampFormats {
		if *position+len(format) > len(in) {
			continue
		}

		timestamp, err := time.Parse(format, in[*position:*position+len(format)])
		if err == nil {
			*position += len(format)
			return timestamp, nil
		}
	}

	return time.Time{}, ErrUnknownTimestampFormat
}

func (p *Parser) parseHostname(in string, position *int) string {
	start := *position
	var end int

	// return anything until next space
	for end = *position; end < len(in); end++ {
		if in[end] == ' ' {
			break
		}
	}

	*position = end
	return in[start:end]
}

func (p *Parser) parseTag(in string, position *int) (string, bool) {
	start := *position

	for *position < len(in) {
		current := in[*position]
		if current == '[' {
			return in[start:*position], true
		}

		if current == ':' {
			*position++
			return in[start : *position-1], false
		}

		*position++
	}

	// this is not a tag, rollback position and return empty tag
	*position = start
	return "", false
}

func (p *Parser) parsePID(in string, position *int) (int, error) {
	if in[*position] != pidStart {
		// PID missing, which is ok
		return -1, nil
	}

	relativePosition := 1 // 1 because we already validated start character
	pidValue := 0
	for relativePosition < len(in) {
		current := in[*position+relativePosition]
		if current == pidEnd {
			if relativePosition == 1 {
				return -1, ErrPIDTooShort
			}

			if len(in) > relativePosition+1 && in[*position+relativePosition+1] == ':' {
				*position++
			}

			*position += relativePosition + 1
			return pidValue, nil
		}

		if !isDigit(current) {
			return -1, ErrPIDNotDigit
		}

		pidValue = (pidValue * 10) + int(current-'0')
		relativePosition++
	}

	return -1, ErrPIDEndMissing
}

func (p *Parser) parseMessage(in string, position *int) string {
	if *position > len(in) {
		return ""
	}

	start := *position
	*position = len(in)
	return in[start:]
}

func isDigit(char byte) bool {
	return char >= '0' && char <= '9'
}

type priority struct {
	priority int
	severity int
	facility int
}

func newPriority(value int) *priority {
	return &priority{
		priority: value,
		severity: value % 8,
		facility: value / 8,
	}
}
