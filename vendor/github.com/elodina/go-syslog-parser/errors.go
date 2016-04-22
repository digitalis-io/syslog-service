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

package parser

import "errors"

var (
	// ErrEmptyMessage is returned when trying to parse an empty slice or string.
	ErrEmptyMessage = errors.New("Message cannot be empty")

	// ErrInvalidPriorityStart is returned when PRI part of the syslog message does not start with "<" character.
	ErrInvalidPriorityStart = errors.New("Invalid priority start character")

	// ErrPriTooLong is returned when PRI part of the syslog message exceeds 5 characters (as per RFC 3164 -
	// "The PRI part MUST have three, four, or five characters and will be bound with angle brackets as the first and last characters.")
	ErrPriTooLong = errors.New("PRI part too long")

	// ErrPriTooShort is returned when PRI part of the syslog message is too short.
	ErrPriTooShort = errors.New("PRI part too short")

	// ErrPriNotDigit is returned when the value enclosed in angle brackets contains a non-digit character.
	ErrPriNotDigit = errors.New("PRI value contains non-digit character")

	// ErrPriEndMissing is returned when PRI part of the syslog message does not end with ">" character.
	ErrPriEndMissing = errors.New("PRI end character missing")

	// ErrUnknownTimestampFormat is returned when the parser fails to parse timestamp part of the syslog message
	// using provided timestamp formats.
	ErrUnknownTimestampFormat = errors.New("Unknown timestamp format")

	// ErrPIDEndMissing is returned when PID part of the syslog message does not end with "]" character.
	ErrPIDEndMissing = errors.New("PID end character missing")

	// ErrPIDTooShort is returned when the value enclosed in square brackets is empty.
	ErrPIDTooShort = errors.New("PID part too short")

	// ErrPIDNotDigit is returned when the value enclosed in square brackets contains a non-digit character.
	ErrPIDNotDigit = errors.New("PID value contains non-digit character")
)
