Go Syslog Parser
================

This package implements [RFC 3164](https://www.ietf.org/rfc/rfc3164.txt) syslog message parsing for Golang.

Installation
------------

```
# go get github.com/elodina/go-syslog-parser
```

Usage
-----

	import github.com/elodina/go-syslog-parser
	...
    
	syslog := parser.New(parser.TimestampFormats)
	message, err := syslog.ParseString(`<165>Apr 14 14:26:12 master logger[1693]: foobar`)
	if err != nil {
		panic(err)
	}
	...
	
Practice shows that sometimes you might want to slightly customize timestamp format yet be still able to parse messages. To provide parser with timestamp formats you expect use the argument to `parser.New()`. For example, if you expect format with micro or nanoseconds, you could use this:

	parser.New([]string{time.StampMicro, time.StampNano})
    
Please note that when providing multiple timestamp formats, they will be tried in the same order as provided, e.g. if you expect most messages to have microsecond precision and some messages with nanosecond precision it makes sense to list `time.StampMicro` first.

Performance
-----------

Measured on Core i5 3230M, 2.60GHz×4 running Ubuntu 15.04

```
BenchmarkParserFullMessage-4	 2000000	       654 ns/op	     144 B/op	       2 allocs/op
BenchmarkParserNoTagAndPID-4	 2000000	       630 ns/op	     144 B/op	       2 allocs/op
BenchmarkParserNoPID-4      	 2000000	       631 ns/op	     144 B/op	       2 allocs/op
BenchmarkParserMixed-4      	 2000000	       670 ns/op	     144 B/op	       2 allocs/op
```