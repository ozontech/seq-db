package main

import (
	"math"
	"net/netip"
	"strconv"
	"strings"
)

const signBit = uint64(1) << 63

type tokenClass uint8

const (
	classString tokenClass = iota
	classInt64
	classDigits
	classDecimal
	classIPv4
	classIPv6
	classUUID
	classHex
	classCount
)

var classNamesTable = [classCount]string{
	classString:  "string",
	classInt64:   "int64",
	classDigits:  "digits",
	classDecimal: "decimal",
	classIPv4:    "ipv4",
	classIPv6:    "ipv6",
	classUUID:    "uuid",
	classHex:     "hex",
}

func (c tokenClass) String() string {
	return classNamesTable[c]
}

func classNames() []string {
	return classNamesTable[:]
}

func classByName(name string) (tokenClass, bool) {
	for c, n := range classNamesTable {
		if n == name {
			return tokenClass(c), true
		}
	}
	return classString, false
}

type typedValue struct {
	param uint8
	hi    uint64
	lo    uint64
}

func (v typedValue) less(o typedValue) bool {
	if v.param != o.param {
		return v.param < o.param
	}
	if v.hi != o.hi {
		return v.hi < o.hi
	}
	return v.lo < o.lo
}

func classify(t string) tokenClass {
	c, _ := parse(t)
	return c
}

func parse(t string) (tokenClass, typedValue) {
	if len(t) == 0 {
		return classString, typedValue{}
	}
	if v, ok := parseInt64(t); ok {
		return classInt64, typedValue{hi: uint64(v) ^ signBit}
	}
	if v, ok := parseDigits(t); ok {
		return classDigits, typedValue{param: uint8(len(t)), hi: v}
	}
	if scale, m, ok := parseDecimal(t); ok {
		return classDecimal, typedValue{param: scale, hi: uint64(m) ^ signBit}
	}
	if hi, lo, ok := parseUUID(t); ok {
		return classUUID, typedValue{hi: hi, lo: lo}
	}
	if hi, lo, ok := parseHex(t); ok {
		return classHex, typedValue{param: uint8(len(t)), hi: hi, lo: lo}
	}
	if c, v, ok := parseIP(t); ok {
		return c, v
	}
	return classString, typedValue{}
}

func isDigits(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func parseInt64(t string) (int64, bool) {
	digits := strings.TrimPrefix(t, "-")
	if !isDigits(digits) || (digits[0] == '0' && len(digits) > 1) || t == "-0" {
		return 0, false
	}
	v, err := strconv.ParseInt(t, 10, 64)
	return v, err == nil
}

func parseDigits(t string) (uint64, bool) {
	if len(t) > 19 || !isDigits(t) {
		return 0, false
	}
	v, err := strconv.ParseUint(t, 10, 64)
	return v, err == nil
}

func parseDecimal(t string) (uint8, int64, bool) {
	neg := strings.HasPrefix(t, "-")
	intPart, fracPart, ok := strings.Cut(strings.TrimPrefix(t, "-"), ".")
	if !ok || !isDigits(intPart) || !isDigits(fracPart) {
		return 0, 0, false
	}
	if intPart[0] == '0' && len(intPart) > 1 {
		return 0, 0, false
	}
	if len(intPart)+len(fracPart) > 18 {
		return 0, 0, false
	}
	m, err := strconv.ParseInt(intPart+fracPart, 10, 64)
	if err != nil || (neg && m == 0) {
		return 0, 0, false
	}
	if neg {
		m = -m
	}
	return uint8(len(fracPart)), m, true
}

func hexNibble(c byte) (uint64, bool) {
	switch {
	case c >= '0' && c <= '9':
		return uint64(c - '0'), true
	case c >= 'a' && c <= 'f':
		return uint64(c-'a') + 10, true
	}
	return 0, false
}

func parseHexUint64(s string) (uint64, bool) {
	var v uint64
	for i := 0; i < len(s); i++ {
		n, ok := hexNibble(s[i])
		if !ok {
			return 0, false
		}
		v = v<<4 | n
	}
	return v, true
}

func parseHex(t string) (uint64, uint64, bool) {
	if len(t) > 32 {
		return 0, 0, false
	}
	hasDigit, hasLetter := false, false
	for i := 0; i < len(t); i++ {
		c := t[i]
		hasDigit = hasDigit || (c >= '0' && c <= '9')
		hasLetter = hasLetter || (c >= 'a' && c <= 'f')
	}
	if !hasDigit || !hasLetter {
		return 0, 0, false
	}
	if len(t) <= 16 {
		v, ok := parseHexUint64(t)
		return v, 0, ok
	}
	split := len(t) - 16
	hi, ok := parseHexUint64(t[:split])
	if !ok {
		return 0, 0, false
	}
	lo, ok := parseHexUint64(t[split:])
	return hi, lo, ok
}

func parseUUID(t string) (uint64, uint64, bool) {
	if len(t) != 36 || t[8] != '-' || t[13] != '-' || t[18] != '-' || t[23] != '-' {
		return 0, 0, false
	}
	hi, ok1 := parseHexUint64(t[0:8] + t[9:13] + t[14:18])
	lo, ok2 := parseHexUint64(t[19:23] + t[24:36])
	return hi, lo, ok1 && ok2
}

func parseIP(t string) (tokenClass, typedValue, bool) {
	if strings.IndexByte(t, '.') < 0 && strings.IndexByte(t, ':') < 0 {
		return classString, typedValue{}, false
	}
	addr, err := netip.ParseAddr(t)
	if err != nil || addr.Zone() != "" || addr.String() != t {
		return classString, typedValue{}, false
	}
	b := addr.As16()
	hi, lo := beUint64(b[:8]), beUint64(b[8:])
	if addr.Is4() {
		return classIPv4, typedValue{hi: lo & math.MaxUint32}, true
	}
	return classIPv6, typedValue{hi: hi, lo: lo}, true
}

func beUint64(b []byte) uint64 {
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v
}

func isWide(c tokenClass, v typedValue) bool {
	switch c {
	case classIPv6, classUUID:
		return true
	case classHex:
		return v.param > 16
	}
	return false
}
