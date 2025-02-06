package main

import (
	"fmt"
	"maps"
	"slices"
	"unicode"
	"unicode/utf8"
)

func same(c uint, cs string) bool {
	lo, up := make([]byte, 4), make([]byte, 4)

	upper := rune(c)
	lower := unicode.To(unicode.LowerCase, upper)

	loWid, upWid := utf8.EncodeRune(up, upper), utf8.EncodeRune(lo, lower)
	if loWid != upWid {
		fmt.Printf("| %c | %c | %U | %U | %d | %d | %s |\n", upper, lower, upper, lower, upWid, loWid, cs)
		return false
	}

	return true
}

func main() {
	fmt.Printf("| upper | lower | unicode upper | unicode lower | width upper | width lower | case range |\n")
	fmt.Printf("| - | - | - | - | - | - | - |\n")
	for _, cs := range slices.Sorted(maps.Keys(unicode.Scripts)) {
		rt := unicode.Scripts[cs]
		for _, r16 := range rt.R16 {
			for c := r16.Lo; c <= r16.Hi; c += r16.Stride {
				same(uint(c), cs)
			}
		}
		for _, r32 := range rt.R32 {
			for c := r32.Lo; c <= r32.Hi; c += r32.Stride {
				same(uint(c), cs)
			}
		}
	}
}
