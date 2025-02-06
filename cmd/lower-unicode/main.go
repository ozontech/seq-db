package main

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

func same(c uint) bool {
	lo, up := make([]byte, 4), make([]byte, 4)

	upper := rune(c)
	lower := unicode.To(unicode.LowerCase, upper)

	loWid, upWid := utf8.EncodeRune(up, upper), utf8.EncodeRune(lo, lower)
	if loWid != upWid {
		fmt.Printf("%c - %c\t%U - %U\t %d - %d\t", upper, lower, upper, lower, upWid, loWid)
		return false
	}

	return true
}

func main() {
	for cs, rt := range unicode.Scripts {
		for _, r16 := range rt.R16 {
			for c := r16.Lo; c <= r16.Hi; c += r16.Stride {
				if !same(uint(c)) {
					fmt.Println(cs)
				}
			}
		}
		for _, r32 := range rt.R32 {
			for c := r32.Lo; c <= r32.Hi; c += r32.Stride {
				if !same(uint(c)) {
					fmt.Println(cs)
				}
			}
		}
	}
}
