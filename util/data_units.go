package util

import (
	"fmt"
	"strconv"
	"time"
)

func ParseDuration(esDuration string) (time.Duration, error) {
	if len(esDuration) < 2 {
		return 0, fmt.Errorf(`error parse inteval "%s"`, esDuration)
	}

	x := time.Duration(0)
	numStr := esDuration[:len(esDuration)-1]
	if esDuration[len(esDuration)-2] == 'm' {
		if esDuration[len(esDuration)-1] == 's' {
			x = time.Millisecond
		} else {
			return 0, fmt.Errorf(`error parse inteval "%s"`, esDuration)
		}
		numStr = esDuration[:len(esDuration)-2]
	} else {
		switch esDuration[len(esDuration)-1] {
		case 's':
			x = time.Second
		case 'm':
			x = time.Minute
		case 'h':
			x = time.Hour
		case 'd':
			x = time.Hour * 24
		case 'w':
			x = time.Hour * 24 * 7
		case 'M':
			x = time.Hour * 24 * 30
		case 'q':
			x = time.Hour * 24 * 91
		case 'y':
			x = time.Hour * 24 * 365
		default:
			return 0, fmt.Errorf(`error parse inteval "%s"`, esDuration)
		}
	}

	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}

	return x * time.Duration(num), nil
}
