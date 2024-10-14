package common

func ToBytesSlice(s []string) [][]byte {
	if s == nil {
		return nil
	}
	res := make([][]byte, 0, len(s))
	for _, v := range s {
		res = append(res, []byte(v))
	}
	return res
}

func ToStringSlice(s [][]byte) []string {
	if s == nil {
		return nil
	}
	res := make([]string, 0, len(s))
	for _, v := range s {
		res = append(res, string(v))
	}
	return res
}

func ToBytesSlice2d(s [][]string) [][][]byte {
	if s == nil {
		return nil
	}
	res := make([][][]byte, 0, len(s))
	for _, v := range s {
		res = append(res, ToBytesSlice(v))
	}
	return res
}

func ToStringSlice2d(s [][][]byte) [][]string {
	if s == nil {
		return nil
	}
	res := make([][]string, 0, len(s))
	for _, v := range s {
		res = append(res, ToStringSlice(v))
	}
	return res
}
