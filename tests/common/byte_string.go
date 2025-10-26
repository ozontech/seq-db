package common

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
