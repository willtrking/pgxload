package pgxload

import "strconv"

// Simple way of rebinding a query using ? for arguments with $1, $2, etc. positional arguments
func RebindPositional(query string) string {

	qb := []byte(query)
	// Add space enough for 10 params before we have to allocate
	rqb := make([]byte, 0, len(qb)+10)
	j := 1
	for _, b := range qb {
		if b == '?' {
			rqb = append(rqb, '$')
			for _, b := range strconv.Itoa(j) {
				rqb = append(rqb, byte(b))
			}
			j++
		} else {
			rqb = append(rqb, b)
		}
	}

	return string(rqb)
}

