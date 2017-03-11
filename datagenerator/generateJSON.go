package datagenerator

import "fmt"

type DataDetails struct {
	BulkSize     uint32
	IndexName    string
	NumberFields uint32
	StringFields uint32
}

func GetBulkJson(str <-chan *string, flt <-chan *string, indexNo uint32, details DataDetails) []byte {
	var request []byte
	for i := uint32(1); i <= details.BulkSize; i++ {
		jsonStr := genJson(str, flt, indexNo, details)
		request = append(request, jsonStr...)
	}
	return request
}

func genJson(str <-chan *string, flt <-chan *string, indexNo uint32, details DataDetails) []byte {
	header := getHeader(indexNo, details)
	content := getContent(str, flt, details)
	newline := []byte("\n")

	return append(append(append(header, newline...), content...), newline...)
}

func getHeader(indexNo uint32, details DataDetails) []byte {
	header := fmt.Sprintf(`{ "index" : { "_index" : "` + details.IndexName + fmt.Sprint(indexNo) + `","_type": "fact"} }`)
	return []byte(header)
}

func getContent(str <-chan *string, flt <-chan *string, details DataDetails) []byte {
	open, close := []byte("{ "), []byte("}")
	garbageFields := getGarbage(str, flt, details)
	return append(append(open, garbageFields...), close...)
	// return append(append(open, knownFields...), close...)
}

func getGarbage(str <-chan *string, flt <-chan *string, details DataDetails) []byte {
	var response []byte
	comma := []byte(",")

	for i := uint32(1); i <= details.StringFields; i++ {
		strValue := <-str
		dimension := fmt.Sprintf(`"Dimension%d":"%s",`, i, *strValue)
		response = append(response, []byte(dimension)...)
	}

	for i := uint32(1); i <= details.NumberFields; i++ {
		fltValue := <-flt
		measure := fmt.Sprintf(`"Measure%d":%s`, i, *fltValue)
		response = append(response, []byte(measure)...)
		if i == details.NumberFields {
			break
		}
		response = append(response, comma...)
	}
	return response
}
