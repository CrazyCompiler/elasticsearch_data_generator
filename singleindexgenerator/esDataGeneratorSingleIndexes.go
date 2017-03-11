package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const indexName = "category"
const typeName = "fact"
const baseURL = "http://localhost:9200"
const bulkURL = baseURL + "/" + indexName + "/" + typeName + "/_bulk"

const mainThreads uint32 = 2 // number of times batch requests are made
const batchSize uint32 = 5 // number of concurrent bulk requests
const bulkSize uint32 = 100  // number of documents in a single bulk api call
const totalDocuments uint32 = mainThreads * batchSize * bulkSize // total number of documents generated

const dimensionCount uint32 = 1 // excluding two predefined dimensions
const measureCount uint32 = 1 // excluding two predefined measures
var stringFields = map[string]string{"primaryskuname":"LORATADINE", "zonename":"Zone", "channel":"channel", "itemType":"ItemType", "merchandisingDivision":"merchandisingDivision", "category":"category", "subCategory":"subCategory", "segment":"segment", "brand":"brand", "pricingClass":"pricingClass", "size":"size", "brandType":"brandType", "linkedBrand":"linkedBrand", "workFlowStatus":"workFlowStatus"}
var numberFields = []string{"currentprice", "suggestedprice", "volume@cp", "revenues@cp", "margine@cp", "suggestedChange", "suggestedChangePercentage", "volume@sp", "revenues@sp", "margine@sp", "volumeChange@sp", "volumeChange@spPercentage", "revenuesChange@sp", "revenuesChange@spPercentage", "marginChange@sp", "marginChange@spPercentage", "newPrice", "priceChange", "priceChangePercentage", "volume@np", "revenues@np", "margin@np", "volumeChange@np", "volumeChange@npPercentage", "revenuesChange@np", "marginChange@np", "marginChange@npPercentage", "revenuesChange@npPercentage", "outputVersion"}

func main() {
	var start = time.Now()

	doMappingOfStringFields()

	strCh := make(chan string)
	fltCh := make(chan string)

	go generateString(strCh)
	go generateFloat(fltCh)

	var wg sync.WaitGroup

	for i := uint32(0); i < mainThreads; i++ {
		fmt.Printf("Main Processing ")
		fmt.Println(i + 1)
		wg.Add(1)
		batch(i, &wg, strCh, fltCh)
	}

	wg.Wait()

	fmt.Println("Main done.")
	fmt.Println("\n\n****************** Summary ******************")
	fmt.Println("Documents processed: ", totalDocuments)
	fmt.Println("Time Taken = ", time.Since(start))
	fmt.Println("****************** Have a good day ******************\n\n")
}

func doMappingOfStringFields() {
	fmt.Println("Starting mapping of string fields")
	var jsonStr string = fmt.Sprintf(`{"mappings":{"%s":{"properties":{`, typeName);
	for k := range stringFields {
		jsonStr += fmt.Sprintf(`"%s":{"type":"text","fields":{"raw":{"type":"keyword"}}},`, k)
	}
	jsonStr = jsonStr[:len(jsonStr) - 1] + "}}}}"
	fmt.Println(jsonStr)

	mappingURL := baseURL + "/" + indexName
	req, err := http.NewRequest("PUT", mappingURL, bytes.NewBuffer([]byte(jsonStr)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	_, err = client.Do(req)
	if err != nil {
		fmt.Println("Error in mapping of string fields: ", err)
		return
	}
	fmt.Println("Completed mapping of string fields")
}

func batch(idx uint32, mg *sync.WaitGroup, str <- chan string, flt <- chan string) {
	var start = time.Now()
	var wg sync.WaitGroup

	for i := uint32(0); i < batchSize; i++ {

		fmt.Println("Batch Processing ", (i + 1))
		wg.Add(1)
		go post(idx, i, &wg, str, flt)
	}

	wg.Wait()
	mg.Done()
	fmt.Printf("Batch %d done. %.2f %% completed. Time Taken = %v\n", (idx + 1), ((float32(((idx + 1) * batchSize * bulkSize)) / float32(totalDocuments)) * 100.00), time.Since(start))
}

func post(mainIdx, batchIdx uint32, wg *sync.WaitGroup, str <-chan string, flt <-chan string) {

	var jsonStr = getBulkJson(str, flt)
	//fmt.Println("Message: ", string(jsonStr))

	req, _ := http.NewRequest("POST", bulkURL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	_, err := client.Do(req)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	//body, _ := ioutil.ReadAll(resp.Body)
	//fmt.Println("response Body:", string(body))

	wg.Done()

}

func getBulkJson(str <-chan string, flt <-chan string) []byte {
	var request []byte
	for i := uint32(1); i <= bulkSize; i++ {
		jsonStr := genJson(str, flt)
		request = append(request, jsonStr...)
	}
	return request
}

func genJson(str <-chan string, flt <-chan string) []byte {
	header := getHeader(str)
	content := getContent(str, flt)
	newline := []byte("\n")

	return append(append(append(header, newline...), content...), newline...)
}

func getHeader(str <-chan string) []byte {
	strValue := <-str
	header := fmt.Sprintf(`{ "index" : { "_index" : "category","_type": "fact", "_id": "%s" } }`, *strValue)
	return []byte(header)
}

func getContent(str <-chan string, flt <-chan string) []byte {
	open, close := []byte("{ "), []byte("}")
	var data string
	for _, field := range numberFields {
		data += fmt.Sprintf(`"%s":%s,`, field, <-flt)
	}

	for field, prefix := range stringFields {
		data += fmt.Sprintf(`"%s":"%s %s",`, field, prefix, <-str)
	}
	//fmt.Println(data)

	data = data[:len(data) - 1]

	knownFields := []byte(data)
	//garbageFields := getGarbage(str, flt)

	//return append(append(append(open, knownFields...), garbageFields...), close...)
	return append(append(open, knownFields...), close...)
}

func getGarbage(str <-chan string, flt <-chan string) []byte {
	var response []byte
	comma := []byte(",")

	for i := uint32(1); i <= dimensionCount; i++ {
		strValue := <-str
		dimension := fmt.Sprintf(`"Dimension%d":"%s",`, i, strValue)
		response = append(response, []byte(dimension)...)
	}

	for i := uint32(1); i <= measureCount; i++ {
		fltValue := <-flt
		measure := fmt.Sprintf(`"Measure%d":%s`, i, fltValue)
		response = append(response, []byte(measure)...)
		if i == measureCount {
			break;
		}
		response = append(response, comma...)
	}
	return response
}


/************* Random string generation logic **********/

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1 << letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n - 1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}



/************* Generator routines **********/

//generateString generates random string
func generateString(ch chan <- string) {
	for {
		ch <- RandStringBytesMaskImprSrc(10)
	}
}


//generateFloat generates random float value as string
func generateFloat(ch chan <- string) {
	for {
		ch <- strconv.FormatFloat((rand.Float64() * 100), 'f', 2, 64)
	}
}
