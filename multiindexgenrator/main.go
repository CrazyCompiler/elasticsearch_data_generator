package main

import (
	d "elasticsearch_data_generator/datagenerator"
	r "elasticsearch_data_generator/randomdataprovider"

	"fmt"
	"net/http"
	"sync"
	"time"
)

const indexName = "category"
const typeName = "fact"
const baseURL = "http://localhost:9200"
const bulkURL = baseURL + "/_bulk"
const settingString = `{"refresh_interval" : "%s" }`

const noOfIndexes uint32 = 1                                     // number of indexes are created
const startingIndex uint32 = 0                                   // indexes starting from
const batchSize uint32 = 1                                       // number of concurrent bulk requests
const bulkSize uint32 = 1                                        // number of documents in a single bulk api call
const totalDocuments uint32 = noOfIndexes * batchSize * bulkSize // total number of documents generated

const stringFields uint32 = 29 // excluding two predefined dimensions
const numberFields uint32 = 28 // excluding two predefined measures

var tookTime int = 0

type ESResponse struct {
	Status int `status`
}

func main() {
	var start = time.Now()

	strCh := make(chan *string)
	fltCh := make(chan *string)

	go r.GenerateString(strCh)
	go r.GenerateFloat(fltCh)

	var wg sync.WaitGroup

	initialIndex := startingIndex

	for i := uint32(0); i < noOfIndexes; i++ {
		fmt.Printf("Main Processing ")
		fmt.Println(i + 1)
		wg.Add(1)
		batch(i, &wg, strCh, fltCh, initialIndex)
		initialIndex++
	}

	wg.Wait()

	_, _ = http.Get(baseURL + "/_refresh")
	fmt.Println("INFO:  Refreshing Elasticsearch ..")

	fmt.Println("Main done.")
	fmt.Println("\n\n****************** Summary ******************")
	fmt.Println("Documents processed: ", totalDocuments)
	fmt.Println("Network And Datageneration Time Taken = ", time.Since(start))
	fmt.Println("****************** Have a good day ******************\n\n")
}

func batch(idx uint32, mg *sync.WaitGroup, str <-chan *string, flt <-chan *string, initialIndex uint32) {
	var start = time.Now()
	var wg sync.WaitGroup

	for i := uint32(0); i < batchSize; i++ {

		fmt.Println("Batch Processing ", (i + 1))
		wg.Add(1)
		go post(initialIndex, i, &wg, str, flt)
	}

	wg.Wait()
	mg.Done()
	fmt.Printf("Batch %d done, inserted into index %d. %.2f %% completed. Time Taken = %v\n", (idx + 1), initialIndex, ((float32(((idx + 1) * batchSize * bulkSize)) / float32(totalDocuments)) * 100.00), time.Since(start))
}

func post(mainIdx, batchIdx uint32, wg *sync.WaitGroup, str <-chan *string, flt <-chan *string) {
	// respBody := ESResponse{}
	details := d.DataDetails{
		BulkSize:     bulkSize,
		IndexName:    indexName,
		NumberFields: numberFields,
		StringFields: stringFields,
	}
	var jsonStr = d.GetBulkJson(str, flt, mainIdx, details)
	fmt.Println("Message: ", string(jsonStr))

	// req, _ := http.NewRequest("POST", bulkURL, bytes.NewBuffer(jsonStr))
	// req.Header.Set("Content-Type", "application/json")
	//
	// client := &http.Client{}
	// resp, err := client.Do(req)
	// if err != nil {
	//     fmt.Println("Error: ", err)
	//     return
	// }
	//
	// body, _ := ioutil.ReadAll(resp.Body)
	// //fmt.Println("response Body:", string(body))
	// err = json.Unmarshal(body, &respBody)
	// if err != nil {
	//     fmt.Println("json parse error", err)
	//     return
	// }
	//
	// if respBody.Status > 300 {
	//     fmt.Println("error while updating mappings \n", string(body))
	// }
	//
	// timeTaken, _ := strconv.Atoi(strings.Split(strings.Split(string(body), ",")[0], ":")[1])
	// tookTime = tookTime + timeTaken
	/* defer resp.Body.Close() */
	wg.Done()

}
