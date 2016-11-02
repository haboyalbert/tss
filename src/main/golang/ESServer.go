package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"github.com/ant0ine/go-json-rest/rest"
    elastic "gopkg.in/olivere/elastic.v3"
)

func main() {
    
	type result struct {
		content string
		city    string
	}
	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)
	api.SetApp(rest.AppSimple(func(w rest.ResponseWriter, r *rest.Request) {
		lat, err := strconv.ParseFloat(r.Form.Get("lat"), 64)
		lon, err := strconv.ParseFloat(r.Form.Get("lon"), 64)
		dist, err := r.Form.Get("dist")
		key, err := r.Form.Get("keyword")
        //TODO: need to add username/pass template in future
		client, err := NewClient(SetURL("http://abc:test@52.211.0.138:9200"))
		if err != nil {
			// Handle error
			panic(err)
		}

		distanceQuery := elastic.NewGeoDistanceQuery("location")
		distanceQuery = distanceQuery.Lat(lat)
		distanceQuery = distanceQuery.Lon(lon)
		distanceQuery = distanceQuery.Distance(dist)

		termQuery := elastic.NewTermQuery("content", keyword)
		boolQuery := elastic.NewBoolQuery()
		boolQuery = boolQuery.Must(elastic.NewMatchAllQuery())
		boolQuery = boolQuery.Filter(distanceQuery)
		boolQuery = boolQuery.Filter(termQuery)

		searchResult, err := client.Search().Index("us_large_cities").Type("city").Query(boolQuery).From(0).Size(100).Pretty(true).Do()

		if err != nil {
			// Handle error
			panic(err)
		}

		if searchResult.Hits.TotalHits > 0 {
			fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)

			// Iterate through results
			for _, hit := range searchResult.Hits.Hits {
				// hit.Index contains the name of the index

				var t result
				err := json.Unmarshal(*hit.Source, &t)
				if err != nil {
					panic(err)
				}

				// write back result
				w.WriteJson(map[string]string{"content": t.content, "city": t.city})

			}
		} else {
			// No hits
			w.WriteHeader(500)
		}

	}))
	log.Fatal(http.ListenAndServe(":8081", api.MakeHandler()))
}

