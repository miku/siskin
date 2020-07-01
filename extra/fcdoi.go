package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/miku/parallel"
	"github.com/sethgrid/pester"
)

var (
	doiKey     = flag.String("k", "doi", "key to doi")
	numWorkers = flag.Int("w", 32, "number of workers")
)

func main() {
	flag.Parse()
	if len(*doiKey) == 0 {
		log.Fatal("invalid key")
	}
	// https://api.fatcat.wiki/redoc#operation/lookup_release
	pp := parallel.NewProcessor(os.Stdin, os.Stdout, func(p []byte) ([]byte, error) {
		var (
			v      = strings.TrimSpace(string(p))
			doi    string
			ok     bool
			bag    = make(map[string]interface{})
			result Result
		)
		if strings.HasPrefix(v, "{") {
			if err := json.Unmarshal(p, &bag); err != nil {
				log.Printf("[skip] decode failed: %v", err)
				return nil, nil
			}
		} else {
			bag[*doiKey] = v
		}
		if doi, ok = bag[*doiKey].(string); !ok {
			log.Printf("[skip] no doi found")
			return nil, nil
		}
		result.DOI = doi
		link := fmt.Sprintf("https://api.fatcat.wiki/v0/release/lookup?expand=files&doi=%s", doi)
		var rlr ReleaseLookupResponse
		resp, err := pester.Get(link)
		if err != nil {
			log.Printf("[skip] request to %s failed with %v", link, err)
			return nil, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Printf("%v on %s", resp.Status, link)
			return nil, nil
		}
		if err := json.NewDecoder(resp.Body).Decode(&rlr); err != nil {
			log.Printf("[skip] decode failed: %v", err)
			return nil, nil
		}
		result.Files = rlr.Files
		result.Payload = bag
		b, err := json.Marshal(result)
		if err != nil {
			log.Printf("[skip] encode failed: %v", err)
			return nil, nil
		}
		b = append(b, []byte("\n")...)
		return b, err
	})
	pp.NumWorkers = *numWorkers
	pp.BatchSize = 100
	if err := pp.Run(); err != nil {
		log.Fatal(err)
	}
}

type Result struct {
	DOI   string `json:"doi"`
	Files []struct {
		Ident      string   `json:"ident"`
		Md5        string   `json:"md5"`
		Mimetype   string   `json:"mimetype"`
		ReleaseIds []string `json:"release_ids"`
		Revision   string   `json:"revision"`
		Sha1       string   `json:"sha1"`
		Sha256     string   `json:"sha256"`
		Size       int64    `json:"size"`
		State      string   `json:"state"`
		Urls       []struct {
			Rel string `json:"rel"`
			Url string `json:"url"`
		} `json:"urls"`
	} `json:"files"`
	Payload map[string]interface{} `json:"payload"`
}

// ReleaseLookupResponse is returned by v0 FC API.
type ReleaseLookupResponse struct {
	Abstracts   []interface{} `json:"abstracts"`
	ContainerId string        `json:"container_id"`
	Contribs    []struct {
		Extra struct {
			Seq string `json:"seq"`
		} `json:"extra"`
		Index   int64  `json:"index"`
		RawName string `json:"raw_name"`
		Role    string `json:"role"`
	} `json:"contribs"`
	ExtIds struct {
		Doi string `json:"doi"`
	} `json:"ext_ids"`
	Extra struct {
		Crossref struct {
			Type string `json:"type"`
		} `json:"crossref"`
	} `json:"extra"`
	Files []struct {
		Ident      string   `json:"ident"`
		Md5        string   `json:"md5"`
		Mimetype   string   `json:"mimetype"`
		ReleaseIds []string `json:"release_ids"`
		Revision   string   `json:"revision"`
		Sha1       string   `json:"sha1"`
		Sha256     string   `json:"sha256"`
		Size       int64    `json:"size"`
		State      string   `json:"state"`
		Urls       []struct {
			Rel string `json:"rel"`
			Url string `json:"url"`
		} `json:"urls"`
	} `json:"files"`
	Ident        string        `json:"ident"`
	Issue        string        `json:"issue"`
	Pages        string        `json:"pages"`
	Publisher    string        `json:"publisher"`
	Refs         []interface{} `json:"refs"`
	ReleaseDate  string        `json:"release_date"`
	ReleaseStage string        `json:"release_stage"`
	ReleaseType  string        `json:"release_type"`
	ReleaseYear  int64         `json:"release_year"`
	Revision     string        `json:"revision"`
	State        string        `json:"state"`
	Title        string        `json:"title"`
	Volume       string        `json:"volume"`
	WorkId       string        `json:"work_id"`
}
