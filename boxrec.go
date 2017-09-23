package main

import (
    // import standard libraries
    "fmt"
    "log"
		"strconv"
		"strings"
		"os"
		"encoding/csv"
    // import third party libraries
    "github.com/PuerkitoBio/goquery"
)

type Boxer struct {
	position uint64
	link string
	name string
	points uint64
	stars uint64
	division string
	age uint64
	won uint64
	lost uint64
	draw uint64
	last6 string
	stance string
	residence string
}

func boxerToStrings(b *Boxer) []string {
	result := make([]string, 13, 13)
	result[0] = strconv.FormatUint(b.position, 10)
	result[1] = b.link
	result[2] = b.name
	result[3] = strconv.FormatUint(b.points, 10)
	result[4] = strconv.FormatUint(b.stars, 10)
	result[5] = b.division
	result[6] = strconv.FormatUint(b.age, 10)
	result[7] = strconv.FormatUint(b.won, 10)
	result[8] = strconv.FormatUint(b.lost, 10)
	result[9] = strconv.FormatUint(b.draw, 10)
	result[10] = b.last6
	result[11] = b.stance
	result[12] = b.residence
	return result
}

func chomp(s string) string {
	return strings.Trim(s, " \n")
}

func strToUint32(s string) (uint64, error) {
  s = chomp(s)
	r, e := strconv.ParseUint(s, 10, 32)
	return r, e
}

func parseTd(root *goquery.Selection, boxers chan Boxer) {
	var b Boxer
	col := root.Find("td").First()
	//position
	b.position, _ = strToUint32(col.Text())
	//name
	col = col.Next()
	name := col.Find("a").First()
	b.name = chomp(name.Text())
	b.link, _ = name.Attr("href")
	//points
	col = col.Next()
	b.points, _ = strToUint32(col.Text())
	//stars
  col = col.Next()
	span := col.Find("div div span span").First()
  style, _ := span.Attr("style")
	width := style[strings.Index(style, "width:")+7:len(style)-2]
	b.stars, _ = strToUint32(width)
	//division
	col = col.Next()
  b.division = chomp(col.Text())
	//age
	col = col.Next()
	b.age, _ = strToUint32(col.Text())
	//W-L-D
	col = col.Next()
	wldspan := col.Find("div span").First()
	b.won, _ = strToUint32(wldspan.Text())
	wldspan = wldspan.Next()
	b.lost, _ = strToUint32(wldspan.Text())
	wldspan = wldspan.Next()
	b.draw, _ = strToUint32(wldspan.Text())
	last6 := col.Find(".last6").First();
	l6 := make([]byte, 6, 6)
	for i:=0; i<6; i++ {
		class, _ := last6.Attr("class")
		l6[i] = class[len(class)-1]
		last6 = last6.Next()
	}
	b.last6 = string(l6)
	//stance
	col = col.Next()
	b.stance = chomp(col.Text())
	//residence
	col = col.Next()
	residence := make([]string, 0, 3)
	col.Find("a").Each(func(index int, item *goquery.Selection){
		residence = append(residence, chomp(item.Text()))
	})
  b.residence = strings.Join(residence, ", ")
	boxers <- b
}

func crawl(url string, finished chan bool, boxers chan Boxer) {
	doc, err := goquery.NewDocument(url)
	defer func() { 
		finished <- true
	} ()
  if err != nil {
    log.Println(err)
  }
  doc.Find("#ratingsResults tbody tr").Each(func(index int, item *goquery.Selection) {
  	parseTd(item, boxers)
  })
}

func usage() {
	log.Fatal("Usage: go-boxers <page start> <page count> <outfile>")
}

func main() {
	if len(os.Args) < 4 {
		usage()
	}
	pagesToFetch, err := strconv.ParseInt(os.Args[2], 10, 0)
	if err != nil || pagesToFetch == 0 {
		usage()
	}
	startPage, err := strconv.ParseInt(os.Args[1], 10, 0)
	if err != nil || startPage < 1 {
		usage()
	}
	pages := int(pagesToFetch)
	start := int(startPage)
	f, err := os.Create("boxers.csv")
	defer f.Close()
	if err != nil {
		log.Fatal("Failed to create file")
	}
	w := csv.NewWriter(f)
	header := []string{
		"position",
		"link",
		"name",
		"points",
		"stars",
		"division",
		"age",
		"won",
		"lost",
		"draw",
		"last six",
		"stance",
		"residence",
	}
	err = w.Write(header)
	if err != nil {
		log.Fatal("Error writing to file")
	}
	finished := make(chan bool)
	boxers := make(chan Boxer)
	for i:=0; i<pages; i++ {
		 go crawl(fmt.Sprintf("http://boxrec.com/en/ratings?offset=%d", (i+start-1)*20), finished, boxers)
	 }
	 counter:=0
	 pageCounter := 0
LOOP:
	for {
	  select {
			case b:= <- boxers:
				err := w.Write(boxerToStrings(&b))
				if err != nil {
					log.Fatal("Error writing to file")
				}
				counter++
			case <- finished:
				log.Println("Crawl finished")
				pageCounter++
				w.Flush()
				if pageCounter == pages {
					break LOOP
				}
		}
	}
	log.Printf("%d boxers fetched", counter)
}
