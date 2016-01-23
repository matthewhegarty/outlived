// Copyright © 2016 Matthew R Hegarty

// Imports data from a source text file into a Redis Sorted Set, and allows querying of the data.
// The source data is a csv containing a list of deceased musicians in the format:
//
// FIELD 1: Name (unquoted)
// FIELD 2: Date of Birth (YYYY-MM-DD)
// FIELD 3: Date of Death (YYYY-MM-DD)
//
// The data can be imported and then queried using this script.
// A date can be passed in (for example, your own date of birth) in order to establish which
// musicians you've outlived.
// Use the '-d' flag to widen the search query.
//
// Usage:
//   ./outlived [OPTIONS] [FILE]
//
// Examples:
//
//     Import:  ./outlived -import musicians.csv
//      Query:  ./outlived -query 1990-09-25 -d 365
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	DB_ADDR  = "127.0.0.1:6379"
	DB_NAME  = "musicians"
	DATE_FMT = "2006-01-02"
)

type Person struct {
	Name      string
	BirthDate string
	DeathDate string
}

func (rec Person) String() string {
	return fmt.Sprintf("%s,%s,%s", rec.Name, rec.BirthDate, rec.DeathDate)
}

var dateFmtRegex = regexp.MustCompile("[0-9]{4}-[0-9]{2}-[0-9]{2}")

var importFile = flag.String("import", "", "Imports files into Redis database using CSV file supplied as arg")
var query = flag.String("query", "", "Query the database using a date supplied in format 'YYYY-MM-DD'")
var dayRange = flag.Int("d", 365, "Number of days either side of target date to return results")

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {

	flag.Parse()
	if *importFile == "" && *query == "" {
		Usage()
		os.Exit(0)
	}

	if *importFile != "" {
		doFileImport(*importFile)
	}
	if *query != "" {
		if *dayRange >= 0 {
			doQuery(*query, *dayRange)
		} else {
			doQuery(*query, 365)
		}
	}
}

// import data from the given file and import into Redis instance
func doFileImport(importFile string) {
	fmt.Printf("Importing records from '%s'\n", importFile)
	records := readCSVFileContents(importFile)
	fmt.Printf("Parsed %d records from file\n", len(records))
	storeRecordsInRedis(records)
	fmt.Println("Successfully completed import into Redis")
}

func doQuery(dateStr string, ndays int) {
	if !dateFmtRegex.MatchString(dateStr) {
		log.Fatalf("invalid query date format: Dates must be in the format 'YYYY-MM-DD'\n")
	}
	now := time.Now().Format(DATE_FMT)
	userAge := getAgeInDays(dateStr, now)

	c, err := redis.Dial("tcp", DB_ADDR)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	results, err := redis.Strings(c.Do("ZRANGEBYSCORE", DB_NAME, userAge-ndays, userAge+ndays))
	if err != nil {
		log.Fatal(err)
	}
	lastAge := 0
	for _, row := range results {
		fields := strings.Split(row, ",")

		name := fields[0]
		bday := fields[1]
		dday := fields[2]

		age := getAgeInDays(bday, dday)
		if userAge >= lastAge && userAge < age {
			printUserAge(userAge)
		}
		fmt.Printf("%-30s (died aged %s)\n", name, formatAgeInYearsAndDays(age))
		lastAge = age
	}
	if userAge >= lastAge { // case where user is older than everyone in return set
		printUserAge(userAge)
	}
}

func printUserAge(userAge int) {
	s := ">>> YOU ARE HERE"
	fmt.Printf("%-30s (     aged %s)\n", s, formatAgeInYearsAndDays(userAge))
}

// Format the age in years and days.
// The calculation is to divide days by 365.25 - this is the simplest method but not 100% accurate
func formatAgeInYearsAndDays(days int) string {
	var daysInYear float64 = 365.25
	ageInYears := int(float64(days) / daysInYear)
	ageInDays := int(math.Mod(float64(days), daysInYear))
	return fmt.Sprintf("%3d years and %3d days", ageInYears, ageInDays)
}

// Read and parse the CSV file and return contents as a 'Person' array
func readCSVFileContents(filename string) []Person {

	csvFile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("import: %v\n", err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	var allRecords []Person
	csvData, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("file parse: %v\n", err)
	}

	var tmpRecord Person
	for _, eachRow := range csvData {
		tmpRecord.Name = eachRow[0]
		tmpRecord.BirthDate = eachRow[1]
		tmpRecord.DeathDate = eachRow[2]
		allRecords = append(allRecords, tmpRecord)
	}
	return allRecords
}

func storeRecordsInRedis(records []Person) {
	c, err := redis.Dial("tcp", DB_ADDR)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	c.Send("MULTI")        // send following commands in a transaction
	c.Send("DEL", DB_NAME) // Remove existing data

	for _, eachRec := range records {
		ageInDays := getAgeInDays(eachRec.BirthDate, eachRec.DeathDate)
		c.Send("ZADD", DB_NAME, ageInDays, eachRec.String())
	}
	if _, err := c.Do("EXEC"); err != nil { // COMMIT data
		log.Fatal(err)
	}
}

// Takes dates as strings in format YYYY-MM-DD and returns the number of days
// between the two dates
func getAgeInDays(d1, d2 string) int {
	bd, err := time.Parse(DATE_FMT, d1)
	if err != nil {
		log.Fatalf("unparseable birth date: %v\n", err)
	}
	dd, err := time.Parse(DATE_FMT, d2)
	if err != nil {
		log.Fatalf("unparseable death date: %v\n", err)
	}
	return int(dd.Sub(bd).Hours() / 24)
}
