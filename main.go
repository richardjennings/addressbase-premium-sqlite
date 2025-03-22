package main

import (
	"archive/zip"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

var count int
var batchSize int
var db *sql.DB
var err error
var dpaQ *sql.Stmt
var schema = `
/*
modified but based on:
https://github.com/arnau/addresspack/blob/main/src/sql/bootstrap.sql
*/
CREATE TABLE IF NOT EXISTS delivery_point_address (
  record_identifier               integer(2)  NOT NULL,
  change_type                     char(1)     NOT NULL,
  pro_order                       integer(16) NOT NULL,
  uprn                            integer(12) NOT NULL,
  udprn                           integer(8)  PRIMARY KEY NOT NULL,
  organisation_name               text,
  department_name                 text,
  sub_building_name               text,
  building_name                   text,
  building_number                 integer(4),
  dependent_thoroughfare          text,
  thoroughfare                    text,
  double_dependent_locality       text,
  dependent_locality              text,
  post_town                       text        NOT NULL,
  postcode                        text        NOT NULL,
  postcode_type                   char(1)     NOT NULL,
  delivery_point_suffix           char(2)     NOT NULL,
  welsh_dependent_thoroughfare    text,
  welsh_thoroughfare              text,
  welsh_double_dependent_locality text,
  welsh_dependent_locality        text,
  welsh_post_town                 text,
  po_box_number                   char(6),
  process_date                    date        NOT NULL,
  start_date                      date        NOT NULL,
  end_date                        date,
  last_update_date                date        NOT NULL,
  entry_date                      date        NOT NULL
);
`

var commit = func() {
	// insert

}

func main() {
	batchSize = 100000
	if len(os.Args) != 2 {
		fmt.Println("aps <path to directory containing csv zip files>")
		os.Exit(1)
	}
	var files []string
	if err := filepath.WalkDir(os.Args[1], func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) == ".zip" && !d.IsDir() {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		e(err)
	}
	fmt.Printf("found %d files with extension '.zip' at path %s\n", len(files), os.Args[1])
	db, err = sql.Open("sqlite3", "ab.sqlite")
	e(err)
	db.Exec("drop table if exists delivery_point_address;")
	_, _ = db.Exec("PRAGMA synchronous = OFF;")
	_, _ = db.Exec("PRAGMA locking_mode = EXCLUSIVE;")
	_, _ = db.Exec("PRAGMA cache_size = -10000;")    // 10 Mibi
	_, _ = db.Exec("pragma mmap_size = 5000000000;") // 5GB
	_, err = db.Exec(schema)
	e(err)
	dpaQ, err = db.Prepare("INSERT INTO delivery_point_address values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	e(err)
	for _, fp := range files {
		e(readFile(fp))
	}
	fmt.Printf("%d rows processed at %s\n", count, time.Now().Format(time.RFC3339))
	_, _ = db.Exec("COMMIT;")
}

func e(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func readFile(path string) error {
	z, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer func() { _ = z.Close() }()
	_, _ = db.Exec("BEGIN TRANSACTION;")
	var res sql.Result
	for _, f := range z.File {
		if filepath.Ext(f.Name) == ".csv" {
			//fmt.Printf("processing %s\n", f.Name)
		} else {
			continue
		}
		fh, err := f.Open()
		if err != nil {
			return err
		}
		cr := csv.NewReader(fh)
		for {
			row, err := cr.Read()
			if err != nil {
				if err == io.EOF {
					return fh.Close()
				} else if errors.Is(err, csv.ErrFieldCount) {
					// this does not matter
				} else {
					_ = fh.Close()
					return err
				}
			}
			if len(row) == 0 {
				continue
			}
			switch row[0] {
			case "10":
				// header
			case "11":
				// street
			case "21":
				// BLPU
			case "15":
				// street descriptor
			case "23":
				// application cross-reference
			case "24":
				// lpi
			case "28":
				// delivery point address
				count++
				res, err = dpaQ.Exec(row[0], row[1], row[2], row[3], row[4],
					row[5], row[6], row[7], row[8], row[9], row[10],
					row[11], row[12], row[13], row[14], row[15],
					row[16], row[17], row[18], row[19], row[20],
					row[21], row[22], row[23], row[24], row[25],
					row[26], row[27], row[28])
				if err != nil {
					return err
				}
				_ = res
			case "29":
				// metadata
			case "30":
				// successor cross-reference
			case "31":
				// organisation
			case "32":
				// classification
			case "99":
				// trailer
			default:
				return fmt.Errorf("invalid row number: %s", row[0])
			}

			if count%batchSize == 0 && count != 0 {
				if count%(batchSize*10) == 0 {
					fmt.Printf("%d rows processed at %s\n", count, time.Now().Format(time.RFC3339))
				} else {
					fmt.Print(".")
				}
				_, _ = db.Exec("COMMIT;")
			}
		}
	}
	return nil
}
