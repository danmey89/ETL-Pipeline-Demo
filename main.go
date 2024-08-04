package main

import (
	"database/sql"
	_ "embed"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-yaml/yaml"
	"github.com/jszwec/csvutil"
	_ "github.com/lib/pq"
)

type LineItem struct {
	OrderID    uint64    `csv:"Order ID"`
	OrderDate  time.Time `csv:"Order Date"`
	Product    string    `csv:"Product"`
	ProductEAN string    `csv:"Product_ean"`
	Quantity   uint64    `csv:"Quantity Ordered"`
	Address    string    `csv:"Purchase Address"`
	Street     string    `csv:"-"`
	City       string    `csv:"-"`
	State      string    `csv:"-"`
	ZIP        string    `csv:"-"`
	Price      float64   `csv:"Price Each"`
	CostPrice  float64   `csv:"Cost price"`
	PriceTotal float64   `csv:"turnover"`
}

type dbParams struct {
	DbName   string `yaml:"dbName"`
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Sslmode  string `yaml:"sslmode"`
}

//go:embed schema.sql
var schemaSQL string

func (li *LineItem) ParseAddress() {
	addr := strings.Split(li.Address, ", ")
	stateZip := strings.Split(addr[2], " ")

	li.Street = addr[0]
	li.City = addr[1]
	li.State = stateZip[0]
	li.ZIP = stateZip[1]
}

func ExtractTransformLoad(file io.Reader, db *sql.DB) (uint64, uint64, error) {

	unmarshalTime := csvutil.UnmarshalFunc(func(input []byte, t *time.Time) error {
		timeSlice := strings.Split(string(input), " ")

		strTime := fmt.Sprintf("%sT%sZ", timeSlice[0], timeSlice[1])

		err := t.UnmarshalText([]byte(strTime))
		if err != nil {
			return err
		}
		return nil
	})

	unmarshalEAN := csvutil.UnmarshalFunc(func(f []byte, i *string) error {
		*i = strings.Split(string(f), ".")[0]
		return nil
	})

	unmarshalers := csvutil.NewUnmarshalers(
		unmarshalTime,
		unmarshalEAN,
	)

	r := csv.NewReader(file)

	dec, err := csvutil.NewDecoder(r)
	if err != nil {
		return 0, 0, fmt.Errorf("decoder error: %s", err)
	}

	dec.WithUnmarshalers(unmarshalers)

	var NumErrors uint64
	var NumRecords uint64

	var row LineItem

	query1 := `INSERT INTO purchase_orders (
		order_id, 
		order_date)
	  	VALUES ($1, $2)
	  	on conflict (order_id) do nothing;`

	query2 := `INSERT INTO line_items (
		order_id,
		product,
		product_ean,
		quantity,
		price,
		cost_price,
		price_total)
		VALUES ($1, $2, $3, $4, $5, $6, $7);`

	query3 := `INSERT INTO order_address(
		order_id,
		street,
		city,
		state,
		zip)
		VALUES ($1, $2, $3, $4, $5);`

	statement1, err := db.Prepare(query1)
	if err != nil {
		return 0, 0, err
	}

	statement2, err := db.Prepare(query2)
	if err != nil {
		return 0, 0, err
	}

	statement3, err := db.Prepare(query3)
	if err != nil {
		return 0, 0, err
	}

	chRow := make(chan LineItem, 1024)

	go func() {
		for c := range chRow {
			if _, err = statement1.Exec(c.OrderID, c.OrderDate); err != nil {
				log.Fatal(err)
			}
			if _, err = statement2.Exec(c.OrderID, c.Product, c.ProductEAN,
				c.Quantity, c.Price, c.CostPrice, c.PriceTotal); err != nil {
				log.Fatal(err)
			}

			if _, err = statement3.Exec(c.OrderID, c.Street, c.City,
				c.State, c.ZIP); err != nil {
				log.Fatal(err)
			}
		}
	}()

	go func() {
		for c := range chRow {
			if _, err = statement1.Exec(c.OrderID, c.OrderDate); err != nil {
				log.Fatal(err)
			}
			if _, err = statement2.Exec(c.OrderID, c.Product, c.ProductEAN,
				c.Quantity, c.Price, c.CostPrice, c.PriceTotal); err != nil {
				log.Fatal(err)
			}

			if _, err = statement3.Exec(c.OrderID, c.Street, c.City,
				c.State, c.ZIP); err != nil {
				log.Fatal(err)
			}
		}
	}()

	go func() {
		for c := range chRow {
			if _, err = statement1.Exec(c.OrderID, c.OrderDate); err != nil {
				log.Fatal(err)
			}
			if _, err = statement2.Exec(c.OrderID, c.Product, c.ProductEAN,
				c.Quantity, c.Price, c.CostPrice, c.PriceTotal); err != nil {
				log.Fatal(err)
			}

			if _, err = statement3.Exec(c.OrderID, c.Street, c.City,
				c.State, c.ZIP); err != nil {
				log.Fatal(err)
			}
		}
	}()

	go func() {
		for c := range chRow {
			if _, err = statement1.Exec(c.OrderID, c.OrderDate); err != nil {
				log.Fatal(err)
			}
			if _, err = statement2.Exec(c.OrderID, c.Product, c.ProductEAN,
				c.Quantity, c.Price, c.CostPrice, c.PriceTotal); err != nil {
				log.Fatal(err)
			}

			if _, err = statement3.Exec(c.OrderID, c.Street, c.City,
				c.State, c.ZIP); err != nil {
				log.Fatal(err)
			}
		}
	}()

	for {

		NumRecords++

		err = dec.Decode(&row)

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("error at row: %d: %w", NumRecords, err)

			NumErrors++

			continue
		}

		row.ParseAddress()

		chRow <- row

	}

	return NumRecords, NumErrors, nil
}

func connectDB() (*sql.DB, error) {

	rr, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	var config dbParams

	if err := yaml.Unmarshal(rr, &config); err != nil {
		log.Fatal(err)
	}

	conn := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=%s",
		config.Host, config.DbName, config.User, config.Password, config.Sslmode)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	return db, nil
}

func main() {

	file, err := os.Open("sales_data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	db, err := connectDB()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(schemaSQL); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	NumRecords, NumErrors, err := ExtractTransformLoad(file, db)

	duration := time.Since(start)

	if err != nil {
		log.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d records, %d errors, duration: %v\n", NumRecords, NumErrors, duration)
}
