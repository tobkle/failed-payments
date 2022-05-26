/**
 * name: failed-payments.exe
 * description: read csv, insert into sqlite3, find payments with more than x payment requests, export result to csv
 * author: Tobias Klemmer <tobias@klemmer.info>
 * date: 2022-05-26
 * state: quick and dirty prototype
**/
package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"fmt"
	"strconv"
	"strings"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	var dbName string
	var csvNameFrom string
	var csvNameTo string
	var minPaymentRequests int

	fmt.Println(" ")
	fmt.Println("****************************************************")
	fmt.Println("PROCESSING PAYMENT REQUESTS -- started")
	fmt.Println("****************************************************")
	
	// get command-line parameters or use defaults
	flag.StringVar(&dbName, "db", "failedPayments.sqlite3", "Sqlite database to import to")
	flag.StringVar(&csvNameFrom, "from", "failed-payments.csv", "CSV file to import from")
	flag.StringVar(&csvNameTo, "to", "customersToSuspend.csv", "CSV file to export result to")
	flag.IntVar(&minPaymentRequests, "requests", 3, "minimum payment requests - for exporting")
	flag.Parse()
	
	if dbName == "" || csvNameFrom == "" || csvNameTo == "" {
		flag.PrintDefaults()
	}
	
	fmt.Println("Received      Database Name:", dbName)
	fmt.Println("Received CSV-From-File Name:", csvNameFrom)
	fmt.Println("Received CSV-To-File   Name:", csvNameTo)
	fmt.Println("Minimum Payment Requests   :", minPaymentRequests)
	fmt.Println("****************************************************")

	// Create or Open Sqlite3 database with name of provided parameter 
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		log.Fatalf("Cannot connect to database: %s", err)
	}

	// Create or Open failedPayments table within Database
	SQLCreateDB := `
	  CREATE TABLE IF NOT EXISTS failedPaymentRequests (
		id                        text primary key, 
		created_at                text,
		resource_type             text,
		action                    text,
		details_origin            text,
		details_cause             text,
		details_description       text,
		details_scheme            text,
		details_reason_code       text,
		links_parent_event        text,
		links_payment             text,
		payments_id               text,
		payments_created_at       text,
		payments_charge_date      text,
		payments_amount           text,
		payments_description      text,
		payments_currency         text,
		payments_status           text,
		customers_id              text,
		customers_given_name      text,
		customers_family_name     text,
		customers_metadata_leadID text
	)`

	stmt, err := db.Prepare(SQLCreateDB)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed: %s", err)
	}

	// Create Index idx_payments_id
	SQLCreateDBIndexOnPaymentsId := `
       CREATE INDEX IF NOT EXISTS idx_payments_id 
	   ON failedPaymentRequests(payments_id)
	`

	stmt, err = db.Prepare(SQLCreateDBIndexOnPaymentsId)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for index on payments_id: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for index on payments_id: %s", err)
	}

	// Create Index idx_customers_id
	SQLCreateDBIndexOnCustomersId := `
       CREATE INDEX IF NOT EXISTS idx_customers_id 
	   ON failedPaymentRequests(customers_id)
	`

	stmt, err = db.Prepare(SQLCreateDBIndexOnCustomersId)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for index on customers_id: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for index on customers_id: %s", err)
	}

	// Create Index idx_customers_family_name
	SQLCreateDBIndexOnCustomersFamilyName := `
       CREATE INDEX IF NOT EXISTS idx_customers_family_name 
	   ON failedPaymentRequests(customers_family_name)
	`

	stmt, err = db.Prepare(SQLCreateDBIndexOnCustomersFamilyName)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for index on customers_family_name: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for index on customers_family_name: %s", err)
	}

	// Open CSV File
	f, err := os.Open(csvNameFrom)
	if err != nil {
		log.Fatalf("Open CSV file failed: %s", err)
	}

	// Read the header row
	r := csv.NewReader(f)
	_, err = r.Read()
	if err != nil {
		log.Fatalf("Missing header row(?): %s", err)
	}

	// prepare insert record
	SQLInsertDB := `
	INSERT INTO failedPaymentRequests(
		id                        , 
		created_at                ,
		resource_type             ,
		action                    ,
		details_origin            ,
		details_cause             ,
		details_description       ,
		details_scheme            ,
		details_reason_code       ,
		links_parent_event        ,
		links_payment             ,
		payments_id               ,
		payments_created_at       ,
		payments_charge_date      ,
		payments_amount           ,
		payments_description      ,
		payments_currency         ,
		payments_status           ,
		customers_id              ,
		customers_given_name      ,
		customers_family_name     ,
		customers_metadata_leadID 
	) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	stmt, err = db.Prepare(SQLInsertDB)
	if err != nil {
		log.Fatalf("Prepare SQL statement for insert into table failed: %s", err)
	}

	// Loop over the records
	for {
		// get next record in csv file
		record, err := r.Read()

		// End of File reached
		if errors.Is(err, io.EOF) {
			break
		}

		//  Map the fields of a csv record to variables		
		id := record[0]
		created_at := record[1]
		resource_type := record[2]
		action := record[3]
		details_origin := record[4]
		details_cause := record[5]
		details_description := record[6]
		details_scheme := record[7]
		details_reason_code := record[8]
		links_parent_event := record[9]
		links_payment := record[10]
		payments_id := record[11]
		payments_created_at := record[12]
		payments_charge_date := record[13]
		payments_amount := record[14]
		payments_description := record[15]
		payments_currency := record[16]
		payments_status := record[17]
		// payments_amount_refunded := record[19]
		// payments_reference := record[20]
		// payments_links_mandate := record[21]
		// payments_links_creditor := record[22]
		// payments_links_payout := record[23]
		// payments_links_subscription := record[24]
		customers_id := record[24]
		customers_given_name := record[25]
		customers_family_name := record[26]
		// customers_company_name := record[28]
		customers_metadata_leadID := record[28]
		// customers_metadata_link := record[30]
		// customers_metadata_xero := record[31]
		// payments_metadata_identity := record[32]
		// payments_metadata_invoiceNumber := record[33]
		// payments_metadata_invoiceType := record[34]
		// payments_metadata_xero := record[35]

		_, err = stmt.Exec(
							id                        , 
							created_at                ,
							resource_type             ,
							action                    ,
							details_origin            ,
							details_cause             ,
							details_description       ,
							details_scheme            ,
							details_reason_code       ,
							links_parent_event        ,
							links_payment             ,
							payments_id               ,
							payments_created_at       ,
							payments_charge_date      ,
							payments_amount           ,
							payments_description      ,
							payments_currency         ,
							payments_status           ,
							customers_id              ,
							customers_given_name      ,
							customers_family_name     ,
							customers_metadata_leadID )
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {
				fmt.Println("SUCCESS: Skipped existing record with id:", id)
			} else {
				fmt.Println("ERROR:   Insert into table failed for id =", id, err)
			}
		} else {
			    fmt.Println("SUCCESS: Insert into table with id:", id)
		}
	}

	fmt.Println("****************************************************")
	fmt.Println("PROCESSING PAYMENT REQUESTS --   ended")
	fmt.Println("****************************************************")
	fmt.Println(fmt.Sprintf("FIND PAYMENTS WITH MORE THAN %s REQUESTS --   started", strconv.Itoa(minPaymentRequests)))
	fmt.Println("****************************************************")

	// write export file
	targetFile, err := os.OpenFile(csvNameTo, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFile.Close()

	SQLQuery := fmt.Sprintf(`
SELECT 
    resource_type             ,
    action                    ,
    details_origin            ,
    details_cause             ,
    details_description       ,
    details_scheme            ,
    details_reason_code       ,
    links_parent_event        ,
    links_payment             ,
    payments_id               ,
    payments_created_at       ,
    payments_charge_date      ,
    payments_amount           ,
    payments_description      ,
    payments_currency         ,
    payments_status           ,
    customers_id              ,
    customers_given_name      ,
    customers_family_name     ,
    customers_metadata_leadID ,
    COUNT(payments_id) 
  FROM failedPaymentRequests 
  GROUP BY payments_id 
  HAVING count(payments_id) > %s 
  ORDER BY payments_id
`, strconv.Itoa(minPaymentRequests))

	row, err := db.Query(SQLQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	headerText := "resource_type,action,details_origin,details_cause,details_description,details_scheme,details_reason_code,links_parent_event,links_payment,payments_id,payments_created_at,payments_charge_date,payments_amount,payments_description,payments_currency,payments_status,customers_id,customers_given_name,customers_family_name,customers_metadata_leadID,payment_requests_counted\n"
	if _, err = targetFile.WriteString(headerText); err != nil {
		panic(err)
	}

	for row.Next() {
		var resource_type string
		var action string
		var details_origin string
		var details_cause string
		var details_description string
		var details_scheme string
		var details_reason_code string
		var links_parent_event string
		var links_payment string
		var payments_id string
		var payments_created_at string
		var payments_charge_date string
		var payments_amount string
		var payments_description string
		var payments_currency string
		var payments_status string
		var customers_id string
		var customers_given_name string
		var customers_family_name string
		var customers_metadata_leadID string
		var payment_requests_counted string

		row.Scan(&resource_type, &action, &details_origin, &details_cause, &details_description, &details_scheme, &details_reason_code, &links_parent_event, &links_payment, &payments_id, &payments_created_at, &payments_charge_date, &payments_amount, &payments_description, &payments_currency, &payments_status, &customers_id, &customers_given_name, &customers_family_name, &customers_metadata_leadID, &payment_requests_counted)

		resultText := 	"\"" + resource_type + "\"," +
						"\"" + action + "\"," + 
						"\"" + details_origin + "\"," + 
						"\"" + details_cause + "\"," + 
						"\"" + details_description + "\"," + 
						"\"" + details_scheme + "\"," + 
						"\"" + details_reason_code + "\"," + 
						"\"" + links_parent_event + "\"," + 
						"\"" + links_payment + "\"," + 
						"\"" + payments_id + "\"," + 
						"\"" + payments_created_at + "\"," + 
						"\"" + payments_charge_date + "\"," + 
						       payments_amount + "," + 
						"\"" + payments_description + "\"," + 
						"\"" + payments_currency + "\"," + 
						"\"" + payments_status + "\"," + 
						"\"" + customers_id + "\"," + 
						"\"" + customers_given_name + "\"," + 
						"\"" + customers_family_name + "\"," + 
						"\"" + customers_metadata_leadID + "\"," + 
						       payment_requests_counted + "\n"

		log.Println(fmt.Sprintf("customer_id %s for payments_id %s had %s payment requests and exceeded the allowed limit --> %s", customers_id, payments_id, payment_requests_counted, csvNameTo))
		
		if _, err = targetFile.WriteString(resultText); err != nil {
			panic(err)
		}
	}

	fmt.Println("****************************************************")
	fmt.Println(fmt.Sprintf("FIND PAYMENTS WITH MORE THAN %s REQUESTS --   ended", strconv.Itoa(minPaymentRequests)))
	fmt.Println("****************************************************")
	fmt.Println(" ")
}