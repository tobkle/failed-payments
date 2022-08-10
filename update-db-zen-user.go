package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"log"
	"os"
	"fmt"
	"errors"
	"io"
	"time"
	"path/filepath"
	"strings"
	_ "github.com/mattn/go-sqlite3"
)

func getCurrentPath() string {
	ex, err := os.Executable()
    if err != nil {
        panic(err)
    }
    exPath := filepath.Dir(ex)
    fmt.Println(exPath)
	return exPath
}

func main () {
	var dbName string
	var csvCRMFrom string
	var timestamp = time.Now().Format("2006-01-02")
	var current_path = getCurrentPath()
	var defaultDatabaseName      = filepath.Join( current_path, "failed-payment-requests-database.sqlite3"      )
	var defaultCRMFileName       = filepath.Join( current_path, "crm-accounts-"                     + timestamp + ".csv" )
	
	flag.StringVar(&dbName, "db", defaultDatabaseName, "Sqlite database to import to")
	flag.StringVar(&csvCRMFrom, "crm", defaultCRMFileName, "CSV file to import crm from")
	flag.Parse()
	
	if dbName == ""  {
		flag.PrintDefaults()
	}

	fmt.Println("Received      Database Name      :", dbName)
	fmt.Println("Received CSV-From-File CRM       :", csvCRMFrom)

	fmt.Println(" ")
	fmt.Println("***********************************************************")
	fmt.Println("UPDATING DATABASE -- started")
	fmt.Println("***********************************************************")
	
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("Cannot connect to database: %s", err)
	}

	SQLAlterTableCRMAccounts := `
		ALTER TABLE crmAccounts ADD COLUMN crm_zen_user_id TEXT
	`

	stmt, err := db.Prepare(SQLAlterTableCRMAccounts)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed Alter Table crmAccounts: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed Alter Table crmAccounts: %s", err)
	}

	// **********************************************************************************************
	// Open CSV File for CRM Accounts
	// **********************************************************************************************
	f3, err := os.Open(csvCRMFrom)
	if err != nil {
		// skip this if not exists
		fmt.Println("Skipping CRM Accounts file, as there is no current crm-accounts-YYYY-MM-DD.csv file provided....")
	} else {
		// process only if the CRM Accounts file exists
		// Read the header row
		semiReader := csv.NewReader(f3)
		// semiReader.Comma = ';'
		_, err = semiReader.Read()
		if err != nil {
			log.Fatalf("Missing header row(?): %s", err)
		}

		// prepare insert record for Accounts
		SQLInsertCRMAccountsDB := `
			INSERT INTO crmAccounts(
				crm_account_number,
				crm_id,
				crm_name,
				crm_email,
				crm_premise_address,
				crm_stage_name,
				crm_zen_user_id     
			) values(?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(crm_account_number) 
			DO UPDATE SET crm_zen_user_id = excluded.crm_zen_user_id
		`

		stmt, err = db.Prepare(SQLInsertCRMAccountsDB)
		if err != nil {
			log.Fatalf("Prepare SQL statement for insert into table failed: %s", err)
		}

		// Loop over the records
		for {
			// get next record in csv file
			record, err := semiReader.Read()

			// End of File reached
			if errors.Is(err, io.EOF) {
				break
			}

			//  Map the fields of a csv record to variables	
			crm_account_number			  := record[0]
			// C0UserID	                  := record[1]
			crm_premise_address           := record[2]
			// Premise_Type	              := record[3]
			crm_stage_name                := record[4]
			// Status	                  := record[5]
			crm_name	                  := record[6]
			crm_email	                  := record[7]
			// C0_Go_CardLess_Customer_ID := record[8]	
			crm_id	                      := record[9]
			crm_zen_user_id               := record[10]			           
			// Count                      := record[11]

			_, err = stmt.Exec(
						crm_account_number,
						crm_id,
						crm_name,
						crm_email,
						crm_premise_address,
						crm_stage_name,
						crm_zen_user_id       )
			if err != nil {
				if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {
					// fmt.Println("SUCCESS: Skipped existing record with id:", customer_account_number)
				} else {
					fmt.Println("ERROR:   Insert into table crmAccounts failed for id =", crm_account_number, crm_id, err)
				}
			} else {
					fmt.Println("SUCCESS: Insert into table crmAccounts with id:", crm_account_number, crm_id)
			}
		}
	}
	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING ELEVATE CRM ACCOUNTS --   ended")
	fmt.Println("***********************************************************")
	fmt.Println(" ")

	fmt.Println("***********************************************************")
	fmt.Println("UPDATING DATABASE -- ended")
	fmt.Println("***********************************************************")
}