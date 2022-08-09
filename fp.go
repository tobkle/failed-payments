/********************************************************************************************************************
 * name: fp
 * description: read csv, insert into sqlite3, find payments with more than x payment requests, export result to csv
 * author: Tobias Klemmer <tobias@klemmer.info>
 * date:    2022-05-28
 * changed: 2022-08-04
 * version: 7
 * state: prototype
 ********************************************************************************************************************/
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
	"time"
	"path/filepath"
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

func main() {
	var dbName string
	var csvAccountsFrom string
	var csvCRMFrom string
	var csvNameFrom string
	var csvNameToWarn string
	var csvNameToSuspendSmall string
	var csvNameToSuspendLarge string
	var paymentRequestsToWarn int
	var minPaymentRequestsToSuspend int
	var amountToSwitch float64
	var timestamp = time.Now().Format("2006-01-02")
	var current_path = getCurrentPath()
	var defaultDatabaseName      = filepath.Join( current_path, "failed-payment-requests-database.sqlite3"      )
	var defaultSourceFileName    = filepath.Join( current_path, "failed-payment-requests-" + timestamp + ".csv" )
	var defaultAccountsFileName  = filepath.Join( current_path, "elevate-accounts-"        + timestamp + ".csv" )
	var defaultCRMFileName       = filepath.Join( current_path, "crm-accounts-"                     + timestamp + ".csv" )
	var defaultToWarnFileName    = filepath.Join( current_path, "customers-to-warn-"       + timestamp + ".csv" )
	var defaultToSuspendFileNameSmall = filepath.Join( current_path, "customers-to-suspend-small-"    + timestamp + ".csv" )
	var defaultToSuspendFileNameLarge = filepath.Join( current_path, "customers-to-suspend-large-"    + timestamp + ".csv" )

	fmt.Println(" ")
	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING PAYMENT REQUESTS -- started")
	fmt.Println("***********************************************************")
	
	// get command-line parameters or use defaults
	flag.StringVar(&dbName, "db", defaultDatabaseName, "Sqlite database to import to")
	flag.StringVar(&csvNameFrom, "from", defaultSourceFileName, "CSV file to import from")
	flag.StringVar(&csvAccountsFrom, "accounts", defaultAccountsFileName, "CSV file to import accounts from")
	flag.StringVar(&csvCRMFrom, "crm", defaultCRMFileName, "CSV file to import crm from")
	flag.StringVar(&csvNameToWarn, "warn", defaultToWarnFileName, "CSV file to export result to")
	flag.StringVar(&csvNameToSuspendSmall, "toSmall", defaultToSuspendFileNameSmall, "CSV file small to export result to")
	flag.StringVar(&csvNameToSuspendLarge, "toLarge", defaultToSuspendFileNameLarge, "CSV file large to export result to")
	flag.IntVar(&paymentRequestsToWarn, "count-warn", 3, "payment requests to warn - for exporting")
	flag.IntVar(&minPaymentRequestsToSuspend, "count-suspend", 4, "minimum payment requests  to suspend- for exporting")
	flag.Float64Var(&amountToSwitch, "amount", 20.0, "amount to switch between small and large amounts")
	flag.Parse()
	
	if dbName == "" || csvNameFrom == ""|| csvNameToWarn == "" || csvNameToSuspendSmall == "" {
		flag.PrintDefaults()
	}
	
	fmt.Println("Received      Database Name      :", dbName)
	fmt.Println("Received CSV-From-File Accounts  :", csvAccountsFrom)
	fmt.Println("Received CSV-From-File CRM       :", csvCRMFrom)
	fmt.Println("Received CSV-From-File Name      :", csvNameFrom)
	fmt.Println("Received CSV-To-Warn-File Name   :", csvNameToWarn)
	fmt.Println("Received CSV-To-Suspend File Name:", csvNameToSuspendSmall)
	fmt.Println("Received CSV-To-Suspend File Name:", csvNameToSuspendLarge)
	fmt.Println("Minimum Payment Requests Warn    :", paymentRequestsToWarn)
	fmt.Println("Minimum Payment Requests Suspend :", minPaymentRequestsToSuspend)
	fmt.Println("***********************************************************")

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

	// Create or Open Accounts table within Database
	SQLCreateAccountsDB := `
	  CREATE TABLE IF NOT EXISTS elevateAccounts (
		elevate_mandate_reference text primary key,
		elevate_account_number    text,
		elevate_customer_name     text 
	)`

	stmt, err := db.Prepare(SQLCreateAccountsDB)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed: %s", err)
	}

	// Create or Open failedPayments table within Database
	SQLCreateDB := `
	  CREATE TABLE IF NOT EXISTS failedPaymentRequests (
		id                         text primary key, 
		created_at                 text,
		resource_type              text,
		action                     text,
		details_origin             text,
		details_cause              text,
		details_description        text,
		details_scheme             text,
		details_reason_code        text,
		links_parent_event         text,
		links_payment              text,
		payments_id                text,
		payments_created_at        text,
		payments_charge_date       text,
		payments_amount            text,
		payments_description       text,
		payments_currency          text,
		payments_status            text,
		customers_id               text,
		customers_given_name       text,
		customers_family_name      text,
		customers_metadata_leadID  text,
		payments_links_mandate     text,
		payments_metadata_identity text
	)`

	stmt2, err := db.Prepare(SQLCreateDB)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed: %s", err)
	}
	_, err = stmt2.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed: %s", err)
	}

	// Create or Open CRM table within Database
	SQLCreateCRMAccountsDB := `
	  CREATE TABLE IF NOT EXISTS crmAccounts (
		crm_account_number    text primary key,
		crm_id                text,
		crm_name              text,
		crm_email             text,
		crm_premise_address   text,
		crm_stage_name        text
	)`

	stmt3, err := db.Prepare(SQLCreateCRMAccountsDB)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed: %s", err)
	}
	_, err = stmt3.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed: %s", err)
	}

	// Create or Open paymentsWarnings table within Database
	SQLCreateTableWarnings := `
	  CREATE TABLE IF NOT EXISTS paymentsWarnings (
		payments_id               text primary key,
		timestamp                 text,
		payment_requests_count    integer,
		customers_id              text,
		customers_given_name      text,
		customers_family_name     text,
		customers_metadata_leadID text
	)`

	stmt, err = db.Prepare(SQLCreateTableWarnings)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for table paymentsWarnings: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for table paymentsWarnings: %s", err)
	}

	// Create or Open paymentsSuspended table within Database
	SQLCreateTableSuspended := `
	  CREATE TABLE IF NOT EXISTS paymentsSuspended (
		payments_id               text primary key,
		timestamp                 text,
		payment_requests_count    integer,
		customers_id              text,
		customers_given_name      text,
		customers_family_name     text,
		customers_metadata_leadID text
	)`

	stmt, err = db.Prepare(SQLCreateTableSuspended)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for table paymentsSuspended: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for table paymentsSuspended: %s", err)
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
	
	// Create Index idx_customers_family_name
	SQLCreateDBIndexOnPaymentsWarningTimestamp := `
       CREATE INDEX IF NOT EXISTS idx_payments_warning_timestamp 
	   ON paymentsWarnings(timestamp)
	`

	stmt, err = db.Prepare(SQLCreateDBIndexOnPaymentsWarningTimestamp)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for index on paymentsWarning timestamp: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for index on paymentsWarning timestamp: %s", err)
	}

	// Create Index idx_customers_family_name
	SQLCreateDBIndexOnPaymentsSuspendedTimestamp := `
       CREATE INDEX IF NOT EXISTS idx_payments_suspended_timestamp 
	   ON paymentsSuspended(timestamp)
	`

	stmt, err = db.Prepare(SQLCreateDBIndexOnPaymentsSuspendedTimestamp)
	if err != nil {
		log.Fatalf("SQL Statement prepare failed for index on paymentsSuspended timestamp: %s", err)
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("SQL Statement execution failed for index on paymentsSuspended timestamp: %s", err)
	}

	// **********************************************************************************************
	// Open CSV File for Accounts
	// **********************************************************************************************
	f, err := os.Open(csvAccountsFrom)
	if err != nil {
		log.Fatalf("Open CSV file failed: %s", err)
	}

	// Read the header row
	r := csv.NewReader(f)
	_, err = r.Read()
	if err != nil {
		log.Fatalf("Missing header row(?): %s", err)
	}

	// prepare insert record for Accounts
	SQLInsertAccountsDB := `
	INSERT INTO elevateAccounts(
		elevate_mandate_reference,
		elevate_account_number,
		elevate_customer_name       
	) values(?, ?, ?)
	`
	stmt, err = db.Prepare(SQLInsertAccountsDB)
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
		   customer_account_number			 := record[0]
		// Customer_ID	                  	 := record[1]
		   customer_name					 := record[2]
		// Site_ID							 := record[3]
		// site_reference					 := record[4]
		// product_category_name			 := record[5]
		// product_type				    	 := record[6]
		// service_id						 := record[7]
		// product_reference				 := record[8]
		// supplier_name					 := record[9]
		// override						     := record[10]
		// start_date						 := record[11]
		// end_date						     := record[12]
		// rental_product_name				 := record[13]
		// cap_price_in_pence				 := record[14]
		// provisioning_status				 := record[15]
		// billable						     := record[16]
		// in_flight_order					 := record[17]
		// force_billing					 := record[18]
		// invoice_frequency				 := record[19]
		// bill_initial_charges_immediately  := record[20]
		// contractName					     := record[21]
		// contractStartDate				 := record[22]
		// contractEndDate					 := record[23]
		// EtcFixed						     := record[24]
		// EtcPercentage					 := record[25]
		// contract_expires_in_months		 := record[26]
		// customerContractDueRenewal		 := record[27]
		// customerContractAutoRollOver	     := record[28]
		// contractProfileName				 := record[29]
		   mandate_reference				 := record[30]
		// site_address_line1                := record[31]
		// site_address_line2                := record[32]
		// town                              := record[33]
		// county                            := record[34]
		// post_code                         := record[35]
		// country                           := record[36]


		_, err = stmt.Exec(
							mandate_reference         , 
							customer_account_number   ,
							customer_name             )
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {
				// fmt.Println("SUCCESS: Skipped existing record with id:", customer_account_number)
			} else {
				fmt.Println("ERROR:   Insert into table elevateAccounts failed for id =", customer_account_number, err)
			}
		} else {
			    fmt.Println("SUCCESS: Insert into table elevateAccounts with id:", customer_account_number)
		}
	}

	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING ELEVATE ACCOUNTS --   ended")
	fmt.Println("***********************************************************")
	fmt.Println(" ")

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
			crm_stage_name     
		) values(?, ?, ?, ?, ?, ?)
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
			// Count                      := record[10]

			_, err = stmt.Exec(
						crm_account_number,
						crm_id,
						crm_name,
						crm_email,
						crm_premise_address,
						crm_stage_name       )
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

	// **********************************************************************************************
	// Open CSV File for FailedPayments
	// **********************************************************************************************
	f2, err := os.Open(csvNameFrom)
	if err != nil {
		log.Fatalf("Open CSV file failed: %s", err)
	}

	// Read the header row
	r2 := csv.NewReader(f2)
	_, err = r2.Read()
	if err != nil {
		log.Fatalf("Missing header row(?): %s", err)
	}

	// prepare insert record for FailedPayments
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
		customers_metadata_leadID ,
		payments_links_mandate    ,
		payments_metadata_identity
	) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	stmt, err = db.Prepare(SQLInsertDB)
	if err != nil {
		log.Fatalf("Prepare SQL statement for insert into table failed: %s", err)
	}

	// Loop over the records
	for {
		// get next record in csv file
		record, err := r2.Read()

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
		payments_links_mandate := record[20]
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
		payments_metadata_identity := record[32]
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
							customers_metadata_leadID ,
						    payments_links_mandate    ,
							payments_metadata_identity)
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

	fmt.Println("***********************************************************")
	fmt.Println("PROCESSING PAYMENT REQUESTS --   ended")
	fmt.Println("***********************************************************")
	fmt.Println(" ")
	fmt.Println("***********************************************************")
	fmt.Println(fmt.Sprintf("FIND PAYMENTS WITH MORE THAN %s REQUESTS --   started", strconv.Itoa(paymentRequestsToWarn)))
	fmt.Println("***********************************************************")
	// avoiding database is locked error by setting up a transaction
    // https://github.com/mattn/go-sqlite3/issues/569
	tx, err := db.Begin()

    // prepare insert record
	SQLInsertTablePaymentsWarning := `
		INSERT INTO paymentsWarnings(
			payments_id               ,
			timestamp                 ,
			payment_requests_count    ,
			customers_id              ,
			customers_given_name      ,
			customers_family_name     ,
			customers_metadata_leadID 
		) values(?, ?, ?, ?, ?, ?, ?)
	`
	stmtInsertWarnings, err := tx.Prepare(SQLInsertTablePaymentsWarning)
	if err != nil {
		log.Fatalf("Prepare SQL statement for insert into table paymentsWarnings failed: %s", err)
	}

	// prepare upsert record
	SQLInsertTablePaymentsSuspended := `
		INSERT INTO paymentsSuspended(
			payments_id               ,
			timestamp                 ,
			payment_requests_count    ,
			customers_id              ,
			customers_given_name      ,
			customers_family_name     ,
			customers_metadata_leadID 
		) values(?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(payments_id) 
		DO UPDATE SET
			timestamp                         = excluded.timestamp,
			payment_requests_count            = excluded.payment_requests_count
		WHERE excluded.payment_requests_count > paymentsSuspended.payment_requests_count
	`
	stmtInsertSuspended, err := tx.Prepare(SQLInsertTablePaymentsSuspended)
	if err != nil {
		log.Fatalf("Prepare SQL statement for insert into table paymentsSuspended failed: %s", err)
	}

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
		WHERE action = "failed"
		GROUP BY payments_id 
		HAVING count(payments_id) >= %s 
		ORDER BY payments_id
	`, strconv.Itoa(paymentRequestsToWarn))

	row, err := tx.Query(SQLQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

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
		var payment_requests_count string

		row.Scan(
			&resource_type, 
			&action, 
			&details_origin, 
			&details_cause, 
			&details_description, 
			&details_scheme, 
			&details_reason_code, 
			&links_parent_event, 
			&links_payment, 
			&payments_id, 
			&payments_created_at, 
			&payments_charge_date, 
			&payments_amount, 
			&payments_description, 
			&payments_currency, 
			&payments_status, 
			&customers_id, 
			&customers_given_name, 
			&customers_family_name, 
			&customers_metadata_leadID, 
			&payment_requests_count)

		// create record into paymentsWarnings
		if payment_requests_count == strconv.Itoa(paymentRequestsToWarn) {
			_, err = stmtInsertWarnings.Exec(
				payments_id               ,
				timestamp                 ,
				payment_requests_count    ,
				customers_id              ,
				customers_given_name      ,
				customers_family_name     ,
				customers_metadata_leadID )
			if err != nil {
				if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {
					fmt.Println("SUCCESS: Skipped existing warning for payments_id                :", payments_id)
				} else {
					fmt.Println("ERROR:   Insert into table paymentWarnings failed for payments_id:", payments_id, err)
				}
			} else {
					fmt.Println("SUCCESS: Inserted new warning for payments_id                    :", payments_id)
			}
		} 

		// create record into paymentsSuspended
		if payment_requests_count >= strconv.Itoa(minPaymentRequestsToSuspend) {
			_, err = stmtInsertSuspended.Exec(
				payments_id               ,
				timestamp                 ,
				payment_requests_count    ,
				customers_id              ,
				customers_given_name      ,
				customers_family_name     ,
				customers_metadata_leadID )
			if err != nil {
				if strings.Contains(fmt.Sprint(err), "UNIQUE constraint failed") {

					fmt.Println("SUCCESS: Skipped existing suspend for payments_id                 :", payments_id)
				} else {
					fmt.Println("ERROR:   Insert into table paymentSuspended failed for payments_id:", payments_id, err)
				}
			} else {
					fmt.Println("SUCCESS: Inserted new suspend for payments_id                     :", payments_id)
			}
		}
	}
	err = row.Err()
	tx.Commit()
	row.Close()

	fmt.Println("***********************************************************")
	fmt.Println(fmt.Sprintf("FIND PAYMENTS WITH MORE THAN %s REQUESTS --   ended", strconv.Itoa(paymentRequestsToWarn)))
	fmt.Println("***********************************************************")
	fmt.Println(" ")

	headerText := "resource_type,action,details_origin,details_cause,details_description,details_scheme,details_reason_code,links_parent_event,links_payment,payments_id,payments_created_at,payments_charge_date,payments_amount,payments_description,payments_currency,payments_status,customers_id,customers_given_name,customers_family_name,customers_metadata_leadID,payments_links_mandate,payments_metadata_identity,elevate_account_number,elevate_customer_name,payment_requests_counted,crm_id,crm_name,crm_email,crm_premise_address,crm_stage_name\n"
	

	fmt.Println("***********************************************************")
	fmt.Println(fmt.Sprintf("CREATE customers-to-warn file WITH %s REQUESTS --   started", strconv.Itoa(paymentRequestsToWarn)))
	fmt.Println("***********************************************************")
	fmt.Println(" ")

	// write export file to customers-to-warn-YYYY-MM-DD.csv
	targetFileWarn, err := os.OpenFile(csvNameToWarn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFileWarn.Close()

	if _, err = targetFileWarn.WriteString(headerText); err != nil {
		panic(err)
	}

	SQLQueryExportWarnings := fmt.Sprintf(`
		SELECT DISTINCT
			failedPaymentRequests.resource_type              ,
			failedPaymentRequests.action                     ,
			failedPaymentRequests.details_origin             ,
			failedPaymentRequests.details_cause              ,
			failedPaymentRequests.details_description        ,
			failedPaymentRequests.details_scheme             ,
			failedPaymentRequests.details_reason_code        ,
			failedPaymentRequests.links_parent_event         ,
			failedPaymentRequests.links_payment              ,
			failedPaymentRequests.payments_id                ,
			failedPaymentRequests.payments_created_at        ,
			failedPaymentRequests.payments_charge_date       ,
			failedPaymentRequests.payments_amount            ,
			failedPaymentRequests.payments_description       ,
			failedPaymentRequests.payments_currency          ,
			failedPaymentRequests.payments_status            ,
			failedPaymentRequests.customers_id               ,
			failedPaymentRequests.customers_given_name       ,
			failedPaymentRequests.customers_family_name      ,
			failedPaymentRequests.customers_metadata_leadID  ,
			failedPaymentRequests.payments_links_mandate     ,
			failedPaymentRequests.payments_metadata_identity ,
			COUNT(paymentsWarnings.payments_id)              ,
			elevateAccounts.elevate_account_number           ,
			elevateAccounts.elevate_customer_name            ,
			crmAccounts.crm_id                               ,
			crmAccounts.crm_name                             ,
			crmAccounts.crm_email                            ,
			crmAccounts.crm_premise_address                  ,
			crmAccounts.crm_stage_name        
		FROM paymentsWarnings
		INNER JOIN failedPaymentRequests 
		ON failedPaymentRequests.payments_id = paymentsWarnings.payments_id
		LEFT  JOIN elevateAccounts
		ON failedPaymentRequests.payments_links_mandate = elevateAccounts.elevate_mandate_reference
		LEFT JOIN crmAccounts
		ON elevateAccounts.elevate_account_number = crmAccounts.crm_account_number
		WHERE paymentsWarnings.timestamp = "%s"
		GROUP BY   paymentsWarnings.payments_id 
		ORDER BY   paymentsWarnings.payments_id
	`, timestamp)

	rowWarnings, err := db.Query(SQLQueryExportWarnings)
	if err != nil {
		log.Fatal(err)
	}
	defer rowWarnings.Close()

	for rowWarnings.Next() {
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
		var payments_links_mandate     string
	    var payments_metadata_identity string
		var payment_requests_count string      
	    var elevate_account_number     string
		var elevate_customer_name      string
		var crm_id                 string
		var crm_name               string
		var crm_email              string
		var crm_premise_address    string
		var crm_stage_name         string

		rowWarnings.Scan(&resource_type, 
			&action, 
			&details_origin, 
			&details_cause, 
			&details_description, 
			&details_scheme, 
			&details_reason_code, 
			&links_parent_event, 
			&links_payment, 
			&payments_id, 
			&payments_created_at, 
			&payments_charge_date, 
			&payments_amount, 
			&payments_description, 
			&payments_currency, 
			&payments_status, 
			&customers_id, 
			&customers_given_name, 
			&customers_family_name, 
			&customers_metadata_leadID, 
			&payments_links_mandate, 
			&payments_metadata_identity, 
			&payment_requests_count, 
			&elevate_account_number, 
			&elevate_customer_name,
			&crm_id,
			&crm_name,
			&crm_email,
			&crm_premise_address,
			&crm_stage_name)

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
						"\"" + payments_links_mandate + "\"," + 
						"\"" + payments_metadata_identity + "\"," + 
						"\"" + elevate_account_number + "\"," + 
						"\"" + elevate_customer_name + "\"," + 
						       payment_requests_count + ","  +
						"\"" + crm_id + "\"," + 
						"\"" + crm_name + "\"," + 
						"\"" + crm_email + "\"," + 
						"\"" + crm_premise_address + "\"," + 
						"\"" + crm_stage_name + "\"\n"


		log.Println(fmt.Sprintf("customer_id %s for payments_id %s had %s payment requests and exceeded the allowed limit --> %s", customers_id, payments_id, payment_requests_count, csvNameToWarn))
		
		if _, err = targetFileWarn.WriteString(resultText); err != nil {
			panic(err)
		}
	}

	 rowWarnings.Close()

	fmt.Println("***********************************************************")
	fmt.Println(fmt.Sprintf("CREATE customers-to-warn file WITH %s REQUESTS --      ended", strconv.Itoa(paymentRequestsToWarn)))
	fmt.Println("***********************************************************")
	fmt.Println(" ")

	fmt.Println("***********************************************************")
	fmt.Println(fmt.Sprintf("CREATE customers-to-suspend file WITH %s REQUESTS -- started", strconv.Itoa(minPaymentRequestsToSuspend)))
	fmt.Println("***********************************************************")
	fmt.Println(" ")

	// write export file to customers-to-suspend-small-YYYY-MM-DD.csv
	targetFileSuspendSmall, err := os.OpenFile(csvNameToSuspendSmall, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFileSuspendSmall.Close()

	if _, err = targetFileSuspendSmall.WriteString(headerText); err != nil {
		panic(err)
	}

	// write export file to customers-to-suspend-large-YYYY-MM-DD.csv
	targetFileSuspendLarge, err := os.OpenFile(csvNameToSuspendLarge, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer targetFileSuspendLarge.Close()

	if _, err = targetFileSuspendLarge.WriteString(headerText); err != nil {
		panic(err)
	}

	SQLQueryExportSuspended := fmt.Sprintf(`
		SELECT DISTINCT
			failedPaymentRequests.resource_type              ,
			failedPaymentRequests.action                     ,
			failedPaymentRequests.details_origin             ,
			failedPaymentRequests.details_cause              ,
			failedPaymentRequests.details_description        ,
			failedPaymentRequests.details_scheme             ,
			failedPaymentRequests.details_reason_code        ,
			failedPaymentRequests.links_parent_event         ,
			failedPaymentRequests.links_payment              ,
			failedPaymentRequests.payments_id                ,
			failedPaymentRequests.payments_created_at        ,
			failedPaymentRequests.payments_charge_date       ,
			failedPaymentRequests.payments_amount            ,
			failedPaymentRequests.payments_description       ,
			failedPaymentRequests.payments_currency          ,
			failedPaymentRequests.payments_status            ,
			failedPaymentRequests.customers_id               ,
			failedPaymentRequests.customers_given_name       ,
			failedPaymentRequests.customers_family_name      ,
			failedPaymentRequests.customers_metadata_leadID  ,
			failedPaymentRequests.payments_links_mandate     ,
			failedPaymentRequests.payments_metadata_identity ,
			COUNT(failedPaymentRequests.payments_id)         ,
			elevateAccounts.elevate_account_number           ,
			elevateAccounts.elevate_customer_name            ,
			crmAccounts.crm_id                               ,
			crmAccounts.crm_name                             ,
			crmAccounts.crm_email                            ,
			crmAccounts.crm_premise_address                  ,
			crmAccounts.crm_stage_name  
		FROM       paymentsSuspended
		INNER JOIN failedPaymentRequests 
		ON         failedPaymentRequests.payments_id = paymentsSuspended.payments_id
		LEFT JOIN elevateAccounts
		ON failedPaymentRequests.payments_links_mandate = elevateAccounts.elevate_mandate_reference
		LEFT JOIN crmAccounts
		ON elevateAccounts.elevate_account_number = crmAccounts.crm_account_number
		WHERE      paymentsSuspended.timestamp = "%s"
		GROUP BY   failedPaymentRequests.payments_id 
		ORDER BY   failedPaymentRequests.payments_id
	`, timestamp)

	rowSuspended, err := db.Query(SQLQueryExportSuspended)
	if err != nil {
		log.Fatal(err)
	}
	defer rowSuspended.Close()

	for rowSuspended.Next() {
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
		var customers_metadata_leadID  string
		var payments_links_mandate     string
		var payments_metadata_identity string
		var payment_requests_count     string
		var elevate_account_number     string
		var elevate_customer_name      string
		var crm_id                 string
		var crm_name               string
		var crm_email              string
		var crm_premise_address    string
		var crm_stage_name         string

		rowSuspended.Scan(
			&resource_type, 
			&action, 
			&details_origin, 
			&details_cause,
			&details_description, 
			&details_scheme, 
			&details_reason_code, 
			&links_parent_event, 
			&links_payment, 
			&payments_id, 
			&payments_created_at, 
			&payments_charge_date, 
			&payments_amount, 
			&payments_description, 
			&payments_currency, 
			&payments_status, 
			&customers_id, 
			&customers_given_name, 
			&customers_family_name, 
			&customers_metadata_leadID, 
			&payments_links_mandate, 
			&payments_metadata_identity, 
			&payment_requests_count,
			&elevate_account_number, 
			&elevate_customer_name,
			&crm_id,
			&crm_name,
			&crm_email,
			&crm_premise_address,
			&crm_stage_name)

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
						"\"" + payments_links_mandate + "\"," + 
						"\"" + payments_metadata_identity + "\"," + 
						"\"" + elevate_account_number + "\"," + 
						"\"" + elevate_customer_name + "\"," + 
						       payment_requests_count +  "," +
						"\"" + crm_id + "\"," + 
						"\"" + crm_name + "\"," + 
						"\"" + crm_email + "\"," + 
						"\"" + crm_premise_address + "\"," + 
						"\"" + crm_stage_name + "\"\n"

		paymentValue, err := strconv.ParseFloat(payments_amount, 64)
		if paymentValue < amountToSwitch {
			log.Println(fmt.Sprintf("customer_id %s for payments_id %s had %s payment requests and exceeded the allowed limit --> %s", customers_id, payments_id, payment_requests_count, csvNameToSuspendSmall))
			if _, err = targetFileSuspendSmall.WriteString(resultText); err != nil {
				panic(err)
			}
		} else {			
			log.Println(fmt.Sprintf("customer_id %s for payments_id %s had %s payment requests and exceeded the allowed limit --> %s", customers_id, payments_id, payment_requests_count, csvNameToSuspendLarge))
			if _, err = targetFileSuspendLarge.WriteString(resultText); err != nil {
				panic(err)
			}
		}
	}

	rowSuspended.Close()

	fmt.Println("***********************************************************")
	fmt.Println(fmt.Sprintf("CREATE customers-to-suspend file WITH %s REQUESTS -- ended", strconv.Itoa(minPaymentRequestsToSuspend)))
	fmt.Println("***********************************************************")
	fmt.Println(" ")
	fmt.Println("***********************************************************")
	fmt.Println(" F I N I S H E D")
	fmt.Println("***********************************************************")
}