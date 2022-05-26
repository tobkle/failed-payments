# Failed Payments Processing

## Usage

```bash
./failed-payments.exe -db failedPayments.sqlite3 -from failed-payments.csv -to customersToSuspend.csv -requests 3
```

or if the database and the files keep their names, the above values are the default values. If you are fine with the default values, you might start the program just like so:

```bash
./failed-payments.exe
```

or just provide any of the above parameters to overwrite the defaults for that run.

## What it does

The above program does the following:

- it reads payment request records from a `-from` file-name.csv, which you download from your payment provider
- it writes those records to a local `-db` dbname.sqlite3 database, and...
  - if the payment request id is already in the database, it skips that record, as it is already in the database, or...
  - if the payment request is not in the database, it inserts the record into the database.
- after inserting all provided new records...
- it checks the database for payments_id, which had more than `-requests` number of payment requests.
- the found payments_id with more than the allowed number of payment requests are exported in a new `-to` customersToSuspend.csv file
- if the customersToSuspend.csv file already exists, it will be overwritten with the new content

## How to import customersToSuspend.csv into Excel

1. Open a new empty Excel file
2. In Excel choose menu "Data"
3. Click on the Dropdown icon of the first menu item: Something like "Import Data from..." (Don't know the exact translation, as I'm using a German version)
4. In the Popup-Menu choose: "From Text (Legacy)"
5. In the File-open-Popup window choose the path and file of the customersToSuspend.csv file
6. Click on "Import Data" button
7. In the Text-Conversion-Assistant Step 1 of 3, choose radio-button "With delimiters - such as Commas,..."
8. Click "Next"
9. In the Text-Conversion-Assistant Step 2 of 3, choose only the checkbox: [x] "Comma"
10. Click "Next"
11. Click "Finish or Finalize"
12. If there is another popup window asking you for the cell location, just accept the suggestion with button "OK"

If it is a lot of data records, it takes a moment to get all records into the view.

## How to change the program

This is a Go program compiled in version 1.18. If you need to adjust the program to your requirements you might copy and change it.

### Setup Go

You need the Google Go language compiler installed on your machine in order to adjust and build an executable such as the above failed-payments.exe

The installation procedure depends on your operating system and chipset of your computer. [Go Installation](https://go.dev/doc/install)

### Setup Git Version Management (optional)

[Git Installation](https://git-scm.com/downloads)

### Download the Source Code

Whether by using

```bash
git clone https://github.com/tobkle/failed-payments
```

or by downloading the zip archive from [this Github Archive](https://github.com/tobkle/failed-payments) and unzip on your computer.

### Adjust the code

In any Source Code editor. You might use for example the free [Microsoft Visual Studio Code](https://code.visualstudio.com/download)
After downloading, installing and opening, you might add the Go Language package to have the Code Syntax Checker for Golang.

### Compile the code

You can compile the code for different platforms:

For OS X open the terminal program, go into the directory of the code and execute:

```bash
go build
```

You can cross compile for Windows platform:

```bash
env GOOS=windows GOARCH=386 go build
```

You can cross compile for Linux platform:

```bash
env GOOS=linux GOARCH=arm go build
```

You just have to choose a valid combination of the environment variables:

- `GOOS` = Operating System (OS)
- `GOARCH` = Chipset Architecture

Choose a valid combination from the following table:

| $GOOS     | $GOARCH  |
| --------- | -------- |
| aix       | ppc64    |
| android   | 386      |
| android   | amd64    |
| android   | arm      |
| android   | arm64    |
| darwin    | amd64    |
| darwin    | arm64    |
| dragonfly | amd64    |
| freebsd   | 386      |
| freebsd   | amd64    |
| freebsd   | arm      |
| illumos   | amd64    |
| ios       | arm64    |
| js        | wasm     |
| linux     | 386      |
| linux     | amd64    |
| linux     | arm      |
| linux     | arm64    |
| linux     | ppc64    |
| linux     | ppc64le  |
| linux     | mips     |
| linux     | mipsle   |
| linux     | mips64   |
| linux     | mips64le |
| linux     | riscv64  |
| linux     | s390x    |
| netbsd    | 386      |
| netbsd    | amd64    |
| netbsd    | arm      |
| openbsd   | 386      |
| openbsd   | amd64    |
| openbsd   | arm      |
| openbsd   | arm64    |
| plan9     | 386      |
| plan9     | amd64    |
| plan9     | arm      |
| solaris   | amd64    |
| windows   | 386      |
| windows   | amd64    |
| windows   | arm      |
| windows   | arm64    |

[Source](https://go.dev/doc/install/source#environment)
