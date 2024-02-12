# title: "PRACTICUM II, Part II"
# author: Victoria Klimkowski, Maik Katko
# date: Fall Full 2023

## Load packages

# Package names
packages <- c("DBI", "RSQLite", "RMySQL")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

## Connect to Database

# 1. Settings freemysqlhosting.net (max 5MB)
db_name_fh <- "sql9665320"
db_user_fh <- "sql9665320"
db_host_fh <- "sql9.freemysqlhosting.net"
db_pwd_fh <- "ITbDar1jGA"
db_port_fh <- 3306

# 2. Connect to remote server database
mydb.fh <-  dbConnect(RMySQL::MySQL(), user = db_user_fh, password = db_pwd_fh,
                      dbname = db_name_fh, host = db_host_fh, port = db_port_fh)

mydb <- mydb.fh

# 3. Connect to SQLite database
dbfile = "sqlite.db"
litedb <- dbConnect(RSQLite::SQLite(), dbfile)



# Drop Tables
sql <- "DROP TABLE IF EXISTS sales_facts"
dbExecute(mydb, sql)

# Create Tables
sql <- "DROP TABLE IF EXISTS rep_facts"
dbExecute(mydb, sql)

sql <- "CREATE TABLE IF NOT EXISTS sales_facts (
        total_sold NUMERIC NOT NULL, 
        total_units INTEGER NOT NULL,
        region TEXT NOT NULL,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        INDEX (total_sold, total_units, year, quarter)
        )"
dbExecute(mydb, sql)

sql <- "CREATE TABLE IF NOT EXISTS rep_facts (
        total_sold NUMERIC NOT NULL,
        total_qty_sold INTEGER NOT NULL,
        total_transactions INTEGER NOT NULL,
        sales_rep TEXT NOT NULL,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        product TEXT NOT NULL,
        INDEX (total_sold, year, quarter)
        )"
dbExecute(mydb, sql)

## ETL for getting data into fact tables

# Pull sales_facts table data from SQLite database
sql <- "SELECT
          SUM(s.total) AS total_sold,
          SUM(s.qty) AS total_units,
          STRFTIME('%Y', s.date) AS year,
          ((CAST(STRFTIME('%m', s.date) AS INTEGER) - 1)/3) + 1 AS quarter,
          r.territory AS region
        FROM sales s
        JOIN reps r USING(rID)
        GROUP BY
          r.territory,
          STRFTIME('%Y', s.date),
          ((CAST(STRFTIME('%m', s.date) AS INTEGER) - 1)/3) + 1"

result <- dbGetQuery(litedb, sql)

# Load sales_facts df into mySQL db
dbWriteTable(mydb, "sales_facts", result, overwrite = F, append = T, row.names = F)

# Verify that sales_facts table loaded correctly
sql <- "SELECT * FROM sales_facts"
check <- dbGetQuery(mydb, sql)
print(check)

# Pull rep_facts table data from SQLite database
# REF for CONCAT() workaround: 
# https://stackoverflow.com/questions/20284528/how-to-concat-two-columns-into-one-with-the-existing-column-name-in-mysql
sql <- "SELECT
          SUM(s.total) AS total_sold,
          SUM(qty) AS total_qty_sold,
          COUNT(tid) AS total_transactions,
          r.first_name || ' ' || r.last_name AS sales_rep,
          STRFTIME('%Y', s.date) AS year,
          ((CAST(STRFTIME('%m', s.date) AS INTEGER) - 1)/3) + 1 AS quarter,
          p.name AS product
        FROM sales s
        JOIN reps r USING(rID)
        JOIN products p USING(pID)
        GROUP BY
          r.rID,
          STRFTIME('%Y', s.date),
          ((CAST(STRFTIME('%m', s.date) AS INTEGER) - 1)/3) + 1,
          p.name"

result <- dbGetQuery(litedb, sql)

# Load rep_facts df into mySQL db
dbWriteTable(mydb, "rep_facts", result, overwrite = F, append = T, row.names = F)

# Verify that rep_facts table loaded correctly
sql <- "SELECT * FROM rep_facts"
check <- dbGetQuery(mydb, sql)
print(check)


## Clean and disconnect

# Clear SqLite tables
sql <- "DROP TABLE sales;"
dbExecute(litedb, sql)
sql <- "DROP TABLE reps;"
dbExecute(litedb, sql)
sql <- "DROP TABLE products;"
dbExecute(litedb, sql)
sql <- "DROP TABLE customers;"
dbExecute(litedb, sql)

# Disconnect from dbs
dbDisconnect(mydb)
dbDisconnect(litedb)
