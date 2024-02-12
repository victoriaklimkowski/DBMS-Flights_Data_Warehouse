# title: "PRACTICUM II, Part 1
# author: Victoria Klimkowski, Maik Katko
# date: Fall Full 2023

## Load packages
################

# Package names
packages <- c("DBI", "RSQLite", "RMySQL", "XML")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

## Connect to Database
######################

library(DBI)

dbfile = "sqlite.db"
lconn <- dbConnect(RSQLite::SQLite(), dbfile)

## Create SQLite Tables: Q1-4
#############################
setupTables <- function(lconn) {
  # Ref: Used chatGPT to speed up formatting from lucidchart diagram tables
  
  # Create reps table
  sql <- "CREATE TABLE IF NOT EXISTS reps (
        rID INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        territory TEXT NOT NULL,
        commission NUMERIC NOT NULL
        );"
  dbExecute(lconn, sql)
  
  # Create products table
  sql <- "CREATE TABLE IF NOT EXISTS products (
        pID INTEGER PRIMARY KEY,
        name TEXT NOT NULL
        );"
  dbExecute(lconn, sql)
  
  # Create customers table
  sql <- "CREATE TABLE IF NOT EXISTS customers (
        cID INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        country TEXT NOT NULL
        );"
  dbExecute(lconn, sql)
  
  # Create sales table
  # We store the origin file name and original transaction ID for referencing purposes
  # ... and use a synthetic tID to ensure uniqueness within the database
  # ... for data provincing purposes
  sql <- "CREATE TABLE IF NOT EXISTS sales (
        tID INTEGER PRIMARY KEY,
        rID INTEGER NOT NULL,
        pID INTEGER NOT NULL,
        cID INTEGER NOT NULL,
        txnID INTEGER NOT NULL,
        xmlID TEXT NOT NULL,
        date DATE NOT NULL,
        qty INTEGER NOT NULL,
        total INTEGER NOT NULL,
        currency TEXT NOT NULL,
        FOREIGN KEY (rID) REFERENCES reps(rID),
        FOREIGN KEY (pID) REFERENCES products(pID),
        FOREIGN KEY (cID) REFERENCES customers(cID)
        );"
  dbExecute(lconn, sql) 
}


## Load XML files and add data into SQLite db: Q5&6
###################################################

loadXMLData <- function(lconn) {

  library(XML)
  
  # Set wd and create path for xml files
  wd <- getwd()
  path <- paste0(wd, "/txn-xml/")
  
  # Create a list containing all the file names
  fullFileNames <- list.files(path, pattern = "*.xml", full.names=TRUE)
  
  # Initialize variables for current pID, cID, and tID maxes
  max_pID <- 100
  max_cID <- 100
  max_tID <- 100
  
  # Iterate through the files 
  for(fileName in fullFileNames) {
    baseFileName <- basename(fileName)
    print("Current file name:")
    print(baseFileName)
    
    # Create xml Object
    xmlObj <- xmlParse(fileName, validate=F)
    # get the root node of the DOM tree
    r <- xmlRoot(xmlObj)
    # get number of children of root
    numNodes <- xmlSize(r)
    
    # Using a regex to account for other versions for reps file
    if(grepl("^pharmaReps.*\\.xml$", baseFileName)) { # Deal with reps xml
      
      # Create df
      reps.df <- data.frame (
        rID = vector (mode = "numeric", length = numNodes),
        first_name = vector (mode = "character", length = numNodes),
        last_name = vector (mode = "character", length = numNodes),
        territory = vector (mode = "character", length = numNodes),
        commission = vector (mode = "numeric", length = numNodes),
        stringsAsFactors = F)
      
      # Iterate through nodes and store data in the df
      for (i in 1:numNodes)
      {
        # get next rep node
        current_rep <- r[[i]]
    
        current_rep_attr <- xmlAttrs(current_rep)
        current_rep_rID <- current_rep_attr[1]
        current_rep_territory <- xpathSApply(current_rep, "./territory", xmlValue)
        current_rep_commission <- xpathSApply(current_rep, "./commission", xmlValue)
        
        # Move down to name portion of the node
        current_rep <- xpathApply(current_rep, "./name")
        current_rep <- current_rep[[1]]
        current_rep_first_name <- xpathSApply(current_rep, "./first", xmlValue)
        current_rep_last_name <- xpathSApply(current_rep, "./sur", xmlValue)
        
        # Load data into reps df
        # Assumption: There are no repeated sales reps, so we don't need to check
        reps.df$rID[i] <- as.numeric(substring(current_rep_rID, 2, nchar(current_rep_rID)))
        reps.df$first_name[i] <- current_rep_first_name
        reps.df$last_name[i] <- current_rep_last_name
        reps.df$territory[i] <- current_rep_territory
        reps.df$commission[i] <- current_rep_commission
        
      }
      
      # Load df data into SQLite
      dbWriteTable(lconn, "reps", reps.df, overwrite = F, append = T, row.names = F)
      
    } else { # Deal with other xml data
      
      ## Create dataframes: 
      #####################
      
      # we actually do not know the number of customers or products 
      # so we cannot pre-allocate the memory for those
      
      customers.df <- data.frame (
        cID = integer(),
        name = character(),
        country = character(),
        stringsAsFactors = F)
      
      products.df <- data.frame (
        pID = integer(),
        name = character(),
        stringsAsFactors = F)
      
      sales.df <- data.frame (
        tID = vector (mode = "numeric", length = numNodes),
        rID = vector (mode = "numeric", length = numNodes),
        pID = vector (mode = "numeric", length = numNodes),
        cID = vector (mode = "numeric", length = numNodes),
        txnID = vector (mode = "numeric", length = numNodes),
        xmlID = vector (mode = "character", length = numNodes),
        date = vector (mode = "character", length = numNodes),
        qty = vector (mode = "numeric", length = numNodes),
        total = vector (mode = "numeric", length = numNodes),
        currency = vector (mode = "character", length = numNodes),
        stringsAsFactors = F)
      
      ## Extract and store XML data
      #############################
      
      # For each txn in the list of txns from this xml file
      for (i in 1:numNodes)
      {
        # get next txn node
        current_txn <- r[[i]]
        
        # get the txn attributes
        current_txn_attr <- xmlAttrs(current_txn)
        current_txn_txnID <- current_txn_attr[1]
        current_txn_repID <- current_txn_attr[2]
        
        # get remainder of elements by applying relative xpath queries
        # so the expression is run relative to the current txn node
        
        # Customer data 
        current_txn_cutomer_name <- xpathSApply(current_txn, "./customer", xmlValue)
        current_txn_customer_country <- xpathSApply(current_txn, "./country", xmlValue)
        
        # Move down to sale portion of the node
        # If we want to allow multiple sales found in any, simply move this and below to a for loops
        current_sale <- xpathApply(current_txn, "./sale")
        current_sale <- current_sale[[1]]
        
        # Products data
        current_sale_product_name <- xpathSApply(current_sale, "./product", xmlValue)
        
        # Sales data
        current_sale_tID <- i + max_tID     # tID is synthetic
        current_sale_rID <- current_txn_repID # Calculated above
        # current_sale_pID    # synthetic, determined below
        # current_sale_cID    # synthetic, determined below
        current_sale_txnID <- current_txn_txnID # Calculated above
        current_sale_xmlID <- baseFileName # store current xml file name
        current_sale_date <- xpathSApply(current_sale, "./date", xmlValue)
        current_sale_qty <- xpathSApply(current_sale, "./qty", xmlValue)
        current_sale_total <- xpathSApply(current_sale, "./total", xmlValue)
        current_sale_currency <- xpathSApply(current_sale, "./total/@currency") # Grab attr
        
        # Convert the date format
        current_sale_date <- as.Date(current_sale_date, format = "%m/%d/%Y")
        current_sale_date <- format(current_sale_date, "%Y-%m-%d")
        
        ## Add extracted values to their dfs
        ####################################
        
        # Products: If this product exists
        if(any(products.df$name==current_sale_product_name)) {
          # update cID to the one that exists already
          current_sale_pID <- products.df$pID[products.df$name==current_sale_product_name]
        } else {
          # Else this is a new product - Add the values to the df
          products_num_rows <- nrow(products.df) + 1
          current_sale_pID <- as.numeric(products_num_rows) + as.numeric(max_pID)
          new_row <- data.frame(pID = current_sale_pID, name = current_sale_product_name)
          products.df <- rbind(products.df, new_row)
        }
        
        # Customers: If this customer exists
        if(any(customers.df$name==current_txn_cutomer_name)) {
          # update cID to the one that exists already
          current_txn_cID <- customers.df$cID[customers.df$name==current_txn_cutomer_name]
        } else {
          # Else it is a new customer - Add the values to the df
          customers_num_rows <- nrow(customers.df) + 1
          current_txn_cID <- as.numeric(customers_num_rows) + as.numeric(max_cID)
          new_row <- data.frame(cID = current_txn_cID, name = current_txn_cutomer_name, country = current_txn_customer_country)
          customers.df <- rbind(customers.df, new_row)
        }
        
        # Sales: add all
        sales.df$tID[i] <- current_sale_tID
        sales.df$rID[i] <- current_sale_rID
        sales.df$pID[i] <- current_sale_pID
        sales.df$cID[i] <- current_sale_cID
        sales.df$txnID[i] <- current_sale_txnID
        sales.df$xmlID[i] <- current_sale_xmlID
        sales.df$date[i] <- current_sale_date
        sales.df$qty[i] <- current_sale_qty
        sales.df$total[i] <- current_sale_total
        sales.df$currency[i] <- current_sale_currency
        
      }
      
      # Update max pid, cid, and tid from data loaded into db
      # could have also implemented with a sql query but this is faster
      max_tID <- sales.df$tID[nrow(sales.df)]
      max_pID <- products.df$pID[nrow(products.df)]
      max_cID <- customers.df$cID[nrow(customers.df)]
      
      ## Load df into SQLite db
      #########################
      dbWriteTable(lconn, "products", products.df, overwrite = F, append = T, row.names = F)
      dbWriteTable(lconn, "customers", customers.df, overwrite = F, append = T, row.names = F)
      dbWriteTable(lconn, "sales", sales.df, overwrite = F, append = T, row.names = F)
    }
    
  }
  
  dbExecute(lconn, "PRAGMA foreign_keys = ON")
  
  # Check results
  sql <- "SELECT * FROM reps LIMIT 5"
  check <- dbGetQuery(lconn, sql)
  print(check)
  
  sql <- "SELECT * FROM products LIMIT 5"
  check <- dbGetQuery(lconn, sql)
  print(check)
  
  sql <- "SELECT * FROM customers LIMIT 5"
  check <- dbGetQuery(lconn, sql)
  print(check)
  
  sql <- "SELECT * FROM sales LIMIT 5"
  check <- dbGetQuery(lconn, sql)
  print(check)
}

main <- function() {
  
  # Call setupTables function and pass connection
  setupTables(lconn)
  
  # Call load XML function and pass connection
  loadXMLData(lconn)
}

# Call main
main()

## Clean and close
##################

# # Clear tables
# sql <- "DROP TABLE sales;"
# dbExecute(lconn, sql)
# sql <- "DROP TABLE reps;"
# dbExecute(lconn, sql)
# sql <- "DROP TABLE products;"
# dbExecute(lconn, sql)
# sql <- "DROP TABLE customers;"
# dbExecute(lconn, sql)

# Disconnect from database
dbDisconnect(lconn)
