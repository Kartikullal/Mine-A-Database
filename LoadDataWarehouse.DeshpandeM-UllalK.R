# Authors: 
#  - Madhusudan Deshpande
#  - Kartik Ullal


##############################################################################################

# The following patch installs (if not already installed) and loads packages

# Package names
packages <- c("XML", "RMySQL", "dplyr", "RSQLite")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

##############################################################################################


#Establish a connection to a sqlite database. 
#At the first run, when the sqlite database does not exist, 
#it will be automatically created

#1. Settings
db_user <- 'root'
db_password <- 'root@123'
db_name <- 'pubmed.db'
db_host <- '127.0.0.1'
db_port <- 3306

#2. Read data from db
db_conn <- dbConnect(RMySQL::MySQL(), user=db_user, password = db_password,
                     host = db_host, port = db_port)

dbExecute(db_conn, 'DROP DATABASE IF EXISTS pubmed;')

dbExecute(db_conn, 'CREATE DATABASE IF NOT EXISTS pubmed;')

dbExecute(db_conn, 'USE pubmed;')

dbExecute(db_conn, 'SET global local_infile=true;') #Allows writing to MySQL DB

dbExecute(db_conn, "DROP TABLE IF EXISTS Jounrals_Fact;")
dbExecute(db_conn, "DROP TABLE IF EXISTS Author_Fact;")
dbExecute(db_conn, "DROP TABLE IF EXISTS Date_Dim;")

sql <- "
  CREATE TABLE Jounrals_Fact(
    jounral_sk Integer,
    journal_title TEXT,
    pubDate_quarter INTEGER,
    pubDate_year INTEGER,
    article_count INTEGER,
    PRIMARY KEY(jounralID, pubDate_quarter, pubDate_year)
  )"
dbExecute(db_conn, sql)

sql <- "
  CREATE TABLE Author_Fact(
    aid Integer,
    last_name TEXT,
    fore_name TEXT,
    pubDate_month INTEGER,
    pubDate_quarter INTEGER,
    pubDate_year INTEGER,
    article_count INTEGER,
    PRIMARY KEY(aid, pubDate_quarter, pubDate_year)
  )"
dbExecute(db_conn, sql)

sql <- "
  CREATE TABLE Date_Dim(
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    PRIMARY KEY(month, quarter, year)
  )"
dbExecute(db_conn, sql)


dbListTables(db_conn);



#Define the database file
fpath = ""
dbfile = "pubmed.db"

#Connect to the database
dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile))

#Enable Foreign Key constraints check
dbExecute(dbcon, "PRAGMA foreign_keys = ON")


dbListTables(dbcon);



sql <- "
  SELECT * from Articles limit 5;
"
dbGetQuery(dbcon, sql)

sql <- "
  SELECT
    j.journal_sk,
    j.journal_title,
    CEIL(CAST(strftime('%m', ji.publication_date) as FLOAT)/3) as pubDate_quarter,
    CAST(strftime('%Y', ji.publication_date) as INTEGER) as pubDate_year,
    count(a.pmid) as article_count
  FROM Journals j
    LEFT JOIN JournalIssues ji
      ON j.journal_sk = ji.journal_sk
    LEFT JOIN Articles a
      ON a.journal_issue_id = ji.journal_issue_id
  GROUP BY 1,2,3,4
"

jounral_fact.df <- dbGetQuery(dbcon, sql)

print(jounral_fact.df)


sql <- "
  SELECT
    a.aid,
    a.last_name,
    a.fore_name,
    CAST(strftime('%m', ji.publication_date) as INTEGER) as pubDate_month,
    CEIL(CAST(strftime('%m', ji.publication_date) as FLOAT)/3) as pubDate_quarter,
    CAST(strftime('%Y', ji.publication_date) as INTEGER) as pubDate_year,
    count(ar.pmid) as article_count
  FROM Authors a
    LEFT JOIN Articles_Authors aa
      ON aa.aid = a.aid
    LEFT JOIN Articles ar
      ON ar.pmid = aa.pmid
    LEFT JOIN JournalIssues ji
      ON ar.journal_issue_id = ji.journal_issue_id
  GROUP BY 1,2,3,4,5,6
"

author_fact.df <- dbGetQuery(dbcon, sql);


#Create entries for Date_Dim table

min_pubDate_year <- min(jounral_fact.df$pubDate_year)
max_pubDate_year <- max(jounral_fact.df$pubDate_year)
nrows_required <- (max_pubDate_year-min_pubDate_year+1)*12

Date_Dim.df <- data.frame(month = vector(mode = "integer", 
                                         length = nrows_required),
                          quarter = vector(mode = "integer", 
                                         length = nrows_required),
                          year = vector(mode = "integer", 
                                         length = nrows_required),
                          stringsAsFactors = F)
          

months <- rep(seq(1,12), times = (max_pubDate_year-min_pubDate_year+1))
quarters <- ceiling(months/3)
years <- rep(min_pubDate_year:max_pubDate_year, 
             times = 1,
             each = 12)

Date_Dim.df$month <- months
Date_Dim.df$quarter <- quarters
Date_Dim.df$year <- years


#Load the tables 'jounral_fact.df' and 'author_fact.df' in MySQL database

dbWriteTable(db_conn,'Jounrals_Fact',jounral_fact.df,append=TRUE,row.names = FALSE)
dbWriteTable(db_conn,'Author_Fact',author_fact.df,append=TRUE,row.names = FALSE)
dbWriteTable(db_conn,'Date_Dim',Date_Dim.df,append=TRUE,row.names = FALSE)

