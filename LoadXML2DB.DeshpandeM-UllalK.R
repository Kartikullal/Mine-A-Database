# Authors: 
#  - Madhusudan Deshpande
#  - Kartik Ullal


##############################################################################################

# The following patch installs (if not already installed) and loads packages

# Package names
packages <- c("XML", "RMySQL", "dplyr", "RSQLite","lubridate","httr")

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
# 
#Define the database file
fpath = ""
dbfile = "pubmed.db"

#Connect to the database
dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile))

#Enable Foreign Key constraints check
dbExecute(dbcon, "PRAGMA foreign_keys = ON")

# 
# ##############################################################################################
# 
# #Part 1, Question 4&5: Design and Realize a normalized relational schema that contains 
# #the following entities/tables: Articles, Journals, Authors.
# 
# #Drop already existing tables. 
# #This is done so that the code can be rerun afresh.
# 
dbExecute(dbcon, "DROP TABLE IF EXISTS Articles_Authors;")
dbExecute(dbcon, "DROP TABLE IF EXISTS JournalIssues;")
dbExecute(dbcon, "DROP TABLE IF EXISTS Articles;")
dbExecute(dbcon, "DROP TABLE IF EXISTS Authors;")
dbExecute(dbcon, "DROP TABLE IF EXISTS Journals;")


#1. Create Articles table
sql <- "
  CREATE TABLE Articles (
    pmid TEXT PRIMARY KEY,
    article_title TEXT,
    journal_issue_id INTEGER,
    FOREIGN KEY(journal_issue_id) REFERENCES JournalIssues(journal_issue_id)
  )"
dbExecute(dbcon, sql)


#2. Create Journal Issue Table
sql <- "
  CREATE TABLE JournalIssues (
    journal_issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    citedMedium TEXT,
    volume TEXT,
    issue TEXT,
    publication_date TEXT,
    journal_sk INTEGER,
    FOREIGN KEY(journal_sk) REFERENCES Journals(journal_sk)
  )"
dbExecute(dbcon, sql)

#3. Create Journals table
sql <- "
  CREATE TABLE Journals (
    journal_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    journal_title TEXT,
    journal_iso_abbreviation TEXT
  )"
dbExecute(dbcon, sql)

#4. Create Authors table
sql <- "
  CREATE TABLE Authors (
    aid INTEGER PRIMARY KEY AUTOINCREMENT,
    author_last_name TEXT,
    author_fore_name TEXT
  )"
dbExecute(dbcon, sql)


#5. Create Articles_Authors table
sql <- "
  CREATE TABLE Articles_Authors (
    pmid TEXT,
    aid INTEGER,
    PRIMARY KEY(pmid,aid),
    FOREIGN KEY (pmid) REFERENCES Articles(pmid),
    FOREIGN KEY (aid) REFERENCES Authors(aid)
  )"
dbExecute(dbcon, sql)


##############################################################################################


# Question 6: Load (with validation of the DTD) the XML file into R 
# from a URL (or locally; but fewer points will be awarded).

require(httr)
UA <- "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36"
my_url <- 'https://raw.githubusercontent.com/Kartikullal/Mine-A-Database/main/pubmed-tfm-xml.xml'
doc <- GET(my_url, user_agent(UA))

xmlDOM <- xmlParse(content(doc, "text"), validate = T)

r<- xmlRoot(xmlDOM)

# Question 7: Extract and transform the data from the XML file into database

# get the number of articles
numArticle <- xmlSize(r)

# create data frames to store values 

# 1) Creating a main dataframe to store all values from XML file to
#     Contains all the necessary columns

main.df <- data.frame(pmid = character(),
                      article_title = character(),
                      citedMedium = character(),
                      volume = character(),
                      issue = character(),
                      publication_date = character(),
                      journal_title = character(),
                      journal_iso_abbreviation = character(),
                      author_last_name = character(),
                      author_fore_name = character(),
                      stringsAsFactors = F
)

# function to parse Date 
parseDate <- function(aDateNode){
  # input = Xml Node which has Date data
  # output = parsed date
  
  # query to get year
  query <- './Year'
  year <- xpathSApply(aDateNode, query,xmlValue)
  
  # query to get month
  query <- './Month'
  month <- xpathSApply(aDateNode, query,xmlValue)
  
  # query to get day
  query <- './Day'
  day <- xpathSApply(aDateNode, query,xmlValue)
  
  # query to get medline date
  query <- './MedlineDate'
  MedlineDate <- xpathSApply(aDateNode, query,xmlValue)
  
  # query to get season
  query <- './Season'
  Season <- xpathSApply(aDateNode, query,xmlValue)
  
  
  # if no value for day, then 01 
  if(length(day) == 0){
    day <- 01
  }
  
  
  # MedlineDate is present, then other formates are not present
  # Extracting date from Medline Date
  if(length(MedlineDate) != 0){
    
    # format 'Year month1-month2' eg. 1975 Jul-Aug
    # splitting the string into 'year' and 'month1-month2'
    MedlineDate <- strsplit(MedlineDate, split =' ')
    
    #splitting the 'month1-month2' into two months lists, and taking the first month as publication month 
    # eg. 'Jul-Aug', then month = Jul
    month <- strsplit(MedlineDate[[1]][2], split ='-')[[1]][1]
    
    # taking year from above format
    year <- MedlineDate[[1]][1]
    
  }
  
  # some nodes don't have month present
  # using season to parse month
  if(length(month) == 0){
    # if season is not present either, then month is 01
    if(length(Season) == 0){
      month <- 01
    }else{
      # if season is summer, then month is 06 (Jun)
      if(Season == 'Summer'){
        month <- 06
      }else if(Season == 'Spring'){
        # if season is Spring, then month is 03 (march)
        month <- 03
      } else if (Season == 'Fall'){
        # if season is fall then month is 08 (Sept)
        month <- 08
      } else{
        # if season is winter, month is 12 (Dec)
        month <- 12
      }
    }
  }
  
  # Using lubridate function to convert to date
  # eg. "1975/Jun/01" will become "1975/06/01"
  pub_date <- paste0(year,'/',month,'/',day)
  pub_date<-lubridate::ymd(pub_date)
  
  return(pub_date)
  
}


# function to parse JounralIssues and Journals

parseJournals <- function(aJournalNode){
  # input : Xml Node that contains journal data
  # output : list() of two df, jounralIssues and Journals
  
  # get issn from node
  query <- './ISSN'
  issn <- xpathSApply(aJournalNode, query,xmlValue)
  
  # get issntype 
  query <- './ISSN'
  issn_type <- xpathSApply(aJournalNode, query,xmlGetAttr,'IssnType') 
  
  # get title
  query <- './Title'
  title <- xpathSApply(aJournalNode, query,xmlValue)
  
  # get isoabbreviation
  query <- './ISOAbbreviation'
  ISOAbbreviation <- xpathSApply(aJournalNode, query,xmlValue)
  
  # change node to JournalIssue
  query <- './JournalIssue'
  JournalIssue <- xpathSApply(aJournalNode, query)[[1]]
  
  # get cited medium
  citedMedium <- xmlGetAttr(JournalIssue, name = 'CitedMedium')
  
  #get volume
  query <- './Volume'
  volume <- xpathSApply(JournalIssue, query, xmlValue)
  
  # get issue
  query <- './Issue'
  issue <- xpathSApply(JournalIssue, query, xmlValue)
  
  # change node to PubDate
  query <- './PubDate'
  pubDate <- xpathSApply(JournalIssue, query)[[1]]
  # get publication date by calling function
  publication_date <- parseDate(pubDate)
  
  # add empty strings to empty lists
  if(length(volume) == 0){
    volume <- ''
  }
  if(length(issue) == 0){
    issue <- ''
  }
  
  if(length(issn) == 0){
    issn <- ''
  }
  if(length(issn_type) == 0){
    issn_type <- ''
  }
  
  # create journalIssues DF
  jounral_sk <- 0
  newJounralIssues.df <- data.frame(citedMedium, volume, issue, publication_date,jounral_sk)
  
  #create jounrals df
  newJournals.df <- data.frame(title,ISOAbbreviation)
  
  return(list(newJounralIssues.df,newJournals.df))
}

# function to parse authors
parseAuthors <- function(aAuthorNode){
  # input : a xml node with authors data
  # output: a dataframe with author data
  
  # get last name
  query <- "./LastName"
  last_name <- xpathSApply(aAuthorNode, query,xmlValue)
  # use empty string for no last name
  if (length(last_name) == 0){
    last_name <- ''
  }
  
  # get forename
  query <- "./ForeName"
  fore_name <- xpathSApply(aAuthorNode, query,xmlValue)
  
  # get initials
  query <- "./Initials"
  initials <- xpathSApply(aAuthorNode, query, xmlValue)
  
  # get collective name
  query <- "./CollectiveName"
  collective_name <- xpathSApply(aAuthorNode, query,xmlValue)
  
  # forename is same as Intials, with a space in between
  # eg. forename = 'A B', Initials = "AB" 
  # therefore, if forename is empty, using Initials as forename
  if(length(fore_name) == 0){
    if(length(initials) != 0){
      fore_name <- initials
    }else{
      # if initials is also empty, then empty string
      fore_name <- ""
    }
  } 
  
  # if both last name and forename is empty, then collective name is present
  # using that for last name in such cases
  if (is.null(last_name)  & is.null(fore_name)){
    if(length(collective_name) ==0) collective_name<- ''
    last_name <- collective_name
  }
  
  # creating data frame with last_name and fore_name
  newAuthors.df <- data.frame(last_name, fore_name)
  
  return(newAuthors.df)
}





i<-1 

while ( i <= numArticle){
  
  # getting the root for article
  aArticle <- r[[i]]
  
  # reading pmid 
  pmid <- xmlGetAttr(aArticle, name = 'PMID')
  
  # reading title
  query <- './PubDetails/ArticleTitle'
  ArticleTitle <- xpathSApply(aArticle, query,xmlValue)
  
  
  
  # Get Journal Node
  query <- './PubDetails/Journal'
  journal_node <-xpathSApply(aArticle, query)
  
  # Calling function to get parsed JournalIssues and Journals df
  df <-  parseJournals(journal_node[[1]])
  newJounralIssues.df <- df[[1]]
  newJournals.df <- df[[2]]
  
  # reading the journal title and iso_abbreviation from journals df
  journal_title <- newJournals.df$title
  journal_iso_abbreviation <- newJournals.df$ISOAbbreviation
  
  
  # reading values from parsed journal_issues df
  citedMedium <- newJounralIssues.df$citedMedium
  volume <- newJounralIssues.df$volume
  issue <- newJounralIssues.df$issue
  publication_date <- as.character(newJounralIssues.df$publication_date)
  
  # get authorlist node
  query <- './PubDetails/AuthorList'
  author_node <-xpathSApply(aArticle, query)
  
  # if authorlist empty, i.e no authors mentioned, then adding NA
  if (length(author_node)!= 0){
    # get author node and number of authors
    author_node <- author_node[[1]]
    numAuthor <- xmlSize(author_node)
    
    # looping through each author node
    for(j in 1:numAuthor){
      
      # reading author data 
      aAuthor <- author_node[[j]]
      newAuthors.df <- parseAuthors(aAuthor)
      
      # storing author data
      author_last_name <- newAuthors.df$last_name
      author_first_name <- newAuthors.df$fore_name
      
      # adding new row of data to latest index
      new_row <- c(pmid,ArticleTitle,citedMedium,volume,issue,publication_date,journal_title, journal_iso_abbreviation,author_last_name,author_first_name)
      main.df[nrow(main.df)+1,] <- new_row
    }
    
  }else{
    # if authors not present, adding NA
    author_last_name <- NA
    author_first_name <- NA
    
    # adding new row to data
    new_row <- c(pmid,ArticleTitle,citedMedium,volume,issue,publication_date,journal_title, journal_iso_abbreviation,author_last_name,author_first_name)
    main.df[nrow(main.df)+1,] <- new_row
  }
  if(i%%500 == 0){
    print(paste0('Iteration: ', as.character(i)))
  }
  i <- i + 1
}

print('Done!')


# writing main dataframe to csv for storing
write.csv(main.df, "main_df.csv", row.names=FALSE)




##############################################################################################

## Splitting and storing data from main dataframe into normalized dataframe

# reading main.df
main.df <- read.csv('main_df.csv')


# 1) Authors Dataframe
authors_df <- unique(main.df[, c('author_last_name', 'author_fore_name')])
authors_df$aid <- 1:nrow(authors_df)
authors_df <- authors_df[,c('aid', 'author_last_name', 'author_fore_name')]


# 2) Journals Dataframe
journals_df <- unique(main.df[, c('journal_title', 'journal_iso_abbreviation')])
journals_df$journal_sk <- 1:nrow(journals_df)
journals_df <- journals_df[,c('journal_sk', 'journal_title', 'journal_iso_abbreviation')]

#  3) Journal Issues Dataframe
jounralIssues_df <- unique(main.df[, c('citedMedium','volume','issue','publication_date','journal_title','journal_iso_abbreviation')])
jounralIssues_df <- jounralIssues_df  %>% 
  left_join(journals_df, on=c('journal_title','journal_iso_abbreviation'))
jounralIssues_df$journal_issue_id <- 1:nrow(jounralIssues_df)
jounralIssues_df <- jounralIssues_df[,c('journal_issue_id', 'citedMedium', 'volume','issue','publication_date','journal_sk')]


# 4)  Articles DataFrame

articles_df <- unique(main.df[,c('pmid','article_title','citedMedium','volume','issue','publication_date','journal_title','journal_iso_abbreviation')])
articles_df <- articles_df  %>% 
  left_join(journals_df, on=c('journal_title','journal_iso_abbreviation'))  %>%
  left_join(jounralIssues_df, on=c( 'citedMedium', 'volume','issue','publication_date'))
articles_df <- articles_df[,c('pmid','article_title','journal_issue_id')]


# 4 ) Article_Author junction Dataframe

article_author_df <- unique(main.df[,c('pmid','author_last_name', 'author_fore_name')])
article_author_df <- article_author_df  %>% 
  left_join(authors_df, on=c('author_last_name', 'author_fore_name'))  %>%
  left_join(articles_df, on='pmid')
article_author_df <- article_author_df[,c('pmid','aid')]




# Save all the parsed data into csv file

write.csv(articles_df, "article_df.csv", row.names=FALSE)
write.csv(jounralIssues_df, "journalIssues_df.csv", row.names=FALSE)
write.csv(journals_df, "journals_df.csv", row.names=FALSE)
write.csv(authors_df, "authors_df.csv", row.names=FALSE)
write.csv(article_author_df, "articlesAuthors_df.csv", row.names=FALSE)


##############################################################################################


## loading data into database

# load the data from csv to dataframes

articles_df <- read.csv('article_df.csv')
jounralIssues_df <- read.csv('journalIssues_df.csv')
journals_df <- read.csv('journals_df.csv')
authors_df <- read.csv('authors_df.csv')
article_author_df <- read.csv('articlesAuthors_df.csv')


# 1) Journal table
dbWriteTable(dbcon,'Journals',journals_df,append=TRUE,row.names = FALSE)
sql <- "select * from Journals limit 5"
dbGetQuery(dbcon, sql)


# 2) JounralIssues table
dbWriteTable(dbcon,'JournalIssues',jounralIssues_df,append=TRUE,row.names = FALSE)
sql <- "select * from JournalIssues limit 5"
dbGetQuery(dbcon, sql)

# 3) Article table
dbWriteTable(dbcon,'Articles',articles_df,append=TRUE,row.names = FALSE)
sql <- "select * from Articles limit 5"
dbGetQuery(dbcon, sql)

# 4) Auhtors table
dbWriteTable(dbcon,'Authors',authors_df,append=TRUE,row.names = FALSE)
sql <- "select * from Authors limit 5"
dbGetQuery(dbcon, sql)

# 5) Article Authors junction table
dbWriteTable(dbcon,'Articles_Authors',article_author_df,append=TRUE,row.names = FALSE)
sql <- "select * from Articles_Authors limit 5"
dbGetQuery(dbcon, sql)


