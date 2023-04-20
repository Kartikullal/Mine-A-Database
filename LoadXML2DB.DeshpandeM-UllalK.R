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

#Define the database file
fpath = ""
dbfile = "pubmed.db"

#Connect to the database
dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath,dbfile))

#Enable Foreign Key constraints check
dbExecute(dbcon, "PRAGMA foreign_keys = ON")


##############################################################################################

#Part 1, Question 4&5: Design and Realize a normalized relational schema that contains 
#the following entities/tables: Articles, Journals, Authors.

#Drop already existing tables. 
#This is done so that the code can be rerun afresh.

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
    last_name TEXT,
    fore_name TEXT
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
my_url <- 'https://raw.githubusercontent.com/Kartikullal/Mine-A-Database/main/pubmed22n0001-tf-sample.xml'
doc <- GET(my_url, user_agent(UA))

xmlDOM <- xmlParse(content(doc, "text"), validate = T)

r<- xmlRoot(xmlDOM)

# Question 7: Extract and transform the data from the XML file into database

# get the number of articles
numArticle <- xmlSize(r)

# create data frames to store values 

# 1) Articles Dataframe

# Can use pre defiend length here 
article.df <- data.frame(pmid = vector(mode = "character",
                                       length = numArticle),
                         article_title = vector(mode = 'character',
                                                length = numArticle),
                         journal_issue_id = vector(mode ='integer',
                                                   length = numArticle),
                         stringsAsFactors = F)

# Following dataframes would require custom sizes, as we don't know how many values there are.
# 2) Jounral Issues DataFrame
journalIssues.df <- data.frame(journal_issue_id = integer(),
                               citedMedium = character(),
                               volume = character(),
                               issue = character(),
                               publication_date = character(),
                               journal_sk = integer(),
                               stringsAsFactors = F
                               )

# 3) Journals DataFrame
journals.df <- data.frame(journal_sk = integer(),
                          journal_title = character(),
                          journal_iso_abbreviation = character(),
                          stringsAsFactors = F)

# 4) Authors Datframe
authors.df <- data.frame( aid = integer(),
                          last_name = character(),
                          fore_name = character(),
                          stringsAsFactors = F)

# 5) articles and authors junction dataframe 
articlesAuthors.df <- data.frame(pmid = character(),
                                 aid = integer(),
                                 stringsAsFactors = F)


# Helper function to check if a row already exists in the dataframe

# if existis, returns the index, else returns last index

rowExists <- function (aRow, aDF)
{
  # input =  two dataframes
  # output = 0 if no match, else index of row that matched
  
  # check if that address is already in the data frame
  n <- nrow(aDF)
  c <- ncol(aDF)
  if (n == 0)
  {
    # data frame is empty, so can't exist
    return(0)
  }
  
  for (a in 1:n)
  {

    # check if all columns match for a row; ignore the aID column
    if (all(aDF[a,] == aRow[1,]))
    {
      # found a match; return it's ID
      return(a)
    }
  }
  
  # none matched
  return(0)
}


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


# Reading previously stored data 
# This is just to parsr main file in batches since it takes a lot of time, is not part of the main code
# We can load the data, and start from last index in article df

#article.df <- read.csv('article_df.csv')
#journalIssues.df <- read.csv('journalIssues_df.csv')
#journals.df <- read.csv('journals_df.csv')
#authors.df <- read.csv('authors_df.csv')
#articlesAuthors.df <- read.csv('articlesAuthors_df.csv')




startIndex <- 1

# To get the next start index for loading data

#if(is.na(which(is.na(article.df$pmid))[1])){ 
#  startIndex <- which(article.df$pmid == '')[1]
#} else startIndex<-which(is.na(article.df$pmid))[1]


# Main loop to parse xml and store in dataframe

for( i in startIndex:numArticle){
  
  # get article node
  aArticle <- r[[i]]
  
  # reading pmid and store in article.df
  pmid <- xmlGetAttr(aArticle, name = 'PMID')
  article.df$pmid[i] <- pmid
  
  # reading title and storing in article.df
  query <- './PubDetails/ArticleTitle'
  ArticleTitle <- xpathSApply(aArticle, query,xmlValue)
  article.df$article_title[i] <- ArticleTitle
  
  # Get Journal Node
  query <- './PubDetails/Journal'
  journal_node <-xpathSApply(aArticle, query)
 
  # Calling function to get parsed JournalIssues and Journals df
  df <-  parseJournals(journal_node[[1]])
  newJounralIssues.df <- df[[1]]
  newJournals.df <- df[[2]]
  

  # check if the row from parsed dataframe is in original dataframe
  pk.Journals <- rowExists(newJournals.df, journals.df[,2:ncol(journals.df)])
  if (pk.Journals == 0)
  {
    # create new journal id
    pk.Journals <- nrow(journals.df) + 1
    # add rest of the columns from parsed dataframe
    journals.df[pk.Journals,2:ncol(journals.df)] <- newJournals.df[1,]
    journals.df[pk.Journals,1] <- pk.Journals
  }


  # create new journal issues id 
  newJounralIssues.df[1,ncol(newJounralIssues.df)] <- pk.Journals
  
  pk.JounralIssues <- rowExists(newJounralIssues.df, journalIssues.df[,2:ncol(journalIssues.df)])
  if(pk.JounralIssues == 0){
    
    pk.JounralIssues <- nrow(journalIssues.df) + 1
    # add rest of the values from parsed dataframe to main journalIssues dataframe
    journalIssues.df[pk.JounralIssues,2] <- newJounralIssues.df$citedMedium
    journalIssues.df[pk.JounralIssues,3] <- newJounralIssues.df$volume
    journalIssues.df[pk.JounralIssues,4] <- newJounralIssues.df$issue
    journalIssues.df[pk.JounralIssues,5] <- as.character(newJounralIssues.df$publication_date)
    journalIssues.df[pk.JounralIssues,6] <- pk.Journals
    journalIssues.df[pk.JounralIssues,1] <- pk.JounralIssues
  }
  article.df$journal_issue_id[i] <- pk.JounralIssues

  

  
  # get authorlist node
  query <- './PubDetails/AuthorList'
  author_node <-xpathSApply(aArticle, query)
  
  # if empty, that means authors are not present
  # so no entry is added into authors table
  # article_author table has only pmid with NA for author id
  if (length(author_node)!= 0){
    
    # get author node and number of authors
    author_node <- author_node[[1]]
    numAuthor <- xmlSize(author_node)
  
    # for each author, call parseAuthor function to get author details
    for(j in 1:numAuthor){
      aAuthor <- author_node[[j]]
      newAuthors.df <- parseAuthors(aAuthor)
      
      # check if author details are already present
      pk.Authors <- rowExists(newAuthors.df, authors.df[,2:ncol(authors.df)])
      if(pk.Authors == 0){
        # if not, then create a aid and add the details
        pk.Authors <- nrow(authors.df) + 1
        authors.df[pk.Authors,2:ncol(authors.df)] <- newAuthors.df[1,]
        authors.df[pk.Authors,1] <- pk.Authors
      }
      
      # add the corresponding entry in articleAuthors dataframe
      row_id <- nrow(articlesAuthors.df) + 1
      articlesAuthors.df[row_id, 1] <- pmid
      articlesAuthors.df[row_id, 2] <- pk.Authors
    }
  } else{
    # this is if no authors are present, then NA is added for aid
    articlesAuthors.df[row_id, 1] <- pmid
    articlesAuthors.df[row_id, 2] <- NA
  }
  
  # just to print number of instances parsed
  if(as.integer(i)%%500 == 0){
    print(paste0('Pmid Number is ', pmid))
  }
}

print('Done!')

# Save all the parsed data into csv file


write.csv(article.df, "article_df.csv", row.names=FALSE)
write.csv(journalIssues.df, "journalIssues_df.csv", row.names=FALSE)
write.csv(journals.df, "journals_df.csv", row.names=FALSE)
write.csv(authors.df, "authors_df.csv", row.names=FALSE)
write.csv(articlesAuthors.df, "articlesAuthors_df.csv", row.names=FALSE)


# load the data from csv to dataframes

#article.df <- read.csv('article_df.csv')
#journalIssues.df <- read.csv('journalIssues_df.csv')
#journals.df <- read.csv('journals_df.csv')
#authors.df <- read.csv('authors_df.csv')
#articlesAuthors.df <- read.csv('articlesAuthors_df.csv')


print(journalIssues.df)

print(journals.df)
# write data from dataframe to db


# 1) Journal table
dbWriteTable(dbcon,'Journals',journals.df,append=TRUE,row.names = FALSE)
sql <- "select * from Journals limit 5"
dbGetQuery(dbcon, sql)


# 2) JounralIssues table
dbWriteTable(dbcon,'JournalIssues',journalIssues.df,append=TRUE,row.names = FALSE)
sql <- "select * from JournalIssues limit 5"
dbGetQuery(dbcon, sql)

# 3) Article table
dbWriteTable(dbcon,'Articles',article.df,append=TRUE,row.names = FALSE)
sql <- "select * from Articles limit 5"
dbGetQuery(dbcon, sql)

# 4) Auhtors table
dbWriteTable(dbcon,'Authors',authors.df,append=TRUE,row.names = FALSE)
sql <- "select * from Authors limit 5"
dbGetQuery(dbcon, sql)

# 5) Article Authors junction table
dbWriteTable(dbcon,'Articles_Authors',articlesAuthors.df,append=TRUE,row.names = FALSE)
sql <- "select * from Articles_Authors limit 5"
dbGetQuery(dbcon, sql)

