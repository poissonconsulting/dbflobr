% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dbflob.R
\name{write_flob}
\alias{write_flob}
\title{Write flob}
\usage{
write_flob(flob, column_name, table_name, exists, key, conn)
}
\arguments{
\item{flob}{A flob.}

\item{column_name}{A string of the name of the BLOB column.}

\item{table_name}{A string of the name of the table.}

\item{exists}{A flag specifying whether the column must already exist.
IF FALSE, a new BLOB column is created.}

\item{key}{A data.frame with column names and values which filter table to a single row.}

\item{conn}{A connection object.}
}
\value{
Modified database.
}
\description{
Write a flob to column of type BLOB in database.
}