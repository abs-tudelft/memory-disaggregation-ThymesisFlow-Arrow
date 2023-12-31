% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/table.R
\name{as_arrow_table}
\alias{as_arrow_table}
\alias{as_arrow_table.default}
\alias{as_arrow_table.Table}
\alias{as_arrow_table.RecordBatch}
\alias{as_arrow_table.data.frame}
\alias{as_arrow_table.RecordBatchReader}
\alias{as_arrow_table.arrow_dplyr_query}
\alias{as_arrow_table.Schema}
\title{Convert an object to an Arrow Table}
\usage{
as_arrow_table(x, ..., schema = NULL)

\method{as_arrow_table}{default}(x, ...)

\method{as_arrow_table}{Table}(x, ..., schema = NULL)

\method{as_arrow_table}{RecordBatch}(x, ..., schema = NULL)

\method{as_arrow_table}{data.frame}(x, ..., schema = NULL)

\method{as_arrow_table}{RecordBatchReader}(x, ...)

\method{as_arrow_table}{arrow_dplyr_query}(x, ...)

\method{as_arrow_table}{Schema}(x, ...)
}
\arguments{
\item{x}{An object to convert to an Arrow Table}

\item{...}{Passed to S3 methods}

\item{schema}{a \link{Schema}, or \code{NULL} (the default) to infer the schema from
the data in \code{...}. When providing an Arrow IPC buffer, \code{schema} is required.}
}
\value{
A \link{Table}
}
\description{
Whereas \code{\link[=arrow_table]{arrow_table()}} constructs a table from one or more columns,
\code{as_arrow_table()} converts a single object to an Arrow \link{Table}.
}
\examples{
# use as_arrow_table() for a single object
as_arrow_table(data.frame(col1 = 1, col2 = "two"))

# use arrow_table() to create from columns
arrow_table(col1 = 1, col2 = "two")

}
