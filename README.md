
<!-- README.md is generated from README.Rmd. Please edit that file -->

<!-- badges: start -->

[![lifecycle](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
[![Travis build
status](https://travis-ci.com/poissonconsulting/dbflobr.svg?branch=master)](https://travis-ci.com/poissonconsulting/dbflobr)
[![AppVeyor build
status](https://ci.appveyor.com/api/projects/status/github/poissonconsulting/dbflobr?branch=master&svg=true)](https://ci.appveyor.com/project/poissonconsulting/dbflobr)
[![Coverage
status](https://codecov.io/gh/poissonconsulting/dbflobr/branch/master/graph/badge.svg)](https://codecov.io/github/poissonconsulting/dbflobr?branch=master)
[![License:
MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
<!-- badges: end -->

# dbflobr

`dbflobr` reads and writes files to databases as
[flobs](https://poissonconsulting.github.io/flobr/reference/flob.html).
A flob is a special type of BLOB that includes the file extension type.

## Installation

To install the latest development version from
[GitHub](https://github.com/poissonconsulting/dbflobr)

    # install.packages("devtools")
    devtools::install_github("poissonconsulting/checkr")
    devtools::install_github("poissonconsulting/err")
    devtools::install_github("poissonconsulting/flobr")
    devtools::install_github("poissonconsulting/dbflobr")

To install the latest development version from the Poisson
[drat](https://github.com/poissonconsulting/drat) repository.

    # install.packages("drat")
    drat::addRepo("poissonconsulting")
    install.packages("dbflobr")

## Usage

Create a connection and populate it with a table of fake data

``` r
library(dbflobr)
library(flobr)
library(DBI)

conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

df <- data.frame(char = c("a", "b", "b"),
                   num = c(1.1, 2.2, 2.2),
                   stringsAsFactors = FALSE)

DBI::dbWriteTable(conn, "df", df)
```

Add a flob to a newly created BLOB column called ‘file’

``` r
# use demo flob from flobr package
x <- flobr::flob_obj

# a new BLOB column is created when exists = FALSE
# specify which observation to add the flob to by providing a key. 

key <- data.frame(num = 1.1)
write_flob(x, column_name = "file", table_name = "df", exists = FALSE, key = key, conn = conn)

# read flob
x <- read_flob(column_name = "file", table_name = "df", key = key, conn = conn)
str(x)
#> List of 1
#>  $ file: raw [1:133836] 58 0a 00 00 ...
#>  - attr(*, "class")= chr [1:2] "flob" "blob"

DBI::dbDisconnect(conn)
```

## Contribution

Please report any
[issues](https://github.com/poissonconsulting/dbflobr/issues).

[Pull requests](https://github.com/poissonconsulting/dbflobr/pulls) are
always welcome.

Please note that the ‘dbflobr’ project is released with a [Contributor
Code of Conduct](CODE_OF_CONDUCT.md). By contributing to this project,
you agree to abide by its terms.

## Creditation

  - [blob](https://github.com/tidyverse/blob)
  - [flobr](https://github.com/poissonconsulting/flobr)
