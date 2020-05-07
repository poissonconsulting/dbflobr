column_names <- function(table_name, conn) {
  DBI::dbListFields(conn, table_name)
}

column_exists <- function(column_name, table_name, conn) {
  columns <- column_names(table_name, conn)
  to_upper(column_name) %in% to_upper(columns)
}

table_names <- function(conn) {
  DBI::dbListTables(conn)
}

table_exists <- function(table_name, conn) {
  tables <- table_names(conn)
  to_upper(table_name) %in% to_upper(tables)
}

get_query <- function(sql, conn) {
  DBI::dbGetQuery(conn, sql)
}

execute <- function(sql, conn) {
  DBI::dbExecute(conn, sql)
}

sql_interpolate <- function(sql, conn, ...) {
  DBI::sqlInterpolate(conn, sql, ...)
}

table_info <- function(table_name, conn) {
  sql <- glue("PRAGMA table_info('{table_name}');")
  table_info <- get_query(sql, conn)
  table_info
}

table_pk <- function(table_name, conn){
  info <- table_info(table_name, conn)
  info$name[info$pk > 0]
}

table_pk_df <- function(table_name, conn){
  info <- table_info(table_name, conn)
  pk <- info$name[info$pk > 0]
  key <- data.frame(matrix(ncol = length(pk), nrow = 1, dimnames = list(NULL, pk)))
  for(i in pk){
    type <- info$type[info$name == i]
    x <- switch (type,
      "TEXT" = character(),
      "INTEGER" = integer(),
      "BOOLEAN" = logical(),
      "REAL" = numeric(),
      logical()
    )
    key[i] <- x
  }
  key[0,,drop = FALSE]
}

sql_pk <- function(x){
  paste0("`", paste(x, collapse = "`, `"), "`")
}

table_column_type <- function(column_name, table_name, conn) {
  table_info <- table_info(table_name, conn)
  table_info$type[to_upper(table_info$name) == to_upper(column_name)]
}

is_column_blob <- function(column_name, table_name, conn) {
  toupper(table_column_type(column_name, table_name, conn)) == "BLOB"
}

blob_columns <- function(table_name, conn){
  table_info <- table_info(table_name, conn)
  table_info$name[table_info$type == "BLOB"]
}

# prevents injection attack from values
safe_key <- function(key, conn) {
  key <- lapply(colnames(key), function(y) {
    value <- key[, y]
    sql <- glue_sql("{`y`} = ?value", .con = conn)
    sql_null <- glue_sql("{`y`} IS NULL", .con = conn)
    if (is.na(value)) {
      return(sql_interpolate(sql_null, conn))
    }
    sql_interpolate(sql, conn,
      value = value
    )
  })
  glue_collapse(key, " AND ")
}

filter_key <- function(table_name, key, conn) {
  sql <- glue("SELECT * FROM ?table_name WHERE {safe_key(key, conn)}")
  sql <- sql_interpolate(sql, conn,
    table_name = table_name
  )
  get_query(sql, conn)
}

query_flob <- function(column_name, table_name, key, conn) {
  sql <- glue_sql("SELECT {`column_name`} FROM {`table_name`} WHERE",
    column_name = column_name,
    table_name = table_name,
    .con = conn
  )
  sql <- glue("{sql} {safe_key(key, conn)}")

  x <- get_query(sql, conn)
  unlist(x, recursive = FALSE)
}
