is_sqlite_connection <- function(x = getOption("ps.conn")) inherits(x, "SQLiteConnection")
