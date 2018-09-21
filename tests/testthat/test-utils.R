context("utils")

test_that("utils", {
  expect_false(is_sqlite_connection(1))
})
