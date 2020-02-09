test_that("ui functions work", {
  expect_identical(ui_value("foo"), "\033[34m'foo'\033[39m")
  expect_is(capture.output(ui_todo("hi")), "character")
  expect_is(capture.output(ui_done("hi")), "character")
  expect_is(capture.output(ui_line("hi")), "character")
})
