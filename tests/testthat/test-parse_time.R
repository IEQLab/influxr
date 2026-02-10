test_that("influx_parse_time handles Date input", {
  d <- as.Date("2024-06-15")
  result <- influx_parse_time(d, tz = "Australia/Sydney")

  expect_s3_class(result, "POSIXct")
  expect_equal(lubridate::tz(result), "Australia/Sydney")
  expect_equal(format(result, "%H:%M:%S"), "00:00:00")
})

test_that("influx_parse_time handles Date with end_of_day", {
  d <- as.Date("2024-06-15")
  result <- influx_parse_time(d, tz = "Australia/Sydney", end_of_day = TRUE)

  expect_equal(format(result, "%H:%M:%S"), "23:59:59")
})

test_that("influx_parse_time handles POSIXct input", {
  dt <- lubridate::ymd_hms("2024-06-15 10:30:00", tz = "UTC")
  result <- influx_parse_time(dt, tz = "Australia/Sydney")

  expect_s3_class(result, "POSIXct")
  expect_equal(lubridate::tz(result), "Australia/Sydney")
  # UTC 10:30 = AEST 20:30
  expect_equal(format(result, "%H:%M:%S"), "20:30:00")
})

test_that("influx_parse_time handles character date", {
  result <- influx_parse_time("2024-06-15", tz = "Australia/Sydney")

  expect_s3_class(result, "POSIXct")
  expect_equal(as.Date(result, tz = "Australia/Sydney"), as.Date("2024-06-15"))
})

test_that("influx_parse_time handles character datetime", {
  result <- influx_parse_time("2024-06-15 14:30:00", tz = "Australia/Sydney")

  expect_s3_class(result, "POSIXct")
  expect_equal(format(result, "%H:%M:%S"), "14:30:00")
})

test_that("influx_parse_time errors on unparseable input", {
  expect_error(influx_parse_time("not-a-date"), "Cannot parse")
})

test_that("influx_parse_time errors on unsupported class", {
  expect_error(influx_parse_time(42), "Unsupported time input class")
})

test_that("influx_to_utc returns ISO8601 Z string", {
  result <- influx_to_utc("2024-06-15", tz = "Australia/Sydney")

  expect_type(result, "character")
  expect_match(result, "Z$")
  # 2024-06-15 00:00 AEST = 2024-06-14 14:00 UTC
  expect_equal(result, "2024-06-14T14:00:00Z")
})

test_that("influx_to_utc with end_of_day", {
  result <- influx_to_utc("2024-06-15", tz = "Australia/Sydney", end_of_day = TRUE)

  expect_match(result, "Z$")
  # 2024-06-15 23:59:59 AEST = 2024-06-15 13:59:59 UTC
  expect_equal(result, "2024-06-15T13:59:59Z")
})
