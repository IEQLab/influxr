test_that("influx_chunk_range produces monthly chunks", {
  chunks <- influx_chunk_range(
    start = "2024-06-01",
    end = "2024-08-15",
    chunk_by = "month",
    tz = "Australia/Sydney"
  )

  expect_s3_class(chunks, "tbl_df")
  expect_equal(nrow(chunks), 3) # June, July, partial August
  expect_named(chunks, c("start", "end", "start_utc", "end_utc"))

  # All start/end should be POSIXct

  expect_s3_class(chunks$start, "POSIXct")
  expect_s3_class(chunks$end, "POSIXct")

  # UTC columns should be character

  expect_type(chunks$start_utc, "character")
  expect_type(chunks$end_utc, "character")
})

test_that("influx_chunk_range caps final chunk at end date", {
  chunks <- influx_chunk_range(
    start = "2024-06-01",
    end = "2024-06-15",
    chunk_by = "month",
    tz = "Australia/Sydney"
  )

  expect_equal(nrow(chunks), 1)

  # End should be June 15, not June 30
  expect_equal(as.Date(chunks$end[1]), as.Date("2024-06-15"))
})

test_that("influx_chunk_range works with daily chunks", {
  chunks <- influx_chunk_range(
    start = "2024-06-01",
    end = "2024-06-03",
    chunk_by = "day",
    tz = "Australia/Sydney"
  )

  expect_equal(nrow(chunks), 3)
})

test_that("influx_chunk_range errors on invalid chunk_by", {
  expect_error(
    influx_chunk_range("2024-06-01", "2024-06-30", chunk_by = "year"),
    "chunk_by must be"
  )
})

test_that("influx_chunk_range UTC values end with Z", {
  chunks <- influx_chunk_range(
    start = "2024-06-01",
    end = "2024-06-30",
    chunk_by = "month",
    tz = "Australia/Sydney"
  )

  expect_match(chunks$start_utc[1], "Z$")
  expect_match(chunks$end_utc[1], "Z$")
})
