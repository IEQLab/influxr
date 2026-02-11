test_that("influx_build_query produces correct baseline output", {
  q <- influx_build_query("temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z")

  expect_match(q, 'r._measurement == "temp"')
  # Default field filter is "value"
  expect_match(q, 'r._field == "value"')
  # No tag filter lines
  expect_no_match(q, 'r\\["')
})

test_that("fields filter is skipped when fields = NULL", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    fields = NULL
  )

  expect_no_match(q, 'r._field')
})

test_that("multiple fields are OR'd", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    fields = c("value", "temperature")
  )

  expect_match(q, 'r._field == "value" or r._field == "temperature"')
})

test_that("single tag with one value adds correct filter line", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    tags = list(source = "house_1")
  )

  expect_match(q, 'filter\\(fn: \\(r\\) => r\\["source"\\] == "house_1"\\)')
})

test_that("single tag with multiple values ORs them", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    tags = list(room = c("bedroom", "kitchen"))
  )

  expect_match(q, 'r\\["room"\\] == "bedroom" or r\\["room"\\] == "kitchen"')
})

test_that("multiple tags produce separate filter lines", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    tags = list(source = "house_1", room = "bedroom")
  )

  expect_match(q, 'filter\\(fn: \\(r\\) => r\\["source"\\] == "house_1"\\)')
  expect_match(q, 'filter\\(fn: \\(r\\) => r\\["room"\\] == "bedroom"\\)')
})
