test_that("influx_build_query without tags matches baseline output", {
  q <- influx_build_query("temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z")

  expect_match(q, 'r._measurement == "temp"')
  expect_match(q, 'keep\\(columns: \\[')
  # No tag filter lines
  expect_no_match(q, 'r\\["')
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

test_that("tag names appear in keep() columns", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    tags = list(room = "bedroom")
  )

  expect_match(q, '"room"')
  # Core columns still present
  expect_match(q, '"_time"')
  expect_match(q, '"_value"')
})

test_that("tags already in keep() are not duplicated", {
  q <- influx_build_query(
    "temp", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z",
    tags = list(source = "house_1")
  )

  # "source" should appear exactly once in the keep() columns
  keep_match <- regmatches(q, regexpr("keep\\(columns: \\[[^]]+\\]\\)", q))
  source_count <- lengths(regmatches(keep_match, gregexpr('"source"', keep_match)))
  expect_equal(source_count, 1)
})
