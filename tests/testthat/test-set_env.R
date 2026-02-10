test_that("influx_set_env writes new vars to a fresh .Renviron", {
  tmp <- withr::local_tempfile()
  withr::local_envvar(INFLUXDB_URL = NA, INFLUXDB_TOKEN = NA, INFLUXDB_ORG = NA)

  expect_message(
    result <- influx_set_env(
      url = "http://localhost:8086", token = "my-token", org = "my-org",
      renviron_path = tmp
    ),
    "Set InfluxDB environment variables"
  )

  lines <- readLines(tmp)
  expect_true(any(grepl("^INFLUXDB_URL=http://localhost:8086$", lines)))
  expect_true(any(grepl("^INFLUXDB_TOKEN=my-token$", lines)))
  expect_true(any(grepl("^INFLUXDB_ORG=my-org$", lines)))

  expect_equal(Sys.getenv("INFLUXDB_URL"), "http://localhost:8086")
  expect_equal(Sys.getenv("INFLUXDB_TOKEN"), "my-token")
  expect_equal(Sys.getenv("INFLUXDB_ORG"), "my-org")

  expect_equal(result$url, "http://localhost:8086")
})

test_that("influx_set_env updates existing vars without clobbering other content", {
  tmp <- withr::local_tempfile()
  writeLines(c("FOO=bar", "INFLUXDB_URL=old-url", "BAZ=qux"), tmp)
  withr::local_envvar(INFLUXDB_URL = NA, INFLUXDB_TOKEN = NA, INFLUXDB_ORG = NA)

  expect_message(
    influx_set_env(
      url = "http://new:8086", token = "new-token", org = "new-org",
      renviron_path = tmp
    ),
    "Set InfluxDB environment variables"
  )

  lines <- readLines(tmp)
  expect_true(any(grepl("^FOO=bar$", lines)))
  expect_true(any(grepl("^BAZ=qux$", lines)))
  expect_true(any(grepl("^INFLUXDB_URL=http://new:8086$", lines)))
  expect_true(any(grepl("^INFLUXDB_TOKEN=new-token$", lines)))
  expect_true(any(grepl("^INFLUXDB_ORG=new-org$", lines)))
  expect_false(any(grepl("old-url", lines)))
})

test_that("influx_set_env errors in non-interactive mode when args are missing", {
  expect_error(
    influx_set_env(),
    "INFLUXDB_URL must be provided"
  )
  expect_error(
    influx_set_env(url = "http://localhost:8086"),
    "INFLUXDB_TOKEN must be provided"
  )
  expect_error(
    influx_set_env(url = "http://localhost:8086", token = "tok"),
    "INFLUXDB_ORG must be provided"
  )
})

test_that("influx_set_env masks token in message", {
  tmp <- withr::local_tempfile()
  withr::local_envvar(INFLUXDB_URL = NA, INFLUXDB_TOKEN = NA, INFLUXDB_ORG = NA)

  msgs <- capture.output(
    influx_set_env(
      url = "http://localhost:8086", token = "abcdefgh1234", org = "my-org",
      renviron_path = tmp
    ),
    type = "message"
  )
  token_msg <- grep("INFLUXDB_TOKEN", msgs, value = TRUE)
  expect_false(grepl("abcdefgh1234", token_msg))
  expect_true(grepl("\\*+1234", token_msg))
})
