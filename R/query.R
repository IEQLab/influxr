#' Build a Flux query string
#'
#' Constructs a Flux query that filters by measurement and time range.
#'
#' @param measurement Measurement name (e.g. `"tvoc"`).
#' @param start_utc Start time as UTC ISO8601 string.
#' @param end_utc End time as UTC ISO8601 string.
#' @param bucket InfluxDB bucket name.
#' @param fields Character vector of field names to filter.
#' @return A single Flux query string.
#' @export
influx_build_query <- function(measurement, start_utc, end_utc,
                               bucket = "dp23",
                               fields = c("value", "temperature", "humidity")) {
  field_filter <- paste0(
    sprintf('r._field == "%s"', fields),
    collapse = " or "
  )

  query <- glue::glue(
    'from(bucket: "{bucket}")',
    ' |> range(start: {start_utc}, stop: {end_utc})',
    ' |> filter(fn: (r) => {field_filter})',
    ' |> filter(fn: (r) => r._measurement == "{measurement}")',
    ' |> keep(columns: ["_time","source", "_measurement", "_field", "entity_id", "_value"])'
  )

  as.character(query)
}


#' Execute a Flux query against InfluxDB v2
#'
#' Sends a raw Flux query via the InfluxDB v2 HTTP API and returns the
#' results as a tibble with renamed and timezone-converted columns.
#'
#' @param query A Flux query string.
#' @param config Connection config from [influx_config()].
#' @param tz Timezone for the returned `datetime` column.
#' @return A tibble with columns: `datetime`, `house`, `parameter`,
#'   `device`, `value`, `value_type`.
#' @export
influx_query <- function(query, config = influx_config(),
                         tz = "Australia/Sydney") {
  # Build and execute HTTP request
  resp <- httr2::request(config$url) |>
    httr2::req_url_path_append("api", "v2", "query") |>
    httr2::req_url_query(org = config$org) |>
    httr2::req_headers(
      Authorization = paste("Token", config$token),
      `Content-type` = "application/vnd.flux",
      Accept = "application/csv"
    ) |>
    httr2::req_body_raw(query, type = "application/vnd.flux") |>
    httr2::req_perform()

  resp_text <- httr2::resp_body_string(resp)

  # Empty response
  if (!nzchar(trimws(resp_text))) {
    return(empty_result(tz))
  }

  # Parse CSV, dropping annotation lines starting with #
  df <- readr::read_csv(
    I(resp_text),
    comment = "#",
    show_col_types = FALSE
  )

  if (nrow(df) == 0) {
    return(empty_result(tz))
  }

  # Drop leading empty column and result/table columns
  drop_cols <- intersect(names(df), c("", "result", "table"))
  df <- dplyr::select(df, !dplyr::any_of(drop_cols))

  # Rename columns
  df <- dplyr::rename(
    df,
    datetime = "_time",
    house = "source",
    parameter = "_measurement",
    device = "entity_id",
    value = "_value",
    value_type = "_field"
  )

  # Convert datetime to local timezone
  df <- dplyr::mutate(
    df,
    datetime = lubridate::with_tz(.data$datetime, tzone = tz)
  )

  df
}


#' Create an empty result tibble with correct column types
#' @param tz Timezone for datetime column.
#' @return An empty tibble.
#' @noRd
empty_result <- function(tz) {
  tibble::tibble(
    datetime = as.POSIXct(character(), tz = tz),
    house = character(),
    parameter = character(),
    device = character(),
    value = numeric(),
    value_type = character()
  )
}
