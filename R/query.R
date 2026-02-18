#' Escape a string for use in Flux query
#'
#' Escapes special characters for Flux string literals.
#' @param x Character vector to escape.
#' @return Escaped character vector.
#' @noRd
flux_escape <- function(x) {
  x <- gsub("\\\\", "\\\\\\\\", x)  # Backslash -> \\
  x <- gsub('"', '\\\\"', x)         # Quote -> \"
  x <- gsub("\n", "\\\\n", x)        # Newline -> \n
  x <- gsub("\r", "\\\\r", x)        # Carriage return -> \r
  x <- gsub("\t", "\\\\t", x)        # Tab -> \t
  x
}


#' Build a Flux query string
#'
#' Constructs a Flux query that filters by measurement and time range,
#' with optional tag filtering.
#'
#' @param measurement Measurement name (e.g. `"tvoc"`).
#' @param start_utc Start time as UTC ISO8601 string.
#' @param end_utc End time as UTC ISO8601 string.
#' @param bucket InfluxDB bucket name.
#' @param fields Character vector of field names to filter, or `NULL`
#'   to return all fields.
#' @param tags Optional named list of tag filters. Names are tag keys, values
#'   are character vectors of allowed values. Multiple values for a single tag
#'   are OR'd; separate tags are AND'd via separate filter steps.
#'   E.g. `list(source = "house_1", room = c("bedroom", "kitchen"))`.
#' @return A single Flux query string.
#' @examples
#' influx_build_query("temperature", "2024-01-01T00:00:00Z",
#'   "2024-02-01T00:00:00Z",
#'   tags = list(source = "house_1", room = c("bedroom", "kitchen"))
#' )
#' @export
influx_build_query <- function(measurement, start_utc, end_utc,
                               bucket = "dp23",
                               fields = "value",
                               tags = NULL) {
  # Escape all string values
  bucket_esc <- flux_escape(bucket)
  measurement_esc <- flux_escape(measurement)

  field_line <- ""
  if (!is.null(fields)) {
    fields_esc <- flux_escape(fields)
    field_filter <- paste0(
      sprintf('r._field == "%s"', fields_esc),
      collapse = " or "
    )
    field_line <- glue::glue(' |> filter(fn: (r) => {field_filter})')
  }

  tag_lines <- build_tag_filters(tags)

  query <- paste0(
    glue::glue(
      'from(bucket: "{bucket_esc}")',
      ' |> range(start: {start_utc}, stop: {end_utc})'
    ),
    field_line,
    glue::glue(' |> filter(fn: (r) => r._measurement == "{measurement_esc}")'),
    tag_lines
  )

  as.character(query)
}


#' Build Flux filter lines for tag filtering
#'
#' @param tags Named list of tag filters, or `NULL`.
#' @return A string of Flux filter lines (empty string if no tags).
#' @noRd
build_tag_filters <- function(tags) {
  if (is.null(tags) || length(tags) == 0) {
    return("")
  }

  lines <- vapply(names(tags), function(key) {
    key_esc <- flux_escape(key)
    vals <- tags[[key]]
    vals_esc <- flux_escape(vals)
    condition <- paste0(
      sprintf('r["%s"] == "%s"', key_esc, vals_esc),
      collapse = " or "
    )
    sprintf(' |> filter(fn: (r) => %s)', condition)
  }, character(1))

  paste0(lines, collapse = "")
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
    httr2::req_error(body = function(resp) httr2::resp_body_string(resp)) |>
    httr2::req_perform()

  resp_text <- httr2::resp_body_string(resp)

  # Empty response
  if (!nzchar(trimws(resp_text))) {
    return(empty_result(tz))
  }

  # Parse CSV, dropping annotation lines starting with #
  df <- suppressMessages(readr::read_csv(
    I(resp_text),
    comment = "#",
    show_col_types = FALSE
  ))

  if (nrow(df) == 0) {
    return(empty_result(tz))
  }

  # Drop InfluxDB metadata columns
  drop_cols <- intersect(names(df), c("", "result", "table", "_start", "_stop", "_field"))
  drop_cols <- c(drop_cols, grep("^\\.{3}\\d+$", names(df), value = TRUE))
  df <- dplyr::select(df, !dplyr::any_of(drop_cols))

  # Rename columns (any_of so missing columns are silently skipped)
  col_map <- c(
    datetime   = "_time",
    house      = "source",
    parameter  = "_measurement",
    device     = "entity_id",
    value      = "_value"
  )
  df <- dplyr::rename(df, dplyr::any_of(col_map))

  # Check if datetime column exists after renaming
  if (!"datetime" %in% names(df)) {
    stop(
      "Expected '_time' column not found in InfluxDB response.\n",
      "Available columns: ", paste(names(df), collapse = ", "),
      "\n\nThis might indicate a malformed query or InfluxDB error."
    )
  }

  # Parse datetime column if it's character, then convert to local timezone
  df <- dplyr::mutate(
    df,
    datetime = if (inherits(.data$datetime, "POSIXct")) {
      lubridate::with_tz(.data$datetime, tzone = tz)
    } else {
      # Parse as ISO8601 UTC timestamp, then convert to target timezone
      lubridate::with_tz(lubridate::ymd_hms(.data$datetime, tz = "UTC"), tzone = tz)
    }
  )

  df
}


#' Create an empty result tibble with correct column types
#' @param tz Timezone for datetime column.
#' @return An empty tibble.
#' @noRd
empty_result <- function(tz) {
  tibble::tibble(
    datetime   = as.POSIXct(character(), tz = tz),
    parameter  = character(),
    value      = numeric()
  )
}
