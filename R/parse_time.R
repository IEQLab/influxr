#' Parse a time input to POSIXct in a given timezone
#'
#' Accepts Date, POSIXct, or character inputs and returns a POSIXct value
#' in the specified timezone.
#'
#' @param x A Date, POSIXct, or character time value.
#' @param tz Target timezone (default `"Australia/Sydney"`).
#' @param end_of_day If `TRUE` and `x` is a Date or date-only character,
#'   set time to 23:59:59 instead of 00:00:00.
#' @return A POSIXct value in `tz`.
#' @export
influx_parse_time <- function(x, tz = "Australia/Sydney", end_of_day = FALSE) {
  if (inherits(x, "POSIXct")) {
    return(lubridate::with_tz(x, tzone = tz))
  }

  if (inherits(x, "Date")) {
    out <- lubridate::force_tz(as.POSIXct(x), tzone = tz)
    if (end_of_day) {
      out <- out + lubridate::hours(23) + lubridate::minutes(59) + lubridate::seconds(59)
    }
    return(out)
  }

  if (is.character(x)) {
    # Try common formats
    parsed <- lubridate::parse_date_time(
      x,
      orders = c("ymd HMS", "ymd HM", "ymd", "dmy HMS", "dmy"),
      tz = tz,
      quiet = TRUE
    )
    if (is.na(parsed)) {
      stop("Cannot parse time value: \"", x, "\"", call. = FALSE)
    }
    if (end_of_day && format(parsed, "%H:%M:%S") == "00:00:00") {
      parsed <- parsed + lubridate::hours(23) + lubridate::minutes(59) + lubridate::seconds(59)
    }
    return(parsed)
  }

  stop("Unsupported time input class: ", class(x)[1], call. = FALSE)
}


#' Convert a time input to a UTC ISO8601 string
#'
#' Parses `x` via [influx_parse_time()] then converts to UTC and formats
#' as an ISO8601 string with a `"Z"` suffix, suitable for Flux queries.
#'
#' @inheritParams influx_parse_time
#' @return A character string in ISO8601 UTC format (e.g. `"2024-06-01T00:00:00Z"`).
#' @export
influx_to_utc <- function(x, tz = "Australia/Sydney", end_of_day = FALSE) {
  parsed <- influx_parse_time(x, tz = tz, end_of_day = end_of_day)
  utc <- lubridate::with_tz(parsed, tzone = "UTC")
  format(utc, "%Y-%m-%dT%H:%M:%SZ")
}
