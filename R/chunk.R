#' Generate time chunks for splitting large queries
#'
#' Divides a time range into chunks (default: monthly) and returns a tibble
#' with local and UTC start/end times for each chunk.
#'
#' @param start Start time (anything accepted by [influx_parse_time()]).
#' @param end End time (anything accepted by [influx_parse_time()]).
#' @param chunk_by Chunking interval: `"month"`, `"week"`, or `"day"`.
#' @param tz Timezone for the time range.
#' @return A tibble with columns `start`, `end`, `start_utc`, `end_utc`.
#' @export
influx_chunk_range <- function(start, end, chunk_by = "month",
                               tz = "Australia/Sydney") {
  start_dt <- influx_parse_time(start, tz = tz)
  end_dt <- influx_parse_time(end, tz = tz, end_of_day = TRUE)

  # Generate sequence of chunk start times
  by_arg <- switch(chunk_by,
    month = "month",
    week  = "week",
    day   = "day",
    stop("chunk_by must be one of: 'month', 'week', 'day'", call. = FALSE)
  )

  starts <- seq.POSIXt(from = start_dt, to = end_dt, by = by_arg)

  # Build chunk ends: start of next chunk minus 1 second
  step <- switch(chunk_by,
    month = lubridate::period(1, "month"),
    week  = lubridate::weeks(1),
    day   = lubridate::days(1)
  )

  ends <- starts + step - lubridate::seconds(1)

  # Cap the final chunk at the requested end
  ends[length(ends)] <- min(ends[length(ends)], end_dt)

  tibble::tibble(
    start = starts,
    end = ends,
    start_utc = purrr::map_chr(starts, ~ influx_to_utc(.x, tz = tz)),
    end_utc = purrr::map_chr(ends, ~ influx_to_utc(.x, tz = tz))
  )
}
