#' Read cached InfluxDB data from compressed CSV files
#'
#' Reads `.csv.gz` files saved by [influx_get_range()] or [influx_update()],
#' optionally filtering by measurement name and time range.
#'
#' @param measurements Character vector of measurement names to read.
#'   If `NULL`, reads all available files.
#' @param start Optional start time filter.
#' @param end Optional end time filter.
#' @param data_dir Directory containing `.csv.gz` files.
#' @param tz Timezone for datetime conversion and filtering.
#' @return A tibble of cached data.
#' @export
influx_read_cached <- function(measurements = NULL,
                               start = NULL, end = NULL,
                               data_dir = "data/raw/influx",
                               tz = "Australia/Sydney") {
  if (!dir.exists(data_dir)) {
    stop("Data directory does not exist: ", data_dir, call. = FALSE)
  }

  files <- list.files(data_dir, pattern = "\\.csv\\.gz$", full.names = TRUE)

  if (length(files) == 0) {
    message("No cached files found in ", data_dir)
    return(empty_result(tz))
  }

  # Filter by measurement name
  if (!is.null(measurements)) {
    pattern <- paste0("(", paste(measurements, collapse = "|"), ")_")
    files <- files[stringr::str_detect(basename(files), pattern)]

    if (length(files) == 0) {
      message("No cached files found for: ", paste(measurements, collapse = ", "))
      return(empty_result(tz))
    }
  }

  # Read and combine all matching files
  df <- purrr::map(files, ~ readr::read_csv(.x, show_col_types = FALSE)) |>
    dplyr::bind_rows()

  if (nrow(df) == 0) {
    return(empty_result(tz))
  }

  # Ensure datetime is in correct timezone
  df <- dplyr::mutate(
    df,
    datetime = lubridate::with_tz(.data$datetime, tzone = tz)
  )

  # Apply time filters
  if (!is.null(start)) {
    start_dt <- influx_parse_time(start, tz = tz)
    df <- dplyr::filter(df, .data$datetime >= start_dt)
  }

  if (!is.null(end)) {
    end_dt <- influx_parse_time(end, tz = tz, end_of_day = TRUE)
    df <- dplyr::filter(df, .data$datetime <= end_dt)
  }

  df
}
