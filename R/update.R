#' Determine the last timestamp for a measurement
#'
#' Finds the most recent data point for a measurement, either from saved
#' files or from an in-memory data frame.
#'
#' @param measurement Measurement name.
#' @param source Where to look: `"files"` or `"data"`.
#' @param data A tibble to search when `source = "data"`.
#' @param data_dir Directory containing `.csv.gz` files.
#' @param tz Timezone for the returned value.
#' @return A POSIXct of the start of the next day (for files) or
#'   max datetime + 1 second (for data), or `NULL` if no data found.
#' @export
influx_get_last_time <- function(measurement,
                                 source = c("files", "data"),
                                 data = NULL,
                                 data_dir = "data/raw/influx",
                                 tz = "Australia/Sydney") {
  source <- match.arg(source)

  if (source == "files") {
    pattern <- paste0("^", measurement, "_.*\\.csv\\.gz$")
    files <- list.files(data_dir, pattern = pattern, full.names = FALSE)

    if (length(files) == 0) return(NULL)

    # Extract dates from filenames: measurement_YYYY-MM-DD.csv.gz
    dates <- stringr::str_extract(files, "\\d{4}-\\d{2}-\\d{2}")
    max_date <- max(as.Date(dates))

    # Return start of next day
    lubridate::force_tz(as.POSIXct(max_date + 1), tzone = tz)
  } else {
    if (is.null(data) || nrow(data) == 0) return(NULL)

    filtered <- dplyr::filter(data, .data$parameter == measurement)
    if (nrow(filtered) == 0) return(NULL)

    max(filtered$datetime) + lubridate::seconds(1)
  }
}


#' Incrementally update InfluxDB data
#'
#' For each measurement, finds the last available data point and downloads
#' everything from there to `end`. Creates output directories as needed.
#'
#' @param measurements Character vector of measurement names.
#' @param config Connection config from [influx_config()].
#' @param from Where to find existing data: `"files"` (default) or `"data"`.
#' @param data An existing tibble when `from = "data"`.
#' @param end End time for the update (default: now).
#' @param bucket InfluxDB bucket name.
#' @param tz Timezone.
#' @param default_start Fallback start date when no existing data is found.
#' @param chunk_by Chunking interval.
#' @param save_files Whether to save chunk files.
#' @param output_dir Directory for saved files.
#' @param verbose Print progress messages.
#' @return A tibble of newly downloaded data.
#' @export
influx_update <- function(measurements,
                          config = influx_config(),
                          from = c("files", "data"),
                          data = NULL,
                          end = NULL,
                          bucket = "dp23",
                          tz = "Australia/Sydney",
                          default_start = "2023-12-01",
                          chunk_by = "month",
                          save_files = TRUE,
                          output_dir = "data/raw/influx",
                          verbose = TRUE) {
  from <- match.arg(from)

  if (is.null(end)) {
    end <- lubridate::now(tzone = tz)
  }

  if (save_files && !dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  results <- list()

  for (meas in measurements) {
    last_time <- influx_get_last_time(
      measurement = meas,
      source = from,
      data = data,
      data_dir = output_dir,
      tz = tz
    )

    if (is.null(last_time)) {
      start <- influx_parse_time(default_start, tz = tz)
      if (verbose) {
        message(glue::glue('No data found for "{meas}" - starting from {default_start}'))
      }
    } else {
      start <- last_time
      if (verbose) {
        message(glue::glue('Found existing data for "{meas}" - starting from {start}'))
      }
    }

    if (start > end) {
      if (verbose) {
        message(glue::glue('No new data to download for "{meas}"'))
      }
      next
    }

    df <- influx_get_range(
      measurements = meas,
      start = start,
      end = end,
      config = config,
      bucket = bucket,
      tz = tz,
      chunk_by = chunk_by,
      save_files = save_files,
      output_dir = output_dir,
      verbose = verbose
    )

    results <- c(results, list(df))
  }

  dplyr::bind_rows(results)
}
