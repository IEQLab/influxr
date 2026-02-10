#' Retrieve data from InfluxDB over a time range
#'
#' Queries InfluxDB for one or more measurements, splitting the request
#' into monthly (or other) chunks. Optionally saves each chunk as a
#' compressed CSV file.
#'
#' @param measurements Character vector of measurement names.
#' @param start Start time (anything accepted by [influx_parse_time()]).
#' @param end End time (anything accepted by [influx_parse_time()]).
#' @param config Connection config from [influx_config()].
#' @param bucket InfluxDB bucket name.
#' @param tags Optional named list of tag filters. Names are tag keys, values
#'   are character vectors of allowed values. Multiple values for a single tag
#'   are OR'd; separate tags are AND'd via separate filter steps.
#'   E.g. `list(source = "house_1", room = c("bedroom", "kitchen"))`.
#' @param tz Timezone for queries and returned data.
#' @param chunk_by Chunking interval passed to [influx_chunk_range()].
#' @param save_files If `TRUE`, save each chunk to a `.csv.gz` file.
#' @param output_dir Directory for saved files.
#' @param verbose If `TRUE`, print progress messages.
#' @return A tibble of all retrieved data.
#' @export
influx_get_range <- function(measurements, start, end,
                             config = influx_config(),
                             bucket = NULL,
                             tags = NULL,
                             tz = "Australia/Sydney",
                             chunk_by = "month",
                             save_files = FALSE,
                             output_dir = "data/raw/influx",
                             verbose = TRUE) {
  chunks <- influx_chunk_range(start, end, chunk_by = chunk_by, tz = tz)

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  results <- list()

  for (meas in measurements) {
    for (i in seq_len(nrow(chunks))) {
      chunk <- chunks[i, ]

      if (verbose) {
        message(
          glue::glue('Downloading "{meas}" from {chunk$start_utc} to {chunk$end_utc}')
        )
      }

      query <- influx_build_query(
        measurement = meas,
        start_utc = chunk$start_utc,
        end_utc = chunk$end_utc,
        bucket = bucket,
        tags = tags
      )

      df <- influx_query(query, config = config, tz = tz)

      if (nrow(df) > 0 && save_files) {
        fname <- glue::glue("{meas}_{as.Date(chunk$end)}.csv.gz")
        fpath <- file.path(output_dir, fname)
        readr::write_csv(df, fpath)
        if (verbose) message(glue::glue("  Saved {fpath}"))
      }

      results <- c(results, list(df))
    }
  }

  dplyr::bind_rows(results)
}
