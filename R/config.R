#' Create an InfluxDB v2 connection configuration
#'
#' Reads connection details from environment variables or explicit arguments.
#' Set `INFLUXDB_URL`, `INFLUXDB_TOKEN`, and `INFLUXDB_ORG` in your
#' `.Renviron` file for persistent configuration.
#'
#' @param url InfluxDB server URL (e.g. `"http://host:8086"`).
#' @param token InfluxDB API token.
#' @param org InfluxDB organisation name.
#' @return A named list with elements `url`, `token`, and `org`.
#' @export
influx_config <- function(url = Sys.getenv("INFLUXDB_URL"),
                          token = Sys.getenv("INFLUXDB_TOKEN"),
                          org = Sys.getenv("INFLUXDB_ORG")) {
  if (!nzchar(url)) stop("InfluxDB URL is not set. Set INFLUXDB_URL env var or pass `url`.", call. = FALSE)
  if (!nzchar(token)) stop("InfluxDB token is not set. Set INFLUXDB_TOKEN env var or pass `token`.", call. = FALSE)
  if (!nzchar(org)) stop("InfluxDB org is not set. Set INFLUXDB_ORG env var or pass `org`.", call. = FALSE)

  list(url = url, token = token, org = org)
}
