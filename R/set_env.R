#' Set InfluxDB environment variables
#'
#' Writes `INFLUXDB_URL`, `INFLUXDB_TOKEN`, and `INFLUXDB_ORG` to your
#' user-level `~/.Renviron` file and sets them in the current R session so
#' they take effect immediately.
#'
#' When called with no arguments in an interactive session, prompts for each
#' value via `readline()`. In non-interactive sessions all three arguments
#' must be supplied explicitly.
#'
#' @param url InfluxDB server URL (e.g. `"http://host:8086"`).
#' @param token InfluxDB API token.
#' @param org InfluxDB organisation name.
#' @param renviron_path Path to the `.Renviron` file. Defaults to `~/.Renviron`.
#' @return Invisibly returns a named list with `url`, `token`, and `org`.
#' @seealso [influx_config()] to read the configuration back.
#' @export
influx_set_env <- function(url = NULL, token = NULL, org = NULL,
                           renviron_path = file.path(Sys.getenv("HOME"), ".Renviron")) {
  if (is.null(url))   url   <- prompt_or_stop("INFLUXDB_URL",   "InfluxDB URL (e.g. http://host:443)")
  if (is.null(token)) token <- prompt_or_stop("INFLUXDB_TOKEN", "InfluxDB API token")
  if (is.null(org))   org   <- prompt_or_stop("INFLUXDB_ORG",   "InfluxDB organisation")

  write_renviron(url = url, token = token, org = org, path = renviron_path)

  Sys.setenv(INFLUXDB_URL = url, INFLUXDB_TOKEN = token, INFLUXDB_ORG = org)

  masked_token <- mask_token(token)
  message("Set InfluxDB environment variables:")
  message("  INFLUXDB_URL   = ", url)
  message("  INFLUXDB_TOKEN = ", masked_token)
  message("  INFLUXDB_ORG   = ", org)
  message("Written to ~/.Renviron and loaded in current session.")


  invisible(list(url = url, token = token, org = org))
}

#' Prompt interactively or stop with an error
#' @noRd
prompt_or_stop <- function(var, label) {
  if (interactive()) {
    val <- readline(paste0(label, ": "))
    if (!nzchar(val)) stop(var, " cannot be empty.", call. = FALSE)
    return(val)
  }
  stop(var, " must be provided in non-interactive mode.", call. = FALSE)
}

#' Mask an API token, showing only the last 4 characters
#' @noRd
mask_token <- function(token) {
  nc <- nchar(token)
  if (nc <= 4) return(token)
  paste0(strrep("*", nc - 4), substr(token, nc - 3, nc))
}

#' Write INFLUXDB_* variables to ~/.Renviron
#' @noRd
write_renviron <- function(url, token, org, path = file.path(Sys.getenv("HOME"), ".Renviron")) {
  vars <- c(
    INFLUXDB_URL   = url,
    INFLUXDB_TOKEN = token,
    INFLUXDB_ORG   = org
  )

  if (file.exists(path)) {
    lines <- readLines(path, warn = FALSE)
  } else {
    lines <- character(0)
  }

  for (nm in names(vars)) {
    pattern <- paste0("^", nm, "=")
    idx <- grep(pattern, lines)
    new_line <- paste0(nm, "=", vars[[nm]])
    if (length(idx) > 0) {
      lines[idx[1]] <- new_line
      if (length(idx) > 1) lines <- lines[-idx[-1]]
    } else {
      lines <- c(lines, new_line)
    }
  }

  writeLines(lines, path)
}
