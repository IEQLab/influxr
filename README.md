
# Influxr

<!-- badges: start -->
<!-- badges: end -->

The goal of influxr is to provide a simple interface for retrieving data from
InfluxDB v2 buckets. It was written for data workflows in the [IEQ Lab](https://www.sydney.edu.au/architecture/our-research/research-labs-and-facilities/indoor-environmental-quality-lab.html) 
but can be generalised for any user. It replaces the archived `influxdbclient` 
CRAN package with direct HTTP calls via [httr2](https://httr2.r-lib.org/), and 
adds features for data science workflows including timezone handling, monthly 
query chunking, compressed file caching, and incremental dataset updates.

## Installation

You can install influxr from GitHub with:

``` r
# install.packages("pak")
pak::pak("IEQLab/influxr")
```

Or from a local clone:

``` r
devtools::install_local("/path/to/influxr")
```

## Configuration

The easiest way to store your InfluxDB credentials is with `influx_set_env()`:

``` r
influx_set_env(
  url   = "http://your-host:8086",
  token = "your-api-token",
  org   = "your-org"
)
```

This writes the values to your `~/.Renviron` file and loads them in the current
session. Call `influx_set_env()` with no arguments for interactive prompts.

You can also edit `~/.Renviron` manually (use `usethis::edit_r_environ()` to
open it):

```
INFLUXDB_URL=http://your-host:8086
INFLUXDB_TOKEN=your-api-token
INFLUXDB_ORG=your-org
```

Or pass credentials explicitly:

``` r
cfg <- influx_config(
  url   = "http://your-host:8086",
  token = "your-api-token",
  org   = "your-org"
)
```

## Usage

``` r
library(influxr)

# Download a specific time range
data <- influx_get_range(
  measurements = c("tvoc", "temperature"),
  start = "2024-06-01",
  end = "2024-06-30"
)

# Incrementally update — picks up where the last download left off
new_data <- influx_get_update(
  measurements = c("tvoc", "temperature", "humidity"),
  save_files = TRUE
)

# Read back cached .csv.gz files
cached <- influx_read_cached("tvoc", start = "2024-06-01")
```

### Key functions

| Function | Purpose |
|---|---|
| `influx_set_env()` | Store InfluxDB credentials in `~/.Renviron` |
| `influx_config()` | Create a connection configuration |
| `influx_get_range()` | Download data for a time range, with optional file saving |
| `influx_get_update()` | Incrementally download new data since the last fetch |
| `influx_read_cached()` | Read previously saved `.csv.gz` files |
| `influx_query()` | Execute a raw Flux query |
| `influx_build_query()` | Construct a Flux query string |
| `influx_parse_time()` | Parse dates/times with timezone handling |
| `influx_chunk_range()` | Split a time range into monthly/weekly/daily chunks |

### Time handling

All functions default to `tz = "Australia/Sydney"`. Times are converted to UTC
for Flux queries and back to local time in the returned data. The
`influx_parse_time()` helper accepts Date objects, POSIXct, or character strings
in common formats.

``` r
# All of these work
influx_parse_time("2024-06-15")
influx_parse_time("2024-06-15 14:30:00")
influx_parse_time(as.Date("2024-06-15"))
influx_parse_time(Sys.time())
```

## Dependencies

influxr uses [httr2](https://httr2.r-lib.org/) for HTTP requests and
[readr](https://readr.tidyverse.org/) for CSV parsing. Other dependencies:
dplyr, lubridate, stringr, glue, purrr, tibble.
