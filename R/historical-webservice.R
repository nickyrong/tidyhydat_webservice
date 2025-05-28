#' Download historical daily data from the ECCC web service
#'
#' Function to retrieve historical daily data from ECCC web service.
#' This function accesses historical daily data which may span many years.
#' The parameters are specified as "level" and "flow" rather than numeric codes.
#'
#' @param station_number Water Survey of Canada station number.
#' @param parameters parameter type. Can take multiple entries. Valid options are "level" and "flow".
#' Defaults to both "level" and "flow".
#' @param start_date Start date in YYYY-MM-DD format.
#' Defaults to 30 days before current date.
#' @param end_date End date in YYYY-MM-DD format.
#' Defaults to current date.
#'
#' @format A tibble with historical daily data variables
#'
#' @examples
#' \dontrun{
#'
#' hist_data <- historical_ws(
#'   station_number = c("07EA004", "07QC005"),
#'   parameters = c("level", "flow")
#' )
#'
#' decade_data <- historical_ws(
#'   station_number = "07EA004",
#'   parameters = "flow",
#'   start_date = "2000-01-01",
#'   end_date = "2009-12-31"
#' )
#' }
#' @family realtime functions
#' @export

historical_ws <- function(
  station_number,
  parameters = NULL,
  start_date = Sys.Date() - 30,
  end_date = Sys.Date()
) {
  if (is.null(parameters)) parameters <- c("level", "flow")

  if (any(!parameters %in% c("level", "flow"))) {
    stop(
      paste0(
        paste0(
          parameters[!parameters %in% c("level", "flow")],
          collapse = ","
        ),
        " are invalid parameters. Valid options are 'level' and 'flow'."
      ),
      call. = FALSE
    )
  }

  if (!is.character(parameters))
    stop("parameters should be character strings ('level' or 'flow')", call. = FALSE)

  # Validate station number format (should be 7 characters: 2 digits + 2 letters + 3 digits)
  invalid_format <- station_number[!grepl("^[0-9]{2}[A-Z]{2}[0-9]{3}$", station_number)]
  if (length(invalid_format) > 0) {
    warning(
      "The following station(s) have invalid format (should be 2 digits + 2 letters + 3 digits): ",
      paste0(invalid_format, collapse = ", ")
    )
  }

  # Convert dates to character format if they are Date objects
  if (inherits(start_date, "Date"))
    start_date <- as.character(start_date)
  if (inherits(end_date, "Date"))
    end_date <- as.character(end_date)

  if (!grepl("[0-9]{4}-[0-1][0-9]-[0-3][0-9]", start_date)) {
    stop(
      "Invalid date format. start_date needs to be in YYYY-MM-DD format",
      call. = FALSE
    )
  }

  if (!grepl("[0-9]{4}-[0-1][0-9]-[0-3][0-9]", end_date)) {
    stop(
      "Invalid date format. end_date needs to be in YYYY-MM-DD format",
      call. = FALSE
    )
  }

  if (!is.null(start_date) & !is.null(end_date)) {
    if (lubridate::ymd(end_date) < lubridate::ymd(start_date)) {
      stop(
        "start_date is after end_date. Try swapping values.",
        call. = FALSE
      )
    }
  }

  ## Build link for GET
  baseurl <- "https://wateroffice.ec.gc.ca/services/daily_data/csv/inline?"

  station_string <- paste0("stations[]=", station_number, collapse = "&")
  parameters_string <- paste0("parameters[]=", parameters, collapse = "&")
  date_string <- paste0(
    "start_date=", start_date,
    "&end_date=", end_date
  )

  ## paste them all together
  query_url <- paste0(
    baseurl,
    station_string,
    "&",
    parameters_string,
    "&",
    date_string
  )

  ## Get data
  req <- httr2::request(query_url)
  resp <- httr2::req_perform(req)

  ## Give webservice some time
  Sys.sleep(1)

  ## Check the respstatus
  httr2::resp_check_status(resp)

  if (httr2::resp_headers(resp)$`Content-Type` != "text/csv; charset=utf-8") {
    stop("Response is not a csv file")
  }

  ## Turn it into a tibble and specify correct column classes
  response_body <- httr2::resp_body_string(resp)
  
  ## Try to read CSV and catch errors that indicate invalid station
  csv_df <- tryCatch({
    readr::read_csv(
      response_body,
      col_types = readr::cols(.default = "c")
    )
  }, error = function(e) {
    if (grepl("does not exist in current working directory", e$message)) {
      stop("Station ID does not exist. Please check if the station number is valid.", call. = FALSE)
    } else {
      stop("Error reading response: ", e$message, call. = FALSE)
    }
  })

  ## Check here to see if csv_df has any data in it
  if (nrow(csv_df) == 0) {
    stop("No data exists for this station query")
  }

  ## What stations were missed?
  differ <- setdiff(unique(station_number), unique(csv_df$ID))
  if (length(differ) != 0) {
    if (length(differ) <= 10) {
      message(
        "The following station(s) were not retrieved: ",
        paste0(differ, sep = " ")
      )
      message(
        "Check station number for typos or if it is a valid station in the network"
      )
    } else {
      message(
        "More than 10 stations from the initial query were not returned."
      )
    }
  } else {
    message("All stations successfully retrieved")
  }

  ## Return it
  return(csv_df)
}

# test the function
# library(tidyhydat)
# 
# historical_ws(
#   station_number = "08GA010",
#   parameters = "flow",
#   start_date = "1840-01-01",
#   end_date = as.character(Sys.Date())
# )$'Symbol/Symbole' %>% unique()
# 
# # test non-sensical ID
# historical_ws(
#   station_number = "09AA888",
#   parameters = "flow",
#   start_date = "2020-01-01",
#   end_date = "2020-12-31"
# )
