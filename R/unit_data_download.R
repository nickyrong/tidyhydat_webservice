# Copyright 2025 Nick Rong
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file is part of the "tidyhydat_webservice" project.
#
# @author Nick Rong
# @date 2025

#' Download and Extract Unit Data for a Specific Station
#'
#' This function downloads compressed unit data (.xz file) from the 
#' Canadian Hydrometrics database, and extracts the CSV file for a specified station ID.
#'
#' @param station_id Character string of the station identifier.
#' @param download_dir Character string of the directory to save the downloaded file.
#'                     Default is a temporary directory.
#' @param keep_zip Logical indicating whether to keep the compressed file after extraction.
#'                Default is FALSE.
#'
#' @return Path to the extracted CSV file.
#'
#' @examples
#' \dontrun{
#' csv_path <- download_unit_data("08MF005")
#' unit_data <- read.csv(csv_path)
#' }
#'
#' @export

# housekeeping
rm(list = ls())
library(tidyverse)
library(tidyhydat)
library(httr2)


download_unit_data <- function(station_id, parameter = "Discharge", data_quality = "corrected", download_dir = tempdir(), keep_zip = FALSE) {
    
    # parameter must be one of "Discharge" or "Stage"
    if (!parameter %in% c("Discharge", "Stage")) {
        stop("Parameter must be either 'Discharge' or 'Stage'.")
    }

    # data_quality must be one of "corrected" or "raw"
    if (!data_quality %in% c("corrected", "raw")) {
        stop("Data quality must be either 'corrected' or 'raw'.")
    }
    
    # Base URL for unit value data
    base_url <- "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/UnitValueData/"
    
    # Extract first two digits for subdirectory
    subdir <- substr(station_id, 1, 2)
    
    # subdirectory, parameter and then quality type, and then first 2 digits of station number
    subdir_url <- paste0(base_url, parameter, "/", data_quality, "/", subdir, "/")
    
    # pull the list of files
    html_content <- request(subdir_url) |>
        req_perform() |>
        resp_body_string()
    
    # parse the HTML to extract .xz filenames
    file_lines <- strsplit(html_content, "\n")[[1]]
    xz_files <- file_lines[grepl("\\.xz", file_lines)]
    
    # extract just the filenames from the HTML links
    filenames <- gsub('.*href="([^"]+\\.xz)".*', '\\1', xz_files)
    filenames <- filenames[grepl(paste0("^", parameter, "\\.Working@"), filenames)]
    
    # extract the file for the specific station number
    station_file <- filenames[grepl(paste0(station_id), filenames)]
    
    # construct the full URL for the station file
    station_url <- paste0(subdir_url, station_file)
    
    # Print a message indicating the file being downloaded
    message("Downloading unit data for station: ", station_id)
    # download the file to specified directory
    temp_file <- file.path(download_dir, basename(station_file))
    download.file(station_url, temp_file, method = "auto", quiet = FALSE)
    
    # print a message indicating the file is being extracted
    message("Extracting unit data from: ", temp_file)
    
    # extract the .xz file
    extracted_file <- sub("\\.xz$", "", temp_file)  
    # when extracting, replace existing file if it exists
    if (file.exists(extracted_file)) {
        file.remove(extracted_file)
    }
    # use system2 to decompress the .xz file
    system2("xz", args = c("-d", temp_file))
    
    # read in the csv file using readLines() 
    unit_data <- readLines(extracted_file)
    
    # remove any lines that start with "#"
    unit_data <- unit_data[!grepl("^#", unit_data)]
    
    # convert to CSV data frame
    unit_df <- read.csv(text = paste(unit_data, collapse = "\n"), header = TRUE) |>
            tibble()
    
    # Convert the first two columns to Date and Time
    unit_df <- unit_df |>
    mutate(
            ISO.8601.UTC = as.POSIXct(ISO.8601.UTC, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
            across(starts_with("Timestamp"), ~ {
                    col_name <- cur_column()
                    # Extract timezone offset from column name (e.g., "08" from "Timestamp..UTC.08.00.")
                    tz_offset <- as.numeric(gsub(".*UTC\\.(\\d{2}).*", "\\1", col_name))
                    # Only process if we successfully extracted a timezone offset
                    if (!is.na(tz_offset)) {
                            tz_string <- paste0("Etc/GMT+", tz_offset)
                            as.POSIXct(.x, format = "%Y-%m-%d %H:%M:%S", tz = tz_string)
                    } else {
                            # Return the column as-is if no timezone offset found
                            .x
                    }
                    })
        ) |>
        # convert the Grade code to string values using this table
        mutate(
            Grade_Description = case_when(
                Grade == -1 ~ "Unspecified",
                Grade == 10 ~ "Ice",
                Grade == 20 ~ "Estimated",
                Grade == 30 ~ "Partial Day",
                Grade == 40 ~ "Dry",
                Grade == 50 ~ "Revised",
                TRUE ~ as.character(Grade)
            )
        ) |>
        # rename the Value column based on parameter type
        rename(!!paste0(parameter, "_Value") := Value) |>
        select(-Grade, -Qualifiers)  # remove the original Grade column
    
    # Clean up files if not keeping
    if (!keep_zip && file.exists(extracted_file)) {
        unlink(extracted_file)
    }
    
    return(unit_df)
}

# # test the function -------- DO NOT RUN --------
# # ID 08GA010 discharge
# example_discharge_data <- download_unit_data(station_id =  "08GA010", 
#                     parameter = "Discharge", data_quality = "corrected")  

# # test 08LG010 discharge stage
# example_stage_data <- download_unit_data(station_id =  "08LG010", 
#                                      parameter = "Stage", data_quality = "corrected")  

# # test raw data
# example_raw_data <- download_unit_data(station_id =  "08GA010", 
#                                      parameter = "Discharge", data_quality = "raw")

# # show the tail of the example datasets
# tail(example_discharge_data)
# tail(example_stage_data)   
# tail(example_raw_data)

# # find non-unique timestamps in the discharge data as a check
# non_unique_timestamps <- example_discharge_data |>
#     group_by(ISO.8601.UTC) |>
#     filter(n() > 1) |>
#     ungroup()

# # compared the values between corrected and raw data
# compare_data <- example_discharge_data |>
#     select(ISO.8601.UTC, Discharge_Value) |>
#     left_join(example_raw_data |>
#               select(ISO.8601.UTC, Discharge_Value), 
#               by = "ISO.8601.UTC", suffix = c("_corrected", "_raw")) |>
#     mutate(Difference = Discharge_Value_corrected - Discharge_Value_raw)

# summarize(compare_data, 
#           Count = n(), 
#           Mean_Difference = mean(Difference, na.rm = TRUE), 
#           SD_Difference = sd(Difference, na.rm = TRUE))

