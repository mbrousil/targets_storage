# Many of these functions have been copied or adapted from the
# `AquaSat/AquaMatch_download_WQP` repository, which originates
# from USGS and ROSSyndicate code.

#' @title Get parameter inventory from the WQP
#' 
#' @description
#' A function that maps over `characteristicNames` from the WQP and takes an
#' inventory of all of the data available for those `characteristicNames` within
#' the current grid cell aoi.
#' 
#' @param grid_aoi An sf object for the current area of interest. Intended to be
#' a single grid cell from a large sampling grid.
#' 
#' @param wqp_characteristics A vector of WQP `characteristicNames` to iterate
#' over while taking inventory of the WQP.
#' 
#' @param wqp_args A named list of all arguments other than bbox and
#' `characteristicName` to provide in the WQP query. 
#' 
#' @returns 
#' Returns a data frame containing counts of records at
#' `MonitoringLocationIdentifier`, `CharacteristicName`, and `grid_id`
#' combinations.
#' 
take_inventory <- function(grid_aoi, wqp_characteristics, wqp_args){
  
  # Get bounding box for the grid polygon
  bbox <- st_bbox(grid_aoi)
  
  # For each combination of grid cell and CharacteristicName retrieve
  # inventory information from WQP
  map_df(.x = wqp_characteristics,
         .f = ~{
           
           # Define arguments for whatWQPdata()
           wqp_args_all <- list(
             wqp_args,
             # Bounding box of the current grid cell
             bBox = c(bbox$xmin, bbox$ymin, bbox$xmax, bbox$ymax),
             characteristicName = .x
           )
           
           # Now attempt to retrieve the info
           retry(whatWQPdata(wqp_args_all),
                 when = "Error:", 
                 max_tries = 3) %>%
             mutate(CharacteristicName = .x,
                    grid_id = grid_aoi$id)
         }
  )
  
}

# Assign download groups to the dataset based on the number of records each site
# has
#' @title Group sites for downloading data without hitting the WQP cap
#' 
#' @description 
#' Function to group inventoried sites into reasonably sized chunks for
#' downloading data.
#' 
#' @param site_counts data frame containing the site identifiers and total 
#' number of records available for each site. Must contain columns 
#' `MonitoringLocationIdentifier` and `results_count`.
#' 
#' @returns 
#' Returns a data frame with columns site id, the total number of records,
#' (retains the column from `site_counts`), site number, and an additional column 
#' called `download_grp` which is made up of unique groups that enable use of 
#' `group_by()` and then `tar_group()` for downloading.
#' 
assign_download_groups <- function(site_counts){
  
  max_sites <- 300
  max_results <- 250000
  
  if(any(site_counts$results_count > max_results)){
    sites_w_many_records <- site_counts %>%
      filter(results_count > max_results) %>%
      pull(MonitoringLocationIdentifier)
    # Print a message to inform the user that some sites contain a lot of data
    # message(sprintf(paste0("results_count exceeds max_results for the sites below. ",
    #                        "Assigning data-heavy sites to their own download group to ",
    #                        "set up a manageable query to WQP. If you are not already ",
    #                        "branching across characteristic names, consider doing so to ",
    #                        "further limit query size. \n\n%s\n"),
    #                 paste(sites_w_many_records, collapse="\n")))
  }
  
  # Check whether any sites have identifiers that are likely to cause problems when
  # downloading the data from WQP
  sitecounts_bad_ids <- identify_bad_ids(site_counts)
  
  # Subset 'good' sites with identifiers that can be parsed by WQP
  sitecounts_good_ids <- site_counts %>%
    filter(!MonitoringLocationIdentifier %in% sitecounts_bad_ids$site_id)
  
  # Within each unique grid_id, use the cumsumbinning function from the MESS package
  # to group sites based on the cumulative sum of results_count across sites that 
  # share the same characteristic name, resetting the download group/task number if 
  # the number of records exceeds the threshold set by `max_results`.
  sitecounts_grouped_good_ids <- sitecounts_good_ids %>%
    rename(site_id = MonitoringLocationIdentifier) %>% 
    split(.$grid_id) %>%
    map_dfr(.f = function(df){
      
      df_grouped <- df %>%
        group_by(CharacteristicName) %>%
        arrange(desc(results_count), .by_group = TRUE) %>%
        mutate(task_num_by_results = cumsumbinning(x = results_count, 
                                                   threshold = max_results, 
                                                   maxgroupsize = max_sites), 
               char_group = cur_group_id()) %>%
        ungroup() %>% 
        # Each group from before (which represents a different characteristic 
        # name) will have task numbers that start with "1", so now we create 
        # a new column called `task_num` to create unique task numbers within
        # each grid. For example, both "Specific conductance" and "Temperature" 
        # may have values for `task_num_by_results` of 1 and 2 but the values 
        # of char_group (1 and 2, respectively) would mean that they have unique
        # values for `task_num` equaling 1, 2, 3, and 4.
        group_by(char_group, task_num_by_results) %>% 
        mutate(task_num = cur_group_id()) %>% 
        ungroup() %>%
        mutate(pull_by_id = TRUE) 
    }) 
  
  # Combine all sites back together (now with assigned download_grp id's) and
  # format columns
  sitecounts_grouped_out <- sitecounts_grouped_good_ids %>%
    # bind_rows(sitecounts_grouped_bad_ids) %>%
    # Ensure the groups are ordered correctly by prepending a dynamic number of 0s
    # before the task number based on the maximum number of tasks.
    mutate(download_grp = sprintf(paste0("%s_%0", nchar(max(task_num)), "d"), 
                                  grid_id, task_num)) %>% 
    arrange(download_grp) %>%
    select(site_id,
           # lat, lon, datum,
           grid_id,
           CharacteristicName, results_count, 
           download_grp, pull_by_id) %>%
    group_by(download_grp) %>%
    tar_group()
  
  return(sitecounts_grouped_out)
  
}

#' @title Find bad site identifiers
#' 
#' @description
#' Function to check sites for identifier names that are likely to cause 
#' problems when downloading data from WQP by siteid. Some site identifiers
#' contain characters that cannot be parsed by WQP, including "/". This function
#' identifies and subsets sites with potentially problematic identifiers.
#' 
#' @param sites data frame containing the site identifiers. Must contain
#' column `MonitoringLocationIdentifier`.
#' 
#' @returns 
#' Returns a data frame where each row represents a site with a problematic
#' identifier, indicated by the new column `site_id`. All other columns within
#' `sites` are retained. Returns an empty data frame if no problematic site
#' identifiers are found.
#' 
#' @examples 
#' siteids <- data.frame(MonitoringLocationIdentifier = 
#'                         c("USGS-01573482","COE/ISU-27630001"))
#' identify_bad_ids(siteids)
#' 
identify_bad_ids <- function(sites){
  
  # Check that string format matches regex used in WQP
  sites_bad_ids <- sites %>%
    rename(site_id = MonitoringLocationIdentifier) %>% 
    mutate(site_id_regex = str_extract(site_id, "[\\w]+.*[\\S]")) %>%
    filter(site_id != site_id_regex) %>%
    select(-site_id_regex)
  
  return(sites_bad_ids)
}



#' @title Download data from the Water Quality Portal
#' 
#' @description 
#' Function to pull WQP data given a dataset of site ids and/or site coordinates.
#'  
#' @param site_counts_grouped data frame containing a row for each site. Columns 
#' contain the site identifiers, the total number of records, and an assigned
#' download group. Must contain columns `site_id` and `pull_by_id`, where
#' `pull_by_id` is logical and indicates whether data should be downloaded
#' using the site identifier or by querying a small bounding box around the site.
#' @param char_names vector of character strings indicating which WQP 
#' characteristic names to query.
#' @param wqp_args list containing additional arguments to pass to whatWQPdata(),
#' defaults to NULL. See https://www.waterqualitydata.us/webservices_documentation 
#' for more information.  
#' @param max_tries integer, maximum number of attempts if the data download 
#' step returns an error. Defaults to 3.
#' @param verbose logical, indicates whether messages from {dataRetrieval} should 
#' be printed to the console in the event that a query returns no data. Defaults 
#' to FALSE. Note that `verbose` only handles messages, and {dataRetrieval} errors 
#' or warnings will still get passed up to `fetch_wqp_data`. 
#' 
#' @returns
#' Returns a data frame containing data downloaded from the Water Quality Portal, 
#' where each row represents a unique data record.
#' 
#' @examples
#' site_counts <- data.frame(site_id = c("USGS-01475850"), pull_by_id = c(TRUE))
#' fetch_wqp_data(site_counts, 
#'               "Temperature, water", 
#'               wqp_args = list(siteType = "Stream"))
#' 
fetch_wqp_data <- function(site_counts_grouped, char_names, wqp_args = NULL, 
                           max_tries = 3, verbose = FALSE){
  
  message(sprintf("Retrieving WQP data for %s sites in group %s, %s",
                  nrow(site_counts_grouped), unique(site_counts_grouped$download_grp), 
                  char_names))
  
  # Define arguments for readWQPdata
  # sites with pull_by_id = FALSE cannot be queried by their site
  # identifiers because of undesired characters that will cause the WQP
  # query to fail. For those sites, query WQP by adding a small bounding
  # box around the site(s) and including bBox in the wqp_args.
  if(unique(site_counts_grouped$pull_by_id)){
    wqp_args_all <- c(wqp_args, 
                      list(siteid = site_counts_grouped$site_id,
                           characteristicName = c(char_names)))
  } else {
    wqp_args_all <- c(wqp_args, 
                      list(bBox = create_site_bbox(site_counts_grouped),
                           characteristicName = c(char_names)))
  }
  
  # Define function to pull data, retrying up to the number of times
  # indicated by `max_tries`
  pull_data <- function(x){
    retry(readWQPdata(x),
          when = "Error:", 
          max_tries = max_tries)
  }
  
  # Now pull the data. If verbose == TRUE, print all messages from dataRetrieval,
  # otherwise, suppress messages.
  if(verbose) {
    wqp_data <- pull_data(wqp_args_all)
  } else {
    wqp_data <- suppressMessages(pull_data(wqp_args_all))
  }
  
  # We applied special handling for sites with pull_by_id = FALSE (see comments
  # above). Filter wqp_data to only include sites requested in site_counts_grouped
  # in case our bounding box approach picked up any additional, undesired sites. 
  # In addition, some records return character strings when we expect numeric 
  # values, e.g. when "*Non-detect" appears in the "ResultMeasureValue" field. 
  # For now, consider all columns to be character so that individual data
  # frames returned from fetch_wqp_data can be joined together. 
  wqp_data_out <- wqp_data %>%
    filter(MonitoringLocationIdentifier %in% site_counts_grouped$site_id) %>%
    mutate(across(everything(), as.character))
  
  return(wqp_data_out)
}


#' @title Export a single target to Google Drive
#' 
#' @description
#' A function to export a single target (as a file) to Google Drive and return
#' the shareable Drive link as a file path.
#' 
#' @param target The name of the target to be exported (as an object not a string).
#' 
#' @param folder_pattern A local path to a location where the target should be
#' exported before uploading.
#' 
#' @param drive_path A path to the folder on Google Drive where the file
#' should be saved.
#' 
#' @returns 
#' Returns a local path to a csv file containing a text link to the uploaded
#' file in Google Drive.
#' 
export_single_file <- function(target, drive_path){
  
  require(googledrive)
  
  # Get target name as a string
  target_string <- deparse(substitute(target))
  
  # Create a temporary file exported locally, which can then be used to upload
  # to Google Drive
  file_local_path <- tempfile(fileext = ".rds")
  
  write_rds(x = target,
            file = file_local_path)
  
  # Once locally exported, send to Google Drive
  out_file <- drive_put(media = file_local_path,
                        # The folder on Google Drive
                        path = drive_path,
                        # The filename on Google Drive
                        name = paste0(target_string, ".rds"))
  
  # Make the Google Drive link shareable: anyone can view
  out_file_share <- out_file %>%
    drive_share(role = "reader", type = "anyone")
  
  # Now remove the local file after upload is complete
  file.remove(file_local_path)
  
}

#' @title Retrieve a dataset from Google Drive
#' 
#' @description
#' A function to retrieve a dataset from Google Drive after it has been uploaded
#' in a previous step.
#' 
#' @param target The name of the target to be exported (as an object not a string).
#' 
#' @param local_folder A string specifying the folder where the file should be
#' downloaded.
#' 
#' @param drive_folder A string specifying the folder location on Google Drive
#' containing the file to be downloaded.
#' 
#' @param stable Logical value. If TRUE, look for file in the "stable" subfolder
#' in Google Drive. If FALSE, use the path as provided by the user.
#' 
#' @param file_type A string giving the file extension to be used.
#' 
#' @returns 
#' The dataset after being downloaded and read into the pipeline from Google Drive.
#' 
retrieve_data <- function(target, local_folder, drive_folder, stable, file_type = ".rds"){
  
  require(googledrive)
  
  # Get target name as a string
  target_string <- paste0(deparse(substitute(target)), file_type)
  
  # Local file download location
  local_path <- file.path(local_folder, target_string)
  
  # Get file contents of the Google Drive folder specified. If stable == TRUE
  # then append "stable/" to go to the subfolder for stable products.
  if(stable){
    folder_contents <- drive_ls(path = paste0(drive_folder, "stable/"))
  } else{
    folder_contents <- drive_ls(path = drive_folder)
  }
  
  # Filter the contents to the file requested and obtain its ID
  drive_file_id <- folder_contents %>%
    filter(name == target_string) %>%
    pull(id) %>%
    as_id(.)
  
  # Run the download
  drive_download(file = drive_file_id,
                 path = local_path,
                 overwrite = TRUE)
  
  # Read dataset into pipeline
  read_rds(local_path)
  
}
