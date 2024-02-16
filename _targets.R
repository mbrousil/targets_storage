# Created by use_targets().

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)

# Set target options:
tar_option_set(
  packages = c("tidyverse")
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source("src/")

# Target list
list(
  
  # The CharacteristicNames we want to query
  tar_target(
    name = wqp_characteristics,
    command = c(
      "Chlorophyll a",
      "Chlorophyll a (probe relative fluorescence)",
      "Chlorophyll a, corrected for pheophytin",
      "Chlorophyll a (probe)",
      "Chlorophyll a, free of pheophytin",
      "Chlorophyll a, uncorrected for pheophytin",
      "Chlorophyll a - Phytoplankton (suspended)"
    )
  ),
  
  # Arguments to send to the WQP queries
  tar_target(
    name = wqp_args,
    command = list(
      sampleMedia = c("Water", "water"),
      siteType = c("Lake, Reservoir, Impoundment",
                   "Stream",
                   "Estuary"),
      # Return sites with at least one data record
      minresults = 1, 
      startDateLo = "1970-01-01",
      startDateHi = Sys.Date()
    )
  ),
  
  # A grid to use for branching
  tar_target(
    name = global_grid,
    command = {
      global_box <- st_bbox(c(xmin = -180, xmax = 180,
                              ymax = 90, ymin = -90), 
                            crs = st_crs(4326))
      
      # Create square grid
      global_grid <- global_box %>%
        st_make_grid(cellsize = c(2, 2), square = TRUE, 
                     offset = c(-180, st_bbox(global_box)$ymin)) %>%
        # Convert to sf object and add an "id" attribute
        st_as_sf() %>%
        mutate(id = row.names(.))
    },
    packages = c("tidyverse", "sf")
  ),  
  
  # The specific area of interest (here, states to query)
  tar_target(
    name = aoi,
    command = states() %>%
      filter(STUSPS %in% c("WA", "OR", "ID", "WY", "UT", "CO")),
    packages = c("tidyverse", "tigris")
  ),
  
  # Subset the grid to the aoi
  tar_target(
    name = grid_aoi,
    command = global_grid %>%
      st_filter(y = st_transform(aoi, st_crs(global_grid)),
                .predicate = st_is_within_distance,
                dist = set_units(0, m)),
    packages = c("tidyverse", "sf", "units")
  ),
  
  tar_target(
    name = chl_inventory,
    command = {
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
    },
    # Each iteration is one grid cell x CharacteristicName combo
    pattern = cross(grid_aoi, wqp_characteristics),
    # Don't fail out if one branch errors
    error = "continue",
    packages = c("tidyverse", "retry", "sf", "dataRetrieval", "units")
  ), 
  
  # Get count of available records by site, we'll need these to avoid
  # overwhelming the server
  tar_target(
    name = chl_site_counts,
    command = {
      chl_inventory %>%
        # Hold onto location info, grid_id, characteristic, and provider data
        # and use them for grouping
        group_by(MonitoringLocationIdentifier, grid_id,
                 CharacteristicName, ProviderName) %>%
        # Count the number of rows per group
        summarize(results_count = sum(resultCount, na.rm = TRUE),
                  .groups = "drop") 
    }
  ),
  
  tar_target(
    name = chl_download_groups,
    command = {
      
      max_sites <- 300
      max_results <- 250000
      
      if(any(chl_site_counts$results_count > max_results)){
        sites_w_many_records <- chl_site_counts %>%
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
      sitecounts_bad_ids <- identify_bad_ids(chl_site_counts)
      
      # Subset 'good' sites with identifiers that can be parsed by WQP
      sitecounts_good_ids <- chl_site_counts %>%
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
      
    },
    iteration = "group",
    packages = c("tidyverse", "MESS")
  ),
  
  tar_target(
    name = chl_wqp_data,
    command = fetch_wqp_data(chl_download_groups,
                             char_names = unique(chl_site_counts$CharacteristicName),
                             wqp_args = wqp_args),
    pattern = map(chl_download_groups),
    error = "continue",
    format = "feather",
    packages = c("dataRetrieval", "tidyverse", "sf", "retry")
  )
  
  
)
