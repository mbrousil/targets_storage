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
  
  # Get an inventory of the chlorophyll data in the WQP
  tar_target(
    name = chl_inventory,
    command = take_inventory(grid_aoi = grid_aoi,
                             wqp_characteristics = wqp_characteristics,
                             wqp_args = wqp_args),
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
  
  # Break data into download groups based on record counts
  tar_target(
    name = chl_download_groups,
    command = assign_download_groups(chl_site_counts = chl_site_counts),
    iteration = "group",
    packages = c("tidyverse", "MESS")
  ),
  
  # Retrieve the data from the WQP
  # As of 2024-02-15 the output data frame from this step is ~300 MB
  tar_target(
    name = chl_wqp_data,
    command = fetch_wqp_data(chl_download_groups,
                             char_names = unique(chl_site_counts$CharacteristicName),
                             wqp_args = wqp_args),
    pattern = map(chl_download_groups),
    error = "continue",
    format = "feather",
    packages = c("dataRetrieval", "tidyverse", "sf", "retry")
  ),
  
  # Export the file
  tar_file(
    name = chl_wqp_data_file,
    command = {
      # Declare file storage location
      out_path <- "data/chla_wqp_data.feather"
      
      write_feather(x = chl_wqp_data,
                    path = out_path)
      
      # Return path to pipeline for tracking
      out_path
    },
    packages = "feather"
  )
  
  
)
