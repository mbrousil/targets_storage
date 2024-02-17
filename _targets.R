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
  
  # Use {googledrive} to send this large data file away for storage but document
  # its location online in a local csv. The goal of this is to mimic the way
  # that large files may be uploaded to Google Drive for storage, (potential)
  # versioning, and transfer between pipelines. Note that it also produces a
  # local .rds file for uploading.
  tar_file(
    name = chl_wqp_data_link_file,
    command = export_single_file(target = chl_wqp_data,
                                 folder_pattern = "data/out/")
  ),
  
  # If this dataset has been previously downloaded, then there's the option to
  # use a prior version instead of the dynamically downloaded one above. This
  # target adds the metadata necessary for this to be used in the rest of the
  # workflow to a table to store in a local csv.
  tar_file(
    name = chl_wqp_data_link_file_stable,
    command = {
      
      # Where to store the csv with link info
      out_path <- "data/out/chl_wqp_data_out_link_stable.csv"
      
      stable_drive_link <- tribble(
        ~dataset, ~local_path, ~drive_link,
        "p2_site_counts_chl", "data/out/chl_wqp_data.rds", "https://drive.google.com/file/d/1krSVWZ6U5A8eNaDd-FZUD8K2AzYH6rRX/view?usp=sharing"
      )
      
      # Export the csv
      write_csv(x = stable_drive_link, file = out_path)
      
      # Return path to pipeline
      out_path
      
    }
  ),
  
  # Here we choose whether we want a stable version of the `chl_wqp_data`
  # dataset, or to use the dynamic version downloaded above. This choice affects
  # the targets below:
  tar_target(
    name = data_version_stable,
    command = FALSE
  ),
  
  # Mimicking the purpose of a second repo, we read in the table containing
  # the link from the export above. If a stable version is requested then
  # we get the download link for the stable dataset defined above
  tar_file_read(
    name = chl_wqp_data_link_in,
    command = {
      if(data_version_stable){
        chl_wqp_data_link_file_stable
      } else {
        chl_wqp_data_link_file
      }
    },
    cue = tar_cue("always"),
    read = read_csv(file = !!.x)
  ),
  
  # Download the dataset to mimic pulling it into a second repository
  tar_target(
    name = chl_wqp_drive_download,
    command = retrieve_data(link_table = chl_wqp_data_link_in,
                            folder_pattern = "data/in/"),
    packages = c("tidyverse", "googledrive")
  )
  
)
