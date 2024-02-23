# This is a helper script to run the pipeline.

# Package handling --------------------------------------------------------

# List of packages required for this pipeline
required_pkgs <- c(
  "config",
  "dataRetrieval",
  "feather",
  "googledrive",
  "MESS",
  "retry",
  "sf",
  "targets", 
  "tarchetypes",
  "tidyverse",
  "tigris",
  "units")

# Helper function to install all necessary packages
package_installer <- function(x) {
  if (x %in% installed.packages()) {
    print(paste0("{", x ,"} package is already installed."))
  } else {
    install.packages(x)
    print(paste0("{", x ,"} package has been installed."))
  }
}

# Iterate through package installs using base lapply
lapply(required_pkgs, package_installer)

# Load packages for use below
library(targets)
library(googledrive)


# Directory handling ------------------------------------------------------

# Check for directory and create if it doesn't exist
if (!dir.exists("data/in/")) {dir.create(path = "data/in/",
                                         recursive = TRUE)}

if (!dir.exists("data/out/")) {dir.create(path = "data/out/",
                                          recursive = TRUE)}


# Google Drive auth -------------------------------------------------------

# Confirm Google Drive is authorized locally. If you are a member of the AquaSat team and 
# are updating the stable version of the pipeline, you will need to authorize Google using
# the ROSSyndicate gmail account. All other users can use any google address, allowing 
# for their personal 'version' of the data download for use throughout the remainder of the
# pipeline.
drive_auth()
# Select existing account (change if starting from scratch)
2


# There are a few options for running this pipeline, and you will need to assure that your Google email has been added in the `config.yml` file. 

# create a fresh version of all {targets} - make sure that line 22 in `_targets.R` is set to "create_new_version"
tar_make()

#  just load the stable version from AquaSat - make sure that line 22 in `_targets.R` is set to "use_stable_link"
tar_make(chl_wqp_data_in)

# members of the AquaSat team can update the stable version using "admin_update" on line 22 in `_targets.R`
tar_make()

tar_make()
