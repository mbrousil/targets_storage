# This is a helper script to run the pipeline.

# Package handling --------------------------------------------------------

# List of packages required for this pipeline
required_pkgs <- c(
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

# map function using base lapply
lapply(required_pkgs, package_installer)

# Load packages for use below
library(targets)


# Directory handling ------------------------------------------------------

# Check for directory and create if it doesn't exist
if (!dir.exists("data/")) {dir.create("data/")}


# Run pipeline ------------------------------------------------------------

tar_make()
