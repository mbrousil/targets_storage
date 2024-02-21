# targets_storage

This repository is meant to be a small vignette of the `AquaSat/AquaMatch_download_WQP` which 
documents some pain points in our use of {targets} with large data objects, especially regarding WQP 
download, which is not static. This architecture requires a google account to function, and one intends to 
update the pipeline for the external world (aka, create stable datasets for downstream use, e.g. AquaSat), 
you will need to authenticate using the ROSSyndicate google account.

The `run.R` file contains the suggested workflow for running this pipeline. As of 2/16/2024, completion takes ~20 minutes.
