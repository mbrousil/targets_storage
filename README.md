# targets_storage

### Background
This repository is meant to be a small vignette of the `AquaSat/AquaMatch_download_WQP` that 
documents some pain points in our use of {targets} with large data objects, especially regarding WQP 
downloads, which are not static. This architecture requires a google account to function, and if you intend to 
update the pipeline for the external world (i.e., create stable datasets for downstream use, e.g. AquaSat), 
you will need to authenticate using the ROSSyndicate google account.

The `run.R` file contains the suggested workflow for running this pipeline. As of 2/26/2024, completion takes ~20 minutes.

### Technical details
This repository uses the {targets} workflow management R package. Technical details on {targets} workflows are
available in the [{targets} User Manual](https://books.ropensci.org/targets/). {targets} workflows are built upon lists of
"targets", which can be thought of as analytical steps written out in code. 

#### Customization
We use `config.yml` to provide several options for profiles that can be used to run the pipeline based on the
behavior that the user needs. The [{config}](https://cran.r-project.org/web/packages/config/index.html) package
is used in the `workflow_config` target to grab the pipeline settings from that file, which are then applied in
other functions throughout the pipeline. 

+ The `default` profile is set up to 1) use the ROSSyndicate google account, 2) use a stable version of the
  chlorophyll dataset (i.e., previously downloaded), and 3) **not** upload a new stable version to Google Drive
  after the pipeline runs
+ The `create_new_version` profile does not use **nor** create a stable dataset. It's intended to allow the user
  to work primarily with the dynamically downloaded version. For this reason, the email is blank currently to indicate
  that it should be customized by the user
+ The `admin_update` profile creates a new stable dataset but does not use one as input. It's intended to update the current stable dataset
