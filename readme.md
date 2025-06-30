# THLHP Data Pipeline

This repository contains automation scripts for ingesting data from [PACS](https://en.wikipedia.org/wiki/Picture_archiving_and_communication_system) and [KoboToolbox](https://kobotoolbox.org/) into the project's central data repository. The scripts were written to run through [Windmill](https://windmill.dev/) which takes care of managing dependencies, environment variables, and resources; however, they can also be run independently.

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [DICOM Pipeline](#dicom-pipeline)
- [KoboToolbox Pipeline](#kobotoolbox-pipeline)
- [Support](#support)

## Overview

The repository has two main components:
1. **DICOM Pipeline**: For downloading, compressing, and managing medical imaging data from a remote PACS system, moving it to a central storage and ultimately to a researcher's local machine.
2. **KoboToolbox Pipeline**: For pulling form data from KoboToolbox and pushing it to a central database

## Requirements

Both scripts require a PostgreSQL database to function. While the DICOM data is stored in binary format on the local filesystem, the DICOM scripts use PostgreSQL as a job queue to track the status of each DICOM series. The KoboToolbox scripts use PostgreSQL to store the form data.

## Installation

There are two ways to setup the scripts:
1. **Windmill**: Clone the repository and push the scripts to your windmill instance using windmill cli. The scripts can also be integrated with git (e.g github, gitlab) to automatically sync changes to your windmill instance.
2. **Independent**: Clone the repository, remove the `import wmill` dependency from any individual python script and run them as normal. This requires some additional effort even though the windmill libraries do not do any of the heavy lifting and are mostly responsible for environment variable management and resource access. This can be replaced by using a local configuration file or environment variables.

## DICOM Pipeline

This pipeline handles the end-to-end process of retrieving, validating, and distributing medical imaging data from a PACS system. The DICOM scripts are designed to run in parallel and can be executed in multiple instances to optimize performance based on available bandwidth. The system uses PostgreSQL as a job queue to track the status of each DICOM series throughout the pipeline. 

![DICOM Pipeline](/images/dicom_pipeline.png)
*High level overview of the DICOM pipeline*

### Step 1: Metadata transfer

- `f/dicoms/db_insert_patients.py` - Queries the PACS for any project participants and stores their metadata in the database.
- `f/dicoms/db_insert_studies.py` - Queries the PACS for study details of any project participants and stores the study metadata in the database.
- `f/dicoms/db_insert_series.py` - Queries the PACS for series details of any project studies and stores the series metadata in the database.

### Step 2: Download DICOM files
- `f/dicoms/download_dicom_files.py` - Downloads the DICOM files that are missing from the local filesystem and marks them as downloaded in the database.
- `f/dicoms/validate_series.py` - Validates the downloaded DICOM files by matching the downloaded file count with the expected file count.
- `f/dicoms/compress_series.py` - Compresses each completed series into a single zip file. 


### Step 3: Move to central storage
- `f/dicoms/sync_chile_asu.sh` - Simple bash `rsync` scripts that syncs the compressed series to the central storage.
- `f/dicoms/prepare_daily_extracted_dicoms.py` - Extracts DICOMs from the last 48 hours in a given directory for manual quality control.


### Step 4: Nightly selective sync
Nightly sync data from central repository to researcher's local machine. This step is managed entirely by [Globus Timers](https://docs.globus.org/api/timers/)

## KoboToolbox Pipeline

- `f/kobo/query_data.py` - Script to query KoboToolbox API and store the result in the central database in a JSONB column.
- `f/kobo/pull_all_forms.flow/flow.yaml` - Iterates over all forms and calls `query_data.py` for each form.

## Support

For support or implementing this pipeline in your own environment, feel free to reach out to me at [Suhail.Ghafoor@asu.edu](mailto:Suhail.Ghafoor@asu.edu)
