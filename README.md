# AAK Tilsyn Sync

This robot syncs case data from **PEZ** (henstillinger/parking tickets) and **Vejman** (road/equipment permissions) into an Azure Cosmos DB container (`aak-tilsyn` / `TilsynItems`), which serves as the backend for the AAK Tilsyn app.

## Process Overview

### process.py

The main orchestration. It runs a single unified sync that:

1. Retrieves credentials for PEZ, Vejman, and Cosmos DB from OpenOrchestrator.
2. Connects to the `TilsynItems` Cosmos DB container.
3. Fetches cases from PEZ and Vejman (see below).
4. For each case, builds a document and checks whether it already exists in Cosmos:
   - **New document** — inserts it with app-field defaults (e.g. `FakturaStatus: "Ny"`).
   - **Existing document with changes** — patches only sync-owned fields (never overwrites app fields like `FakturaStatus`). Uses batched patch operations (Cosmos limits patches to 10 ops per call) with `content_hash` always last so a partial failure triggers a retry on the next run.
   - **Existing document, unchanged** — skipped (compared via `content_hash`).
5. Coordinates are resolved through a pipeline: source coordinates are used if valid and not suspiciously close to the depot (Blixens), otherwise the address is geocoded via Nominatim, and finally any existing coordinates are kept as a fallback.
6. Company names for PEZ cases are looked up via the CVR API when not provided by PEZ itself.
7. Logs a summary of created, updated, and unchanged documents.

### pez.py

Handles authentication and data extraction from the PEZ parking ticket system (pez.giantleap.net):

1. Logs in via OAuth (username/password) and obtains an access token.
2. Paginates through all cases of type `PARKING_TICKET` in a specific workflow step.
3. For each case, fetches the detail view and the vehicle owner info.
4. Filters violations (forseelser) to only those with allowed codes (e.g. `1B.`, `2B.`, etc.) that map to specific invoice line types (fakturalinje).
5. Validates the CVR number using a mod-11 check.
6. Returns a list of cases with address, coordinates, CVR, violations, and dates.
7. Provides `add_sent_to_tilsyn_comment()` which posts an internal comment back on the PEZ case when it is first synced to Tilsyn, listing which violations were sent.

### vejman.py

Fetches road permission cases from the Vejman API (vejman.vd.dk):

1. Queries three separate case lists: expired permissions (state 3), completed permissions (state 8), and new permissions (states 3/6/8/12) — filtered to yesterday/today date ranges.
2. Deduplicates by case number and filters to cases belonging to allowed caseworkers (by initials).
3. For each case, fetches the detail view to get site/building info for a more precise address.
4. Extracts coordinates from LINESTRING geometry data and converts from EPSG:25832 (UTM zone 32N) to WGS84 (lat/lon).
5. Returns a list of cases with address, coordinates, applicant, equipment type, dates, and case metadata.

---

# Robot-Framework V4

This repo is meant to be used as a template for robots made for [OpenOrchestrator](https://github.com/itk-dev-rpa/OpenOrchestrator) v2.

## Quick start

1. To use this template simply use this repo as a template (see [Creating a repository from a template](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template)).
__Don't__ include all branches.

2. Go to `robot_framework/__main__.py` and choose between the linear framework or queue based framework.

3. Implement all functions in the files:
    * `robot_framework/initialize.py`
    * `robot_framework/reset.py`
    * `robot_framework/process.py`

4. Change `config.py` to your needs.

5. Fill out the dependencies in the `pyproject.toml` file with all packages needed by the robot.

6. Feel free to add more files as needed. Remember that any additional python files must
be located in the folder `robot_framework` or a subfolder of it.

When the robot is run from OpenOrchestrator the `main.py` file is run which results
in the following:

1. The working directory is changed to where `main.py` is located.
2. A virtual environment is automatically setup with the required packages.
3. The framework is called passing on all arguments needed by [OpenOrchestrator](https://github.com/itk-dev-rpa/OpenOrchestrator).

## Requirements

Minimum python version 3.11

## Flow

This framework contains two different flows: A linear and a queue based.
You should only ever use one at a time. You choose which one by going into `robot_framework/__main__.py`
and uncommenting the framework you want. They are both disabled by default and an error will be
raised to remind you if you don't choose.

### Linear Flow

The linear framework is used when a robot is just going from A to Z without fetching jobs from an
OpenOrchestrator queue.
The flow of the linear framework is sketched up in the following illustration:

![Linear Flow diagram](Robot-Framework.svg)

### Queue Flow

The queue framework is used when the robot is doing multiple bite-sized tasks defined in an
OpenOrchestrator queue.
The flow of the queue framework is sketched up in the following illustration:

![Queue Flow diagram](Robot-Queue-Framework.svg)

## Linting and Github Actions

This template is also setup with flake8 and pylint linting in Github Actions.
This workflow will trigger whenever you push your code to Github.
The workflow is defined under `.github/workflows/Linting.yml`.
