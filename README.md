# OVERWATCH Orthorectification Tool

This project provides an automated pipeline for processing drone imagery through OpenDroneMap (NodeODM) and uploading results to a CKAN datalake.


## Requirements

* Docker and Docker Compose
* Python 3.12+ (for local development)
* uv package manager (for local development)

## Installation

There are two ways of installing this tool: either fully containerized (the recommended option for a cleaner installation), or hybrid.
Regardless, NodeODM requires to be launched from a Docker container, unless provided externally.

### Docker Setup (Recommended)

1. Clone the repository:

```
git clone git@github.com:links-ads/overwatch-orthophotos.git
cd overwatch-orthophotos
```

2. Create your configuration:

```
cp configs/template.yml configs/your-target.yml # e.g., configs/dev.yml
```

3. Set up your data directory:

```
# link to your data directory (adjust path as needed)
ln -s /path/to/your/drone-imagery/data data
```

4. build and start services

```
# start NodeODM service
docker-compose up -d nodeodm

# build ODM Tools image
TARGET=your-target docker-compose build odm-tools
```

### Local Development Setup

1. Install `uv`

```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install dependencies

```
uv sync
```

3. Install the `odm_tools` as editable dependency

```
uv pip install -e .
```

## Usage

Both installations will provide an entrypoint called `odm-tools`. This CLI provides three subcommands to launch, monitor, list, and cleaup tasks.

1. `process` allows to start new orthorectification tasks. If the selected directory has already been completed or in progress, the CLI will
respectively skip or track the currently active task.

2. `list` provides a simple way to list ongoing tasks with a few set of filters (e.g., request, task status).

3. `cleanup` allows to remove old tasks (also to repeat them), providing some filtering utilities to select what to clean up.

### Using Docker

The ODM Tools service is configured with a `manual` profile to prevent auto-start. Use `docker compose` run to execute commands:

```
# syntax
docker-compose run --rm odm-tools [COMMAND] [ARGS]
# general help
docker-compose run --rm odm-tools --help
# command-specific help
docker-compose run --rm odm-tools [COMMAND] --help

```

For instance:
```
# List all tasks
docker-compose run --rm odm-tools list

# List tasks with specific status
docker-compose run --rm odm-tools list -s completed failed

# List tasks for specific request
docker-compose run --rm odm-tools list -r data/aukerman
```

### Local Usage

First, you'll need to provide a valid `config.yml` file in the project's root directory.
This can be done by copying or linking a config file inside `configs/`, e.g.:

```
ln -s path/to/configs/ config.yml
```

then, similar to the Docker setup, if you have the package installed locally:

```
# syntax
odm-tools [COMMAND] [ARGS]
# general help
odm-tools --help
# command-specific help
odm-tools [COMMAND] --help
```

> [!WARNING]
> In this case, remember to update NodeODM's host from `nodeodm` to `localhost`.

## Project Structure

The project is organized as follows:

```bash
overwatch/
├── configs/ # configuration files
│   ├── template.yml
│   ├── dev.yml
│   └── ... 
├── containers/
│   └── ... # docker specific stuff
├── data/
│   ├── request_1/
│   │   ├── rgb/
│   │   ├── thermal/
│   │   └── request.json # dump of the original drone mission request
│   ├── request_2/
│   │   └── ...
│   └── ... # each request its own folder
├── src/
│   └── odm_tools/
│       ├── __init__.py
│       └── ...
├── tests/
│   ├── ...
├── tools/
│   ├── ...
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
├── README.md
└── LICENSE
```

## Key Directories

- **`configs/`** - YAML configuration files for different environments
- **`containers/`** - Docker build files
- **`data/`** - Symlink to drone imagery input data. The expected folder structure for each drone mission is as follows:

```
├── data/
│   ├── request_id/
│   │   ├── rgb/            # folder containing RGB JPG files
│   │   │   ├── 1234.jpg
│   │   │   ├── ...
│   │   │   └── 5678.jpg
│   │   ├── thermal/        # folder containing IR JPG files
│   │   │   ├── 1234.jpg
│   │   │   ├── ...
│   │   │   └── 5678.jpg
│   │   └── request.json    # JSON file containing the drone mission request, as received from the message bus.
```

- **`src/odm_tools/`** - Main Python package source code
- **`tests/`** - Unit tests and test data
- **`tools/`** - Utility scripts for data processing

Once the task is completed, the tool will dump the results inside each request directory, using ODM's standard directory structure, e.g.:

```
outputs/
├── odm_dem/
├── odm_georeferencing/
├── odm_orthophoto/
└── odm_report/
```