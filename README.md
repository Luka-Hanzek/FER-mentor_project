# FER apache sedona benchmark framework

## 1. About

This is a minimal implementation of benchmarking framework in apache sedona.


## 2. Functionality

1. Convert OSM data (.osm or .osm.pbf) format into parquet format.

    See: `scripts/python/create_geometries.py`.

    This script creates a "geometry" column and separates the original data into "points" and "ways".<br>
    Relations are much more complex to parse so this is not yet implemented.

2. Run the benchmark.

    See: `scripts/python/run_benchmark.py`

    This script reads the config file specified as `--config` argument and runs the benchmark according its definition.<br>
    Example config is in the root directory: `bench_config.yaml`.<br>

    The benchmark outputs results into `.json` and creates some plots to visualize the results.

    #### Example output from a query
    ![](/assets/fig.png)

    #### Example plots from a query
    ![](/assets/plot1.png) ![](/assets/plot2.png)

3. Templating

    `scripts/bash/watch-templates <templates-dir>` generates the following as a starting point:
    - `<templates-dir>/`
    - `<templates-dir>/templates/`
    - `<templates-dir>/templates/gen.py`
    - `<templates-dir>/templates/empty.sql`
    - `<templates-dir>/instantiated_templates/`

    It watches the `<templates-dir>/templates/` directory for file changes (either `gen.py` or `*.sql` files)
    and generates the instantiations of a template to `<templates-dir>/instantiated_templates/` directory.<br>
    The script `<templates-dir>/templates/gen.py` is used to modify the template.

## 3. How to run

This project is best set up for VSCode development. All the depencencies are in the docker container.

1. To start the container:

        cd docker/sedona
        docker compose build
        docker compose up -d

    This will start the container with all the dependencies set up.

    If you just want to run the code, execute below line and see: **2. Functionality**

        docker exec -it sedona bash

    If you want to develop in the container, follow the next steps:

2. Start VSCode

    2.1. Install extension "_Dev Containers_".<br>
    2.2. Attach VSCode instance to the running container "_sedona_".<br>
    2.3. Open folder `dev-root`.<br>
    2.4. Attach a new terminal with command palette "_Terminal: Create New Terminal_".<br>

The entire project is visible in the container at `/dev-root` directory.
You can now do anything you would do on your local machine.

### NOTES:

`launch.json` and `tasks.json` are available so you can debug and run tasks (see: **2. Functionality**).<br>
Tasks:
- *Run benchmark*
- *Run create_geometries*
- *Print schemas*

There are predefined datasets in `data/test-data` directory.

### IMPORTANT:

`Dockerfile` adds the `sedona_fer` module into `PYTHONPATH`. This means that you can import stuff from anywhere:

    import sedona_fer
    import sedona_fer.data
    import sedona_fer.bench

    from sedona_fer.data.import_export import ParquetLoader
    ...


## 4. Folder structure

- `.vscode`: vscode settings
- `docker/sedona`: docker setup for running and development
- `scripts`: runnable scripts
- `sedona_fer`: benchmarking and related functionality
- `pyosmium`: failed attempt to use "pyosmium" package to create geometries. Extremely slow.


## 5. Design decisions

- geometries are constructed explicitly in sedona because for large datasets (see: https://download.geofabrik.de/europe.html) other tools are either too slow (pyosmium) or unable to create geometries (ogr2ogr).
- there is no good reason this project uses docker container. It was just in case it turns out there's many dependencies. This could've been python virtual environment afterall.
