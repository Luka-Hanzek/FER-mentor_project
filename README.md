# FER apache sedona benchmark framework

## 1. About

This is a minimal implementation of benchmarking framework in apache sedona.


## 2. Functionality

1. Convert OSM data (.osm or .osm.pbf) format into parquet format.

    See: `scripts/python/create_geometries.py`.
    
    This script creates a "geometry" column and separates data into "points" and "ways".

2. Run the benchmark.

    See: `sedona_fer/bench/run.py`

    This script reads the `sedona_fer/bench/bench_config.yaml` and runs the benchmark according to the definition.
    
    See the contents of `bench_config.yaml`. It's self explanatory.

    The benchmark outputs results into `.json` and creates some plots to visualize the results.

## 3. How to run

This project is best set up for VSCode development. All the depencencies are in the docker container.


1. To start the container:

        cd docker/sedona
        docker compose build
        docker compose up -d

    This will start the container with all the dependencies set up.

    If you just want to run the code, execute below line and see: **2. Functionality**

        docker exec -it sedona bash

    If you want to develop follow next steps:

2. Start VSCode

    2.1. Install extension "Dev Containers"

    2.2. Attach VSCode instance to the running container (sedona).

    2.3. Open folder `dev-root`.

The entire project is visible in the container at `/dev-root` directory.
You can now do anything you would do on your local machine.

### NOTE:

`launch.json` and `tasks.json` are available so you can debug and run tasks.

Tasks:
- *Run benchmark*
- *Run create_geometries*

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
- `scripts`: scripts
- `sedona_fer`: benchmarking and related functionality
- `pyosmium`: failed attempt to use "pyosmium" package to create geometries. Extremely slow.
