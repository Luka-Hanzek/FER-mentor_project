FROM apache/sedona:1.8.1

RUN apt update && apt install -y osmctools
RUN apt install -y python3-gpxpy
RUN apt install -y inotify-tools

ENV PYTHONPATH="${PYTHONPATH}:/dev-root"