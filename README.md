## How to run the project
`1` Make sure docker is running, then create a docker network and spin up the master, workers services
```console
docker network create sparkNetwork | docker-compose up -d
```
`2` You can scale up the workers if your machine can handle it
```console
docker-compose up -d --scale spark-worker=3
```
`3` Run the myspark service
```console
docker-compose run --rm myspark
```

## What's running

- **spark-master** — the cluster coordinator, UI at `localhost:8080`
- **spark-worker** — the muscle, scale these up if your machine allows
- **myspark** — your job, runs `runner.py` against the cluster
- **cadvisor** — watches every container (CPU, memory, network, disk) at `localhost:8090`
- **process-exporter** — goes deeper, tracks individual JVM and Python processes at `localhost:9256`
- **prometheus** — collects and stores all the metrics at `localhost:9090`
- **grafana** — where you actually look at everything at `localhost:3000`

## Setting up Grafana
Open `http://localhost:3000` and log in with `admin` and whatever password you set in the compose file.

First, connect it to Prometheus. Go to **Connections → Data Sources → Add → Prometheus** and set the URL to `http://prometheus:9090`. Hit **Save & Test** and it should say it connected successfully.

Then import the two dashboards that actually matter for this setup. Go to **Dashboards → Import**, paste the ID, hit Load, pick your Prometheus data source, and import:

- `14282` — container view from cAdvisor (CPU, memory, network per container)
- `249` — process view (per JVM/Python process, threads, memory)

You only need to do this once. The dashboards persist in the `grafana_data` volume.


## A few things worth knowing

**Scaling workers** — the worker has a hard cap of 4 CPUs and 5 GiB RAM set in the compose file. Keep that in mind before spinning up 5 of them on a 16 GiB machine.

**process-exporter on Windows** — Docker on Windows will sometimes create a directory instead of mounting a single config file. The fix is already applied here: we mount the whole `process-exporter-config/` folder instead of the yml file directly. Don't change this.

**myspark restarts on failure** — if your job crashes, Docker will automatically retry it. Check `docker logs myspark` if it's looping.

**Checking Prometheus targets** — go to `http://localhost:9090`, then Status → Targets. Both `cadvisor` and `process_exporter` should show as `UP`. If either is down, the corresponding container probably isn't running.

Spark config is hardcoded in runner.py, executor memory, cores, and driver memory are set directly in the SparkSession builder. If you want to change them (say, after scaling workers or on a beefier machine), edit the values there before running the job.


## Dataset
This project uses the NYC TLC High Volume For-Hire Vehicle trip record data for June 2025 (`fhvhv_tripdata_2025-06.parquet`).

Download it from the official NYC Taxi and Limousine Commission site:
**https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page**

Scroll to 2025, find June, and download the High Volume For-Hire Vehicle file and rename the downloaded data to 'tripdata_2025-06'. Place it in the `data/` folder before running the job.
