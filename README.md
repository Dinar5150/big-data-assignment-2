# big-data-assignment-2

This repository follows the course template for Assignment 2.

## How to run

1. Install Docker and Docker Compose.
2. From the repository root run:

```bash
docker compose up
```

The master container mounts the local `app/` folder to `/app` and runs `/app/app.sh`.

## Repository layout

- `docker-compose.yml`: Hadoop master, Hadoop worker, and Cassandra containers
- `app/`: all scripts and Python code used by the assignment
- `report.pdf`: placeholder report file that you should replace before submission

## Notes

- The scripts are written to rebuild the index from scratch each time.
- Put one parquet file from the provided dataset at `app/dataset/a.parquet`.
