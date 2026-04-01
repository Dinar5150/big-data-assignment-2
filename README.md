# big-data-assignment-2

## How to run

1. Install Docker and Docker Compose.
2. The dataset file `a.parquet` is already part of the repository.
   You can get it in one of these two ways:

   - if you cloned the repository with Git LFS, run:

```bash
git lfs pull
```

   - or place the file manually at:

```bash
app/a.parquet
```

3. Start the project:

```bash
docker compose up
```

This will create three containers: a master node, a worker node, and a Cassandra server.  
The master node runs `/app/app.sh`.
