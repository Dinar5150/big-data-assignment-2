# big-data-assignment-2

## How to run

1. Install Docker and Docker Compose.
2. If you cloned the repository with Git LFS, run:

```bash
git lfs pull
```

3. Start the project:

```bash
docker compose up
```

This will create three containers: a master node, a worker node, and a Cassandra server.  
The master node runs `/app/app.sh`.
