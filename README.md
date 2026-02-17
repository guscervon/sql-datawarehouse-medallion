# sql-datawarehouse-medallion
Building a Data Warehouse using medallion architecture

## Diagram
![Data Warehouse Architecture](./assets/datawarehouse_architecture.drawio.svg)


## **Run with Docker Compose**

- **Prerequisite:** Docker installed and `docker compose` available.
- **Start services:** Run the containers in detached mode.

```bash
docker compose up -d
```

- **Stop services:**

```bash
docker compose down
```

- **View logs:** Follow the database logs:

```bash
docker compose logs -f db
```

## **Run SQL files manually (psql)**

- **Default DB credentials (from `docker-compose.yaml`):**
  - **User:** postgres
  - **Password:** mypassword
  - **Database:** datawarehouse
  - **Port:** 5432

- **Connect from the host using `psql`:**

```bash
PGPASSWORD=mypassword psql -h localhost -p 5432 -U postgres -d datawarehouse
```

- **Execute a SQL file from the host:**

```bash
PGPASSWORD=mypassword psql -h localhost -p 5432 -U postgres -d datawarehouse -f scripts/schemas.sql
```
