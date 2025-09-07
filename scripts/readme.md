Why COPY Instead of \copy in bronze.load_bronze()
The bronze.load_bronze() function uses PostgreSQL’s COPY command to load CSV data into the bronze layer tables. I chose COPY over \copy for the following reasons:

Server-Side ETL Logic: COPY is embedded in a PostgreSQL function, demonstrating reusable, server-side ETL logic, a common pattern in data engineering pipelines where data files are accessible to the database server.
Local Setup Compatibility: As this is a local project, the PostgreSQL server has access to the CSV files (e.g., 'D:/Data Engineer/...'), and permissions are configured, making COPY seamless and effective.
Simplicity: Since the function already works with COPY and the data is successfully loaded, no code changes are needed.

Alternatively, \copy (a psql meta-command) could be used for client-side file loading, which is more portable for non-local setups. However, \copy cannot be used in functions and would require a separate script, adding complexity for this local project. For reviewers, a psql script using \copy is provided in load_data_psql.sql as an alternative.
