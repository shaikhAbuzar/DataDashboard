docker run --name my_postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -v $PWD/../Data/db_data:/var/lib/postgresql/data \
  -d \
  -p 5432:5432 \
  postgres