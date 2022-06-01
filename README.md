# scloud-cashflow-forecast



Run init-container.sh to create the docker container for the api (run only in special cases, preferably always run compose)

run init-compose.sh when you firstly initialize the app and test data
- must have AWS credentials set in the .aws/credentials file, to download the test data
  - `aws_access_key_id={}`
  - `aws_secret_access_key={}`
- dev mode also builds a postgres image with test data and tables (data insertion should take around 30 mins)

run ` docker-compose -f docker-compose.dev.yml up ` to update and run the dev container

run ` docker-compose up ` to update and run the prod container

to download the test data, you must set env variables
don't forget to set up a remote docker python interpreter from the IDE
