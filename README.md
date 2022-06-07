# scloud-cashflow-forecast

Machine learning service for SorcrateCloud ERP 

purpose: predicts if an invoice or batch of invoices will be paid and in how much time in days

run init-compose.sh when you firstly initialize the app and test data
- must have AWS credentials set in the .aws/credentials file to download the test data
  - `aws_access_key_id={}`
  - `aws_secret_access_key={}`
- dev mode also builds a postgres image with test data and tables (data insertion should take around 30 mins)

run ` docker-compose -f docker-compose.dev.yml up ` to update and run the dev container

run ` docker-compose up ` to update and run the prod container

to download the test data, you must set aws creds env variables

the prod app is made of 4 docker services:
- data_cleaning_db: postgres container used as a data lake
- data-cleaning: run as a AWS ECS Task triggered by a scheduled CloudWatch Event -> starts data_cleaning_db postgres container which acts as a data lake for the processed and aggregated data for each client and business partner
- model-producer: run as a AWS ECS Task triggered by a scheduled CloudWatch Event, after data-cleaning task has been run -> uses the data from data_cleaning_db to build predictive regression and classification models after probing for the best one for each client and uploads them and the plots of the ROC curve of every client to AWS S3
- model-consumer: run as a EC2 app or Lambda function -> accepts a csv file in a multi-part request and an ad_client_id as request parameter -> retrieves the models for the given client and the data from the data-lake for the business partners of the given invoices and returns the prediction data response as a list of JSON objects for each given invoice.

the dev app is made of the containers mentioned above, plus:
- dev_db: database that automatically initializes with the required tables and test data downloaded from S3 by init-compose.sh, to simulate a production environment
- jupyter: jupyter notebook for fast prototyping, testing and plotting 

The given csv must contain the following fields:
- c_invoice_id: id of the invoice -> int64
- ad_org_id: id of the organization -> int64
- dateinvoiced: date of the invoice -> date of format YYYY-MM-DD
- c_bpartner_id: id of the business partner -> int64
- c_bpartner_location_id: id of the business partner invoice location from the invoice -> int64
- paymentrule: the payment rule of the invoice (A/B/C/D/I/K/M/P/S/T)
- grandtotal: the total amt of the invoice -> numeric
- duedate: due date of the invoice -> date of format YYYY-MM-DD
- totalopenamt: amt still unpaid -> numeric
- paidamt: amt paid -> numeric
- tendertype: payment method of the linked payment of the invoice (A/C/D/I/C/null)

The response is a json containing all the given columns plus:
- paid: the prediction if the given invoice will be paid 1=True, 0=False
- daystosettle_pred: the prediction of the time period in days until the invoice will be paid

TODO: 
- implemet Loggly hooks
- change default server implementation of model-consumer (currently using the flask default implementation, which is not suitable for production use)
- implement nginx reverse proxy as container
- maybe add terraform infrastructure-as-code for easier deployment and CI-CD
- integration/unit tests
