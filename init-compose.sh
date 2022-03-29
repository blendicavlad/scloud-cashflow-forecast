#!/bin/bash
container_name=data-cleaning-app
dev_container_name=data-cleaning-app-dev

# shellcheck disable=SC2162
read -p "Build dev container (y/n)? " answer
case ${answer:0:1} in
y | Y)
  declare -a files=("SCEU01_scloud_c_allocationline.csv" "SCEU01_scloud_c_invoice.csv" "SCEU01_scloud_c_payment.csv" "SCEU01_scloud_c_paymentterm.csv")
  for i in "${files[@]}"
  do
      if [ ! -f "db/init_scripts/$i" ]; then
        docker run --rm -ti -v "$(pwd)/.aws:/root/.aws" -v "$(pwd)/db/init_scripts:/aws" amazon/aws-cli s3 cp "s3://ml-cashflow-forecast/dev_data/$i" "$i" --region eu-central-1
      fi
  done
  echo Rebuild dev compose container...
  docker-compose -f docker-compose.dev.yml -p $dev_container_name up --build --no-start
  ;;
*)
  echo Rebuild compose container...
  docker-compose -p $container_name up --build --no-start
  ;;
esac
exit 1
