create table machine_learning.regression_stats(
    regression_stats_id SERIAL PRIMARY KEY ,
    ad_client_id bigint,
    date_run timestamp(0) default current_timestamp::timestamp,
    test_mse numeric,
    test_r2 numeric,
    train_r2 numeric
);