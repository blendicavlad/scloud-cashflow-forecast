create table machine_learning.classification_stats (
    classification_stats_id SERIAL PRIMARY KEY,
    ad_client_id bigint,
    date_run timestamp(0) default current_timestamp::timestamp,
    auc numeric,
    precision numeric,
    recall numeric,
    accuracy numeric,
    score numeric,
    fp bigint,
    fn bigint,
    tp bigint,
    tn bigint
);