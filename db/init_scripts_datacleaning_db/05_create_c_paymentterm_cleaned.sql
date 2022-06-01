CREATE TABLE machine_learning.c_paymentterm_cleaned
 (
     c_paymentterm_id        bigint                                 not null
             primary key,
     ad_client_id            bigint                                 not null,
     netdays                 bigint       default (0)::bigint       not null
 );