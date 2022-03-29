CREATE TABLE machine_learning.c_allocationline_cleaned
(
    c_allocationline_id      bigint                                 not null
            primary key,
    ad_client_id             bigint                                 not null,
    ad_org_id                bigint                                 not null,
    c_invoice_id             bigint,
    c_bpartner_id            bigint,
    c_payment_id             bigint,
    c_cashline_id            bigint,
    amount                   numeric      default (0)::numeric      not null
);