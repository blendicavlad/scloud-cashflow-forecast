CREATE TABLE machine_learning.c_invoice_cleaned(
    c_invoice_id               bigint                                  not null
            primary key,
    ad_client_id               bigint                                  not null,
    ad_org_id                  bigint                                  not null,
    dateinvoiced               timestamp(0)                            not null,
    c_bpartner_id              bigint                                  not null,
    c_bpartner_location_id     bigint                                  not null,
    c_currency_id              bigint                                  not null,
    paymentrule                char          default 'P'::bpchar       not null,
    c_paymentterm_id           bigint                                  not null,
    grandtotal                 numeric       default (0)::numeric      not null,
    c_payment_id               bigint,
    c_cashline_id              bigint,
    duedate                    timestamp(0),
    totalopenamt               numeric       default (0)::numeric      not null,
    paidamt                    numeric       default (0)::numeric      not null
);