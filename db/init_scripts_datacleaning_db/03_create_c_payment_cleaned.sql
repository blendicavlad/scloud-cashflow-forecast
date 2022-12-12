CREATE TABLE cashflow_forecast.c_payment_cleaned(
    c_payment_id                   bigint                                       not null
            primary key,
    ad_client_id                   bigint                                       not null,
    ad_org_id                      bigint                                       not null,
    datetrx                        timestamp(0)                                 not null,
    c_doctype_id                   bigint                                       not null,
    c_bpartner_id                  bigint,
    c_invoice_id                   bigint,
    tendertype                     char         default 'K'::bpchar             not null,
    c_currency_id                  bigint,
    payamt                         numeric      default (0)::numeric            not null,
    isallocated                    char         default 'N'::bpchar             not null,
    duedate                        timestamp(0),
    allocatedamt                   numeric      default (0)::numeric            not null
);