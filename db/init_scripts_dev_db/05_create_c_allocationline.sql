create table cashflow_forecast.c_allocationline
(
    c_allocationline_id      bigint                                 not null
        constraint pk390
            primary key,
    ad_client_id             bigint                                 not null,
    ad_org_id                bigint                                 not null,
    isactive                 char         default 'Y'::bpchar       not null,
    created                  timestamp(0) default clock_timestamp() not null,
    createdby                bigint       default (0)::bigint       not null,
    updated                  timestamp(0) default clock_timestamp() not null,
    updatedby                bigint       default (0)::bigint       not null,
    datetrx                  timestamp(0),
    allocationno             varchar(30),
    ismanual                 char         default 'N'::bpchar,
    posted                   char         default 'N'::bpchar             not null,
    c_invoice_id             bigint,
    c_bpartner_id            bigint,
    c_order_id               bigint,
    c_payment_id             bigint,
    c_cashline_id            bigint,
    amount                   numeric      default (0)::numeric      not null,
    discountamt              numeric      default (0)::numeric      not null,
    writeoffamt              numeric      default (0)::numeric      not null,
    overunderamt             numeric,
    c_allocationhdr_id       bigint                                 not null,
    ad_orgtrx_id             bigint,
    c_activity_id            bigint,
    sourcedoc_id             bigint,
    cashdiscountamt          numeric,
    closed                   char         default 'N'::bpchar,
    withholdingamt           numeric      default (0)::numeric      not null,
    c_duewithholding_id      bigint,
    c_invoicepayschedule_id  bigint,
    c_paymentdebt_id         bigint,
    c_paymentdebtschedule_id bigint
);

