create table cashflow_forecast.c_payment
(
    c_payment_id                   bigint                                       not null
        constraint pk335
            primary key,
    ad_client_id                   bigint                                       not null,
    ad_org_id                      bigint                                       not null,
    isactive                       char         default 'Y'::bpchar             not null,
    created                        timestamp(0) default clock_timestamp()       not null,
    createdby                      bigint       default (0)::bigint             not null,
    updated                        timestamp(0) default clock_timestamp()       not null,
    updatedby                      bigint       default (0)::bigint             not null,
    documentno                     varchar(60)  default NULL::character varying not null,
    datetrx                        timestamp(0)                                 not null,
    isreceipt                      char         default 'N'::bpchar             not null,
    c_doctype_id                   bigint                                       not null,
    trxtype                        char         default 'S'::bpchar             not null,
    c_bankaccount_id               bigint                                       not null,
    c_bpartner_id                  bigint,
    c_invoice_id                   bigint,
    c_bp_bankaccount_id            bigint,
    c_paymentbatch_id              bigint,
    tendertype                     char         default 'K'::bpchar             not null,
    creditcardtype                 char         default 'M'::bpchar,
    creditcardnumber               varchar(20),
    creditcardexpmm                bigint       default (1)::bigint,
    creditcardexpyy                bigint       default (8)::bigint,
    micr                           varchar(20),
    routingno                      varchar(20),
    accountno                      varchar(20),
    checkno                        varchar(20),
    a_name                         varchar(60),
    a_street                       varchar(60),
    a_city                         varchar(60),
    a_state                        varchar(40),
    a_zip                          varchar(20),
    a_ident_dl                     varchar(20),
    a_ident_ssn                    varchar(20),
    a_email                        varchar(255) default NULL::character varying,
    voiceauthcode                  varchar(20),
    orig_trxid                     varchar(20),
    ponum                          varchar(60),
    c_currency_id                  bigint                                       not null,
    payamt                         numeric      default (0)::numeric            not null,
    discountamt                    numeric      default (0)::numeric,
    writeoffamt                    numeric      default (0)::numeric,
    taxamt                         numeric,
    isapproved                     char         default 'N'::bpchar             not null,
    r_pnref                        varchar(20),
    r_result                       varchar(20),
    r_respmsg                      varchar(60),
    r_authcode                     varchar(20),
    r_avsaddr                      char,
    r_avszip                       char,
    r_info                         varchar(2000),
    processing                     char         default 'N'::bpchar,
    oprocessing                    char,
    docstatus                      char(2)      default 'DR'::bpchar            not null,
    docaction                      char(2)      default 'CO'::bpchar            not null,
    isreconciled                   char         default 'N'::bpchar             not null,
    isallocated                    char         default 'N'::bpchar             not null,
    isonline                       char         default 'N'::bpchar             not null,
    processed                      char         default 'N'::bpchar             not null,
    posted                         char         default 'N'::bpchar             not null,
    isoverunderpayment             char         default 'N'::bpchar             not null,
    overunderamt                   numeric      default (0)::numeric,
    a_country                      varchar(40),
    c_project_id                   bigint,
    isselfservice                  char         default 'N'::bpchar             not null,
    chargeamt                      numeric,
    c_charge_id                    bigint,
    isdelayedcapture               char         default 'N'::bpchar             not null,
    r_authcode_dc                  varchar(20),
    r_cvv2match                    char         default 'N'::bpchar,
    r_pnref_dc                     varchar(20),
    swipe                          varchar(80),
    ad_orgtrx_id                   bigint,
    c_campaign_id                  bigint,
    c_activity_id                  bigint,
    user1_id                       bigint,
    user2_id                       bigint,
    c_conversiontype_id            bigint,
    description                    varchar(255),
    dateacct                       timestamp(0)                                 not null,
    c_order_id                     bigint,
    isprepayment                   char         default 'N'::bpchar             not null,
    ref_payment_id                 bigint,
    issotrx                        char         default 'Y'::bpchar             not null,
    a_asset_id                     bigint,
    employee_id                    bigint,
    duedate                        timestamp(0),
    depositiondate                 timestamp(0),
    processingso                   char         default 'N'::bpchar,
    sourcedoc_id                   bigint,
    c_ticket_id                    bigint,
    ticketno                       varchar(40),
    bill_location_id               bigint,
    generateinvoice                char(2)      default NULL::bpchar,
    cashondeliverypaymentdocno     varchar(30),
    courierservice                 char(3),
    trackingno                     varchar(60),
    c_duewithholding_id            bigint,
    m_product_category_capex_id    bigint,
    m_product_subcategory_capex_id bigint,
    so_capexopexcategory_id        bigint,
    so_capexopexsubcategory_id     bigint,
    c_invoicepayschedule_id        bigint,
    c_orderpayschedule_id          bigint,
    c_paymentdebtschedule_id       bigint,
    c_paymentdebt_id               bigint,
    reversaldate                   timestamp(0),
    allocatedamt                   numeric      default (0)::numeric            not null,
    c_foreclosuredeed_id           bigint,
    isrdsprepayment                char         default 'N'::bpchar,
    istvapayment                   char         default 'N'::bpchar,
    c_so_contract_id               bigint,
    isdistributedonprojects        char         default 'N'::bpchar
);