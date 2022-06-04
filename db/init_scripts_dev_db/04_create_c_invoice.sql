create table machine_learning.c_invoice
(
    c_invoice_id               bigint                                  not null
        constraint pk318
            primary key,
    ad_client_id               bigint                                  not null,
    ad_org_id                  bigint                                  not null,
    isactive                   char          default 'Y'::bpchar       not null,
    created                    timestamp(0)  default clock_timestamp() not null,
    createdby                  bigint        default (0)::bigint       not null,
    updated                    timestamp(0)  default clock_timestamp() not null,
    updatedby                  bigint        default (0)::bigint       not null,
    issotrx                    char          default NULL::bpchar      not null,
    documentno                 varchar(30)                             not null,
    docstatus                  char(2)       default 'DR'::bpchar      not null,
    docaction                  char(2)       default 'CO'::bpchar      not null,
    processing                 char          default 'N'::bpchar,
    processed                  char          default 'N'::bpchar       not null,
    posted                     char          default 'N'::bpchar       not null,
    c_doctype_id               bigint        default (0)::bigint       not null,
    c_doctypetarget_id         bigint                                  not null,
    c_order_id                 bigint,
    isapproved                 char          default NULL::bpchar      not null,
    istransferred              char          default 'N'::bpchar       not null,
    isprinted                  char          default 'N'::bpchar       not null,
    dateinvoiced               timestamp(0)                            not null,
    dateprinted                timestamp(0),
    dateacct                   timestamp(0)                            not null,
    c_bpartner_id              bigint                                  not null,
    c_bpartner_location_id     bigint                                  not null,
    poreference                varchar(60)   default NULL::character varying,
    isdiscountprinted          char          default 'N'::bpchar       not null,
    dateordered                timestamp(0)  default NULL::timestamp without time zone,
    c_currency_id              bigint                                  not null,
    paymentrule                char          default 'P'::bpchar       not null,
    c_paymentterm_id           bigint                                  not null,
    c_charge_id                bigint,
    chargeamt                  numeric,
    totallines                 numeric       default (0)::numeric      not null,
    grandtotal                 numeric       default (0)::numeric      not null,
    istaxincluded              char          default 'N'::bpchar       not null,
    c_campaign_id              bigint,
    c_project_id               bigint,
    c_activity_id              bigint,
    ispaid                     char          default 'N'::bpchar       not null,
    c_payment_id               bigint,
    c_cashline_id              bigint,
    createfrom                 char,
    generateto                 char,
    sendemail                  char          default 'N'::bpchar       not null,
    ad_user_id                 bigint,
    copyfrom                   char,
    isselfservice              char          default 'N'::bpchar       not null,
    ad_orgtrx_id               bigint,
    user1_id                   bigint,
    user2_id                   bigint,
    c_conversiontype_id        bigint,
    ispayschedulevalid         char          default 'N'::bpchar       not null,
    ref_invoice_id             bigint,
    isindispute                char          default 'N'::bpchar       not null,
    invoicecollectiontype      char,
    matchrequirementi          char,
    m_pricelist_id             bigint                                  not null,
    dateregistered             timestamp(0),
    isreturntrx                char          default 'N'::bpchar       not null,
    a_asset_id                 bigint,
    employee_id                bigint,
    c_so_customsbillofentry_id bigint,
    c_bp_bankaccount_id        bigint,
    c_projectphase_id          bigint,
    s_timeexpense_id           bigint,
    c_bpartnersr_id            bigint,
    isedisubmitted             char          default 'N'::bpchar,
    isfixeddate                char          default 'N'::bpchar,
    duedate                    timestamp(0),
    sourcedoc_id               bigint,
    grandtotal_rma             numeric,
    ismanual                   char,
    c_so_contract_id           bigint,
    ispenalty                  char          default 'N'::bpchar,
    cashdiscount               numeric,
    c_allocationline_id        bigint,
    iscashdiscount             char          default 'N'::bpchar,
    deleteposting              char,
    datereport                 timestamp(0),
    c_deliveryterms_id         bigint,
    m_freightcategory_id       bigint,
    transactiontypea           varchar(10),
    transactiontypea_id        bigint,
    transactiontypeb_id        bigint,
    so_meanofconveyance_id     bigint,
    meanofconveyancename       varchar(60),
    mocsalesrepname            varchar(60),
    mocsalesrepdescription     varchar(255),
    c_paymentmode_id           bigint,
    trackingno                 varchar(60),
    datereceived               timestamp(0),
    withholdingtotal           numeric       default (0)::numeric      not null,
    totalopenamt               numeric       default (0)::numeric      not null,
    ismandatorypayschedule     char          default 'N'::bpchar       not null,
    generatepayschedule        char,
    iban                       varchar(60),
    isediexported              char          default 'N'::bpchar,
    exporttoedi                char,
    salesrep_id                bigint,
    description                varchar(255),
    receiptno                  bigint        default (1)::bigint,
    isreceipt                  char          default 'N'::bpchar,
    documentnote               varchar(2000) default NULL::character varying,
    amef                       bigint        default (1)::bigint,
    findscannedfile            char,
    url                        varchar(255),
    printfiscalreceipt         char          default 'N'::bpchar       not null,
    c_voidreason_id            bigint,
    paidamt                    numeric       default (0)::numeric      not null,
    sendtowarehouse            char,
    exporttodocprocess         char,
    docprocessexportstatus     char          default 'N'::bpchar,
    isvalidatetaxid            char          default 'N'::bpchar,
    validvatpayer              char(3)       default 'ITI'::bpchar     not null,
    datestartsplittva          timestamp(0),
    issplittax                 char          default 'N'::bpchar,
    rds_tipfactura             char          default 'S'::bpchar       not null,
    c_projecttask_id           bigint,
    isdistributedonprojects    char          default 'N'::bpchar,
    c_bankaccount_id           bigint,
    isdiscountoffinvoice       char          default 'N'::bpchar
);