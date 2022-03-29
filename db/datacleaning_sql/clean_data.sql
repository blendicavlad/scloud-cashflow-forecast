INSERT INTO c_invoice_cleaned SELECT c_invoice_id,ad_client_id,ad_org_id,
                                 docstatus,dateinvoiced,
                                 c_bpartner_id,c_bpartner_location_id,c_currency_id,
                                 paymentrule,c_paymentterm_id,grandtotal,
                                 c_payment_id,c_cashline_id,ispayschedulevalid,
                                 isindispute,isreturntrx,duedate,
                                 totalopenamt,paidamt
FROM c_invoice WHERE c_invoice.issotrx = 'Y' and c_invoice.docstatus = 'CO' and c_invoice.isreturntrx = 'N' AND  c_invoice.grandtotal > 0 and c_invoice.paidamt >=0;

UPDATE c_invoice_cleaned SET c_payment_id = 999999 WHERE c_payment_id is NULL;
UPDATE c_invoice_cleaned SET c_cashline_id = 999999 WHERE c_cashline_id is NULL;
ALTER TABLE c_invoice_cleaned
    drop column  docstatus,
    drop column  isreturntrx,
    drop column  ispayschedulevalid,
    drop column  isindispute;



INSERT INTO c_payment_cleaned SELECT c_payment_id,ad_client_id,ad_org_id,datetrx,isreceipt,c_doctype_id,c_bpartner_id ,c_invoice_id,tendertype,c_currency_id,payamt,discountamt,writeoffamt,docstatus,isallocated,isprepayment,duedate,allocatedamt FROM c_payment WHERE c_payment.isreceipt = 'Y' and c_payment.docstatus = 'CO';
UPDATE c_payment_cleaned SET c_invoice_id = 999999 WHERE c_invoice_id is NULL;
UPDATE c_payment_cleaned SET c_bpartner_id = 999999 WHERE c_bpartner_id is NULL;
ALTER TABLE c_payment_cleaned DROP COLUMN isreceipt,
    DROP COLUMN  docstatus,
    DROP COLUMN discountamt,
    DROP COLUMN writeoffamt,
    DROP COLUMN isprepayment;




INSERT INTO c_allocationline_cleaned SELECT c_allocationline_id,ad_client_id,ad_org_id,c_invoice_id,c_bpartner_id,c_payment_id,c_cashline_id,amount,discountamt,writeoffamt,c_invoicepayschedule_id FROM c_allocationline WHERE c_allocationline.amount > 0;

UPDATE c_allocationline_cleaned SET c_invoice_id = 0 WHERE c_invoice_id is NULL;
UPDATE c_allocationline_cleaned SET c_bpartner_id = 0 WHERE c_bpartner_id is NULL;
UPDATE c_allocationline_cleaned SET c_payment_id = 0 WHERE c_payment_id is NULL;
UPDATE c_allocationline_cleaned SET c_cashline_id = 0 WHERE c_cashline_id is NULL;

UPDATE c_allocationline_cleaned SET amount = amount  + discountamt + writeoffamt;

ALTER TABLE c_allocationline_cleaned DROP COLUMN writeoffamt,
    DROP COLUMN discountamt,
    DROP COLUMN c_invoicepayschedule_id,
    DROP COLUMN c_allocationline_id;




INSERT INTO c_paymentterm_cleaned SELECT c_paymentterm_id,ad_client_id,ad_org_id,name,netdays,gracedays,value FROM c_paymentterm;

ALTER TABLE c_paymentterm_cleaned DROP COLUMN ad_client_id,
   DROP COLUMN ad_org_id,
   DROP COLUMN name,
   DROP COLUMN gracedays,
   DROP COLUMN value;

SELECT * FROM c_invoice_cleaned
LEFT OUTER JOIN c_paymentterm_cleaned
ON c_invoice_cleaned.c_paymentterm_id = c_paymentterm_cleaned.c_paymentterm_id;



CREATE TABLE invoice_terms AS
    (
        SELECT * FROM c_invoice_cleaned
        left outer join  c_paymentterm_cleaned
        using (c_paymentterm_id)
    );



UPDATE invoice_terms SET duedate = dateinvoiced + netdays*interval '1 day'
WHERE duedate IS NULL;

DELETE FROM invoice_terms
WHERE dateinvoiced > duedate;


ALTER TABLE invoice_terms DROP COLUMN c_paymentterm_id,
DROP COLUMN netdays;

CREATE TABLE invoice_terms_payment AS
    (
        SELECT * FROM invoice_terms
        LEFT OUTER JOIN (
                    SELECT c_payment_id as c_payment_id_paym,datetrx,c_doctype_id,
                           c_invoice_id,tendertype,
                           payamt,isallocated,allocatedamt
                    FROM c_payment_cleaned
            ) cpn
        USING(c_invoice_id)
    );




CREATE TABLE fully_paid AS
    (
        SELECT * FROM invoice_terms_payment
        WHERE paidamt = allocatedamt AND paidamt = grandtotal AND datetrx >= dateinvoiced
    );


DELETE FROM invoice_terms_payment WHERE c_invoice_id in (SELECT c_invoice_id FROM fully_paid);



--DELETE FROM fully_paid WHERE datetrx < dateinvoiced;

CREATE TABLE partially_paid AS
    (
        SELECT *, grandtotal- paidamt AS balance FROM invoice_terms_payment
        WHERE  paidamt = allocatedamt AND datetrx >= dateinvoiced
    );

DELETE FROM invoice_terms_payment WHERE c_invoice_id in (SELECT c_invoice_id FROM partially_paid);

CREATE TABLE unpaid_balance AS
    (
        SELECT * FROM partially_paid
    );

-- unpaid_balances()
DELETE FROM unpaid_balance WHERE c_invoice_id in (SELECT c_invoice_id FROM unpaid_balance WHERE balance < 0 );

UPDATE unpaid_balance SET grandtotal = balance,
                          totalopenamt = balance,
                          paidamt = 0,
                          allocatedamt = NULL,
                          c_payment_id_paym = NULL,
                          datetrx = NULL,
                          c_doctype_id = NULL,
                          tendertype = NULL,
                          payamt = NULL,
                          isallocated = NULL;
ALTER TABLE unpaid_balance DROP COLUMN balance;
-- end

UPDATE partially_paid SET allocatedamt = CASE
                                         WHEN balance < 0 THEN allocatedamt + balance
                                         ELSE allocatedamt
                                         END,
                        grandtotal = allocatedamt,
                        totalopenamt = allocatedamt,
                        paidamt = allocatedamt,
                        payamt = allocatedamt;

ALTER TABLE partially_paid DROP COLUMN balance;

CREATE TABLE partially_paid_2 AS
    (
        SELECT * FROM invoice_terms_payment
        WHERE  datetrx is NOT NULL
    );

DELETE FROM invoice_terms_payment WHERE c_invoice_id in (SELECT c_invoice_id FROM partially_paid_2);

CREATE TABLE partially_paid_3 AS
    (
        SELECT * FROM partially_paid_2
        WHERE  grandtotal = paidamt
    );
UPDATE partially_paid_2 SET grandtotal = allocatedamt,
                            totalopenamt = allocatedamt,
                            paidamt = allocatedamt;

CREATE TABLE paid_multiple_payments AS
    (
        SELECT * FROM invoice_terms_payment WHERE datetrx IS NOT NULL
    );

DELETE FROM invoice_terms_payment WHERE c_invoice_id in (SELECT c_invoice_id FROM paid_multiple_payments);

CREATE TABLE fully_paid_multiple_payments AS
    (
        SELECT * FROM paid_multiple_payments WHERE grandtotal = paidamt
    );

UPDATE paid_multiple_payments SET grandtotal = allocatedamt,
                                  totalopenamt = allocatedamt,
                                  paidamt = allocatedamt ;

CREATE TABLE unpaid_invocies AS
    (
        SELECT * FROM invoice_terms_payment WHERE paidamt = 0
    );

DELETE FROM invoice_terms_payment WHERE c_invoice_id in (SELECT c_invoice_id FROM unpaid_invocies);




CREATE TABLE invoice_terms_payment_allocation AS
    (
        SELECT * FROM invoice_terms_payment
        LEFT OUTER JOIN
            (
                SELECT  c_invoice_id,c_cashline_id as c_cashline_id_y, c_payment_id as c_payment_id_y,
                       amount
                FROM c_allocationline_cleaned
            ) can
        USING(c_invoice_id)
    );

CREATE TABLE paid_nodata AS
    (
        SELECT * FROM invoice_terms_payment_allocation WHERE amount IS NULL
    );

DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM paid_nodata);

CREATE TABLE fully_paid_2 AS
    (
        SELECT * FROM invoice_terms_payment_allocation WHERE paidamt = amount and paidamt = grandtotal
    );

DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM fully_paid_2);

CREATE TABLE cash_paid AS
    (
        SELECT * FROM invoice_terms_payment_allocation WHERE c_cashline_id_y != 0  and c_payment_id_y =0
    );

DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM cash_paid);

UPDATE  cash_paid SET payamt = amount,
                      allocatedamt = amount,
                      c_cashline_id = c_cashline_id_y,
                      datetrx = dateinvoiced;

ALTER TABLE cash_paid DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;


UPDATE  fully_paid_2 SET c_payment_id_paym = c_payment_id,
                         allocatedamt = amount,
                         c_payment_id = c_payment_id_y;

ALTER TABLE fully_paid_2 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;


CREATE TABLE paid1 AS
    (
        SELECT * FROM invoice_terms_payment_allocation WHERE paidamt = amount  and c_payment_id_y !=0
    );

DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM paid1);


ALTER TABLE paid1 ADD COLUMN balance numeric;
UPDATE  paid1 SET balance = grandtotal - paidamt;

CREATE TABLE unpaid_balance2 AS
    (
        SELECT * FROM paid1
    );

--unpaid balance2
UPDATE unpaid_balance2 SET grandtotal = balance,
                          totalopenamt = balance,
                          paidamt = 0,
                          allocatedamt = NULL,
                          c_payment_id_paym = NULL,
                          datetrx = NULL,
                          c_doctype_id = NULL,
                          tendertype = NULL,
                          payamt = NULL,
                          isallocated = NULL;
ALTER TABLE unpaid_balance2 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y,DROP COLUMN balance, DROP COLUMN amount;

--end

UPDATE paid1 SET amount = CASE
                          WHEN balance < 0 THEN amount + balance
                          ELSE amount
                          END,
                        grandtotal = amount,
                        paidamt = amount,
                        totalopenamt = amount,
                        allocatedamt = amount,
                        c_payment_id_paym = c_payment_id,
                        c_payment_id = c_payment_id_y;
ALTER TABLE paid1 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN balance, DROP COLUMN amount;

CREATE TABLE fully_paid_3 AS
    (
        SELECT * FROM invoice_terms_payment_allocation WHERE paidamt = grandtotal
    );

DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM fully_paid_3);

CREATE TABLE cash_paid2 AS
    (
        SELECT * FROM fully_paid_3 WHERE c_cashline_id_y !=0
    );

DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM cash_paid2);

UPDATE cash_paid2 SET payamt = amount,
                      allocatedamt = amount,
                      grandtotal = amount,
                      totalopenamt = amount,
                      c_cashline_id = c_cashline_id_y,
                      datetrx = dateinvoiced;
ALTER TABLE cash_paid2 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;

UPDATE fully_paid_3 SET grandtotal = amount,
                      totalopenamt = amount,
                      paidamt = amount,
                      allocatedamt = amount,
                      c_payment_id_paym = c_payment_id,
                      c_payment_id = c_payment_id_y;
ALTER TABLE fully_paid_3 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;

-- Eliminam facturile care apar platite dar nu au nicio referinta a platii.
DELETE FROM invoice_terms_payment_allocation
WHERE c_invoice_id IN (SELECT c_invoice_id FROM invoice_terms_payment_allocation WHERE c_payment_id_y = 0 AND c_cashline_id_y = 0);

CREATE TABLE cash_paid3 AS
    (
        SELECT * FROM invoice_terms_payment_allocation WHERE c_cashline_id_y !=0
    );
DELETE FROM invoice_terms_payment_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM cash_paid3);

UPDATE cash_paid3 SET grandtotal = amount,
                      totalopenamt = amount,
                      paidamt = amount,
                      payamt =amount,
                      allocatedamt = amount,
                      c_cashline_id = c_cashline_id_y,
                      datetrx = dateinvoiced;
ALTER TABLE cash_paid3 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;


UPDATE invoice_terms_payment_allocation SET grandtotal = amount,
                      totalopenamt = amount,
                      paidamt = amount,
                      allocatedamt = amount,
                      c_payment_id_paym = c_payment_id,
                      c_payment_id = c_payment_id_y;
ALTER TABLE invoice_terms_payment_allocation DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;

CREATE TABLE partially_paid_3_allocation AS
    (
        SELECT * FROM partially_paid_3
        LEFT OUTER JOIN
            (
                SELECT  c_invoice_id,c_cashline_id as c_cashline_id_y, c_payment_id as c_payment_id_y,
                       amount
                FROM c_allocationline_cleaned
            ) can
        USING(c_invoice_id)
    );

UPDATE partially_paid_3_allocation SET grandtotal = amount,
                      totalopenamt = amount,
                      paidamt = amount,
                      allocatedamt = amount;

CREATE TABLE cash_paid4 AS
    (
        SELECT * FROM partially_paid_3_allocation WHERE c_cashline_id_y !=0
    );

DELETE FROM partially_paid_3_allocation WHERE c_invoice_id IN (SELECT c_invoice_id FROM cash_paid4);

UPDATE cash_paid4 SET payamt = amount,
                      c_cashline_id = c_cashline_id_y,
                      datetrx = dateinvoiced,
                      c_payment_id_paym = NULL,
                      c_doctype_id = NULL,
                      tendertype = NULL,
                      isallocated = NULL;

ALTER TABLE cash_paid4 DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;

ALTER TABLE partially_paid_3_allocation DROP COLUMN c_cashline_id_y, DROP COLUMN c_payment_id_y, DROP COLUMN amount;

CREATE TABLE fully_paid_2_payment AS
    (
        SELECT * FROM fully_paid_2
         LEFT OUTER JOIN (
                    SELECT c_payment_id as c_p_id ,datetrx AS datetrx_y
                    FROM c_payment_cleaned
            ) cpn
        ON cpn.c_p_id = fully_paid_2.c_payment_id
        WHERE datetrx_y IS NOT NULL
    );

DELETE FROM fully_paid_2_payment WHERE datetrx_y < dateinvoiced;

ALTER TABLE fully_paid_2_payment DROP COLUMN datetrx_y, DROP COLUMN c_p_id;

CREATE TABLE paid1_payment AS
    (
        SELECT * FROM fully_paid_2
         LEFT OUTER JOIN (
                    SELECT c_payment_id as c_p_id,datetrx AS datetrx_y
                    FROM c_payment_cleaned
            ) cpn
        ON cpn.c_p_id = fully_paid_2.c_payment_id
    );
DELETE FROM paid1_payment WHERE datetrx_y < dateinvoiced;
ALTER TABLE paid1_payment DROP COLUMN datetrx_y,DROP COLUMN c_p_id;

CREATE TABLE fully_paid_3_payment AS
    (
        SELECT * FROM fully_paid_3
         LEFT OUTER JOIN (
                    SELECT c_payment_id as c_p_id ,datetrx AS datetrx_y
                    FROM c_payment_cleaned
            ) cpn
        ON cpn.c_p_id = fully_paid_3.c_payment_id
        WHERE datetrx_y IS NOT NULL
    );
DELETE FROM fully_paid_3_payment WHERE datetrx_y < dateinvoiced;
ALTER TABLE fully_paid_3_payment DROP COLUMN datetrx_y, DROP COLUMN c_p_id;

CREATE TABLE invoice_terms_payment_allocation_payment AS
    (
        SELECT * FROM invoice_terms_payment_allocation
         LEFT OUTER JOIN (
                    SELECT c_payment_id as c_p_id,datetrx AS datetrx_y
                    FROM c_payment_cleaned
            ) cpn
        ON cpn.c_p_id = invoice_terms_payment_allocation.c_payment_id
        WHERE datetrx_y IS NOT NULL
    );

DELETE FROM invoice_terms_payment_allocation_payment WHERE datetrx_y < dateinvoiced;
ALTER TABLE invoice_terms_payment_allocation_payment DROP COLUMN datetrx_y, DROP COLUMN c_p_id;

CREATE TABLE allmerged AS
    (
        SELECT * FROM fully_paid
        UNION ALL
        SELECT * FROM partially_paid
        UNION ALL
        SELECT * FROM unpaid_balance
        UNION ALL
        SELECT * FROM partially_paid_2
        UNION ALL
        SELECT * FROM cash_paid
        UNION ALL
        SELECT * FROM partially_paid_3_allocation--p1
        UNION ALL
        SELECT * FROM unpaid_invocies
        UNION ALL
        SELECT * FROM cash_paid4
        UNION ALL
        SELECT * FROM fully_paid_2_payment --fp2 --astaa!!
        UNION ALL
        SELECT *  FROM unpaid_balance2
        UNION ALL
        SELECT * FROM paid1_payment --asta
        UNION ALL
        SELECT * FROM cash_paid2
        UNION ALL
        SELECT * FROM cash_paid3
        UNION ALL
        SELECT * FROM fully_paid_3_payment --p3 asta
        UNION ALL
        SELECT * FROM invoice_terms_payment_allocation_payment --p4 asta
    );


