/*
INCERCARE DE CREARE DATE_ANTRENAMENT NEREUSITA...
*/

CREATE TABLE date_antrenament AS
    (
        SELECT * from allmerged WHERE grandtotal > 0
    );

DELETE FROM date_antrenament WHERE datetrx < dateinvoiced;


ALTER TABLE date_antrenament ADD COLUMN days_late numeric, ADD COLUMN days_to_settle numeric, ADD COLUMN late numeric,ADD COLUMN paid numeric;


SELECT MAX(datetrx) FROM date_antrenament;
SELECT MIN(datetrx) FROM date_antrenament;

UPDATE date_antrenament SET datetrx = '2021-01-28' WHERE datetrx is NULL;

UPDATE  date_antrenament SET days_late = CASE
                                         WHEN EXTRACT(DAY FROM datetrx - duedate) < 0 THEN 0
                                         ELSE EXTRACT(DAY FROM datetrx - duedate)
                                         END,
                             days_to_settle = EXTRACT(DAY FROM datetrx - dateinvoiced);

UPDATE  date_antrenament SET late = CASE
                                    WHEN days_late > 0 then 1
                                    ELSE 0
                                    END,
                             paid = CASE
                                    WHEN grandtotal - paidamt <= 0 THEN 1
                                    ELSE 0
                                    END;
ALTER TABLE date_antrenament
    DROP COLUMN c_payment_id,
    DROP COLUMN c_cashline_id,
    DROP COLUMN c_payment_id_paym,
    DROP COLUMN isallocated,
    DROP COLUMN c_doctype_id,
    DROP COLUMN payamt;


CREATE TABLE date_antrenament_1 AS
    (
        SELECT * FROM date_antrenament WHERE datetrx IS NOT NULL
    );
ALTER TABLE date_antrenament_1 ADD COLUMN days_late_bucket varchar;

UPDATE date_antrenament_1 SET days_late_bucket = CASE
                                                 WHEN days_late = 0 THEN '0'
                                                 WHEN days_late < 30 and days_late > 0 THEN 'up to 30 days'
                                                 WHEN days_late < 60 and days_late > 30 THEN 'up to 60 days'
                                                 WHEN days_late < 90 and days_late > 60 THEN 'up to 90 days'
                                                 ELSE '90+ days'
                                                 END;






DISCARD TEMP

CREATE TEMP TABLE days_late AS
    (
        SELECT c_bpartner_id,date_antrenament.days_late as days_late from date_antrenament
    );


CREATE TEMP TABLE total_facturi AS
    (
        SELECT c_bpartner_id,count(c_invoice_id) as total_facturi from date_antrenament
        GROUP BY c_bpartner_id
    );

CREATE TEMP TABLE nr_facturi_inchise AS
    (
        SELECT c_bpartner_id,count(paid) as nr_facturi_inchise from date_antrenament
        WHERE paid = 1
        GROUP BY c_bpartner_id
    );


CREATE TEMP TABLE fac_inchisa_late AS
    (
        SELECT c_bpartner_id, CASE WHEN paid = 1 and late = 1 then 1
            ELSE 0 END fac_inchisa_late FROM date_antrenament
    );

CREATE TEMP TABLE nr_facturi_inchise_late AS
    (
        SELECT c_bpartner_id,sum(fac_inchisa_late.fac_inchisa_late) as nr_facturi_inchise_late from fac_inchisa_late
        GROUP BY c_bpartner_id
    );

CREATE TEMP TABLE procent_platite_late AS
    (
        SELECT c_bpartner_id, nfil.nr_facturi_inchise_late * 1.0/ nr_facturi_inchise   as procent_platite_late from nr_facturi_inchise
        inner JOIN nr_facturi_inchise_late nfil using(c_bpartner_id)
        WHERE nfil.nr_facturi_inchise_late !=0
    );



CREATE TEMP TABLE total_platit AS
    (
        SELECT c_bpartner_id,SUM(allocatedamt) as total_platit FROM date_antrenament GROUP BY c_bpartner_id
    );

CREATE TEMP TABLE suma_platita_late AS
    (
        SELECT c_bpartner_id,(allocatedamt * late) as suma_platita_late  FROM date_antrenament
    );

CREATE TEMP TABLE total_platit_late AS
    (
        SELECT c_bpartner_id,SUM(suma_platita_late) as total_platit_late FROM suma_platita_late
        GROUP BY c_bpartner_id
    );


CREATE TEMP TABLE procent_val_platite_late AS
    (
        SELECT c_bpartner_id, CAST(tpl.total_platit_late as FLOAT)/ CAST(total_platit AS FLOAT)as procent_val_platite_late from total_platit
        inner JOIN total_platit_late tpl using(c_bpartner_id)
    );

CREATE TEMP TABLE days_late_fac_inchisa_late AS
    (
        SELECT *,days_late * fac_inchisa_late.fac_inchisa_late AS days_late_fac_inchisa_late FROM date_antrenament
        INNER JOIN fac_inchisa_late USING(c_bpartner_id)
    );




CREATE TEMP TABLE avg_days_paid_late AS
    (
        SELECT c_bpartner_id, SUM(days_late_fac_inchisa_late) / (SELECT SUM( days_late_fac_inchisa_late)  FROM days_late_fac_inchisa_late WHERE  days_late_fac_inchisa_late !=0) AS avg_days_paid_late
        FROM date_antrenament
        INNER JOIN days_late_fac_inchisa_late
        using(c_bpartner_id)
        GROUP BY c_bpartner_id
    );
UPDATE avg_days_paid_late SET avg_days_paid_late = 0 WHERE avg_days_paid_late.avg_days_paid_late is NULL;

CREATE TEMP TABLE nr_facturi_unpaid AS
    (
        SELECT c_bpartner_id, (total_facturi - nr_facturi_inchise) AS nr_facturi_unpaid  FROM total_facturi INNER JOIN nr_facturi_inchise using(c_bpartner_id)
    );

CREATE TEMP TABLE factura_late_unpaid AS
    (
        SELECT c_bpartner_id, late*(1-paid) AS factura_late_unpaid FROM date_antrenament
    );

CREATE TEMP TABLE nr_facturi_late_unpaid AS
    (
        SELECT c_bpartner_id, SUM(factura_late_unpaid) AS nr_facturi_late_unpaid FROM factura_late_unpaid
        GROUP BY c_bpartner_id
    );

CREATE TEMP TABLE prct_facturi_late_unpaid AS
    (
        SELECT c_bpartner_id, (nr_facturi_late_unpaid / nr_facturi_unpaid) AS prct_facturi_late_unpaid FROM nr_facturi_late_unpaid
        INNER JOIN nr_facturi_unpaid
        USING (c_bpartner_id)
        WHERE nr_facturi_unpaid != 0

    );
UPDATE prct_facturi_late_unpaid SET prct_facturi_late_unpaid = 0 WHERE prct_facturi_late_unpaid is NULL;

CREATE TEMP TABLE suma_unpaid AS
(
        SELECT c_bpartner_id, (grandtotal - paidamt) AS suma_unpaid  FROM date_antrenament
    );
CREATE TEMP TABLE suma_facturi_unpaid AS
(
        SELECT c_bpartner_id, sum(suma_unpaid) AS suma_facturi_unpaid FROM suma_unpaid
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE suma_unpaid_late AS
(
        SELECT c_bpartner_id, (suma_unpaid * late) AS suma_unpaid_late  FROM suma_unpaid
        INNER JOIN date_antrenament
        using (c_bpartner_id)
);

CREATE TEMP TABLE suma_facturi_unpaid_late AS
(
        SELECT c_bpartner_id, sum(suma_unpaid_late) AS suma_facturi_unpaid_late  FROM suma_unpaid_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE prc_suma_facturi_unpaid_late AS
(
        SELECT c_bpartner_id, (suma_facturi_unpaid_late / suma_facturi_unpaid) AS prc_suma_facturi_unpaid_late FROM suma_facturi_unpaid
        INNER JOIN suma_facturi_unpaid_late
        USING (c_bpartner_id)
        WHERE suma_facturi_unpaid != 0
);


CREATE TEMP TABLE max_platit AS
(
        SELECT c_bpartner_id, MAX(allocatedamt) AS max_platit FROM date_antrenament
        GROUP BY c_bpartner_id
);
CREATE TEMP TABLE min_platit AS
(
        SELECT c_bpartner_id, min(allocatedamt) AS min_platit FROM date_antrenament
        GROUP BY c_bpartner_id
);
CREATE TEMP TABLE avg_platit AS
(
        SELECT c_bpartner_id, avg(allocatedamt) AS avg_platit FROM date_antrenament
        GROUP BY c_bpartner_id
);
CREATE TEMP TABLE std_platit AS
(
        SELECT c_bpartner_id, stddev(allocatedamt) AS std_platit FROM date_antrenament
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE max_platit_late AS
(
        SELECT c_bpartner_id, MAX(suma_platita_late) AS max_platit_late FROM suma_platita_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE min_platit_late AS
(
        SELECT c_bpartner_id, min(suma_platita_late) AS min_platit_late FROM suma_platita_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE avg_platit_late AS
(
        SELECT c_bpartner_id, avg(suma_platita_late) AS avg_platit_late FROM suma_platita_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE std_platit_late AS
(
        SELECT c_bpartner_id, stddev(suma_platita_late) AS std_platit_late FROM suma_platita_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE max_unpaid AS
(
        SELECT c_bpartner_id, MAX(suma_unpaid) AS max_unpaid FROM suma_unpaid
        GROUP BY c_bpartner_id
);
CREATE TEMP TABLE min_unpaid AS
(
        SELECT c_bpartner_id, min(suma_unpaid) AS min_unpaid FROM suma_unpaid
        GROUP BY c_bpartner_id
);
CREATE TEMP TABLE avg_unpaid AS
(
        SELECT c_bpartner_id, avg(suma_unpaid) AS avg_unpaid FROM suma_unpaid
        GROUP BY c_bpartner_id
);
CREATE TEMP TABLE std_unpaid AS
(
        SELECT c_bpartner_id, stddev(suma_unpaid) AS std_unpaid FROM suma_unpaid
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE max_unpaid_late AS
(
        SELECT c_bpartner_id, MAX(suma_unpaid_late) AS max_unpaid_late FROM suma_unpaid_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE min_unpaid_late AS
(
        SELECT c_bpartner_id, min(suma_unpaid_late) AS min_unpaid_late FROM suma_unpaid_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE avg_unpaid_late AS
(
        SELECT c_bpartner_id, avg(suma_unpaid_late) AS avg_unpaid_late FROM suma_unpaid_late
        GROUP BY c_bpartner_id
);

CREATE TEMP TABLE std_unpaid_late AS
(
        SELECT c_bpartner_id, stddev(suma_unpaid_late) AS std_unpaid_late FROM suma_unpaid_late
        GROUP BY c_bpartner_id
);



CREATE TABLE date_antrenament_finale AS
(
    SELECT * from date_antrenament
    INNER JOIN total_facturi using(c_bpartner_id)
    INNER JOIN nr_facturi_inchise using (c_bpartner_id)
    INNER JOIN nr_facturi_inchise_late using(c_bpartner_id)
    INNER JOIN procent_platite_late using(c_bpartner_id)
    INNER JOIN total_platit using(c_bpartner_id)
    INNER JOIN total_platit_late using(c_bpartner_id)
    INNER JOIN procent_val_platite_late using(c_bpartner_id)
    INNER JOIN days_late_fac_inchisa_late using(c_bpartner_id)
    INNER JOIN avg_days_paid_late using(c_bpartner_id)
    INNER JOIN nr_facturi_unpaid using(c_bpartner_id)
    INNER JOIN factura_late_unpaid using(c_bpartner_id)
    INNER JOIN nr_facturi_late_unpaid using(c_bpartner_id)
    INNER JOIN prct_facturi_late_unpaid using(c_bpartner_id)
    INNER JOIN suma_unpaid using(c_bpartner_id)
    INNER JOIN suma_facturi_unpaid using(c_bpartner_id)
    INNER JOIN suma_unpaid_late using(c_bpartner_id)
    INNER JOIN suma_facturi_unpaid_late using(c_bpartner_id)
    INNER JOIN prc_suma_facturi_unpaid_late using(c_bpartner_id)


   INNER JOIN max_platit using(c_bpartner_id)
    INNER JOIN min_platit using(c_bpartner_id)
    INNER JOIN avg_platit using(c_bpartner_id)
    INNER JOIN std_platit using(c_bpartner_id)

    INNER JOIN max_platit_late using(c_bpartner_id)
    INNER JOIN min_platit_late using(c_bpartner_id)
    INNER JOIN avg_platit_late using(c_bpartner_id)
    INNER JOIN std_platit_late using(c_bpartner_id)

   INNER JOIN max_unpaid using(c_bpartner_id)
    INNER JOIN min_unpaid using(c_bpartner_id)
    INNER JOIN avg_unpaid using(c_bpartner_id)
    INNER JOIN std_unpaid using(c_bpartner_id)

       INNER JOIN max_unpaid_late using(c_bpartner_id)
    INNER JOIN min_unpaid_late using(c_bpartner_id)
    INNER JOIN avg_unpaid_late using(c_bpartner_id)
    INNER JOIN std_unpaid_late using(c_bpartner_id)
)


