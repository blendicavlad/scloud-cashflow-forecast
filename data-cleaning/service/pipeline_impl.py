import logging
import datetime
import time
import numpy as np
import pandas as pd
from pandas import DataFrame
from sentry_sdk import start_span

logger = logging.getLogger('appLog')
pd.options.mode.chained_assignment = None


class AbstractPipeline:
    """ Generic representation of a DataCleaning AbstractPipeline"""
    source_table_name = None
    dest_table_name = None

    def __init__(self, ad_client_id):
        self.__COLUMNS_TO_USE = []
        self.__FILTERS = None
        self.ad_client_id = ad_client_id

    @property
    def columns_to_use(self):
        return self.__COLUMNS_TO_USE

    @property
    def filters(self):
        if self.__FILTERS is not None:
            return ' AND '.join(self.__FILTERS)
        else:
            return None

    def clean_df(self, df: DataFrame) -> DataFrame:
        raise 'Not implemented'


class InvoiceCleaningPipeline(AbstractPipeline):
    source_table_name = 'c_invoice'
    dest_table_name = 'c_invoice_cleaned'

    def __init__(self, ad_client_id):
        super().__init__(ad_client_id)
        self.__COLUMNS_TO_USE = [
            "c_invoice_id",
            "ad_client_id",
            "ad_org_id",
            "dateinvoiced",
            "c_bpartner_id",
            "c_bpartner_location_id",
            "c_currency_id",
            "paymentrule",
            "c_paymentterm_id",
            "grandtotal",
            "c_payment_id",
            "c_cashline_id",
            "duedate",
            "totalopenamt",
            "paidamt",
            "issotrx",
            "docstatus",
            "isreturntrx"
        ]
        self.__FILTERS = ["issotrx = 'Y'"]
        self.__FILTERS.append("ad_client_id = " + str(self.ad_client_id))

    @property
    def columns_to_use(self):
        return self.__COLUMNS_TO_USE

    @property
    def filters(self):
        if self.__FILTERS is not None:
            return ' AND '.join(self.__FILTERS)
        else:
            return None

    def clean_df(self, df: DataFrame) -> DataFrame:
        if df.empty:
            return df
        df = df[df['docstatus'] == "CO"]
        df = df[df['isreturntrx'] == "N"]
        df.drop(columns=['issotrx', 'docstatus', 'isreturntrx'], inplace=True)
        df.fillna({'c_payment_id': 0, 'c_cashline_id': 0}, inplace=True)
        id_columns = list(filter(lambda column: column.endswith('id'), list(df)))
        df[id_columns] = df[id_columns].astype(np.int64)
        return df


class AllocationCleaningPipeline(AbstractPipeline):
    source_table_name = 'c_allocationline'
    dest_table_name = 'c_allocationline_cleaned'

    def __init__(self, ad_client_id):
        super().__init__(ad_client_id)
        self.__COLUMNS_TO_USE = [
            "c_allocationline_id",
            "ad_client_id",
            "ad_org_id",
            "c_invoice_id",
            "c_bpartner_id",
            "c_payment_id",
            "c_cashline_id",
            "amount"
        ]
        self.__FILTERS = []
        self.__FILTERS.append("ad_client_id = " + str(self.ad_client_id))

    @property
    def columns_to_use(self):
        return self.__COLUMNS_TO_USE

    @property
    def filters(self):
        if self.__FILTERS is not None:
            return ' AND '.join(self.__FILTERS)
        else:
            return None

    def clean_df(self, df: DataFrame) -> DataFrame:
        if df.empty:
            return df
        # Pentru facturile de clienti, amount trebuie sa fie > 0 (sumele cu - reprezinta plati catre furnizori)

        # Fill NULL Values for ID's
        df = df[df["amount"] > 0]
        df.fillna(
            {x: 0 for x in [column for column in self.__COLUMNS_TO_USE if column.endswith('id')]}
            , inplace=True)

        df[['c_invoice_id', 'c_bpartner_id', 'c_payment_id', 'c_cashline_id', 'c_allocationline_id']] = df[
            ['c_invoice_id', 'c_bpartner_id', 'c_payment_id', 'c_cashline_id', 'c_allocationline_id']].astype(int)

        return df


class PaymentCleaningPipeline(AbstractPipeline):
    source_table_name = 'c_payment'
    dest_table_name = 'c_payment_cleaned'

    def __init__(self, ad_client_id):
        super().__init__(ad_client_id)
        self.__COLUMNS_TO_USE = [
            "c_payment_id",
            "ad_client_id",
            "ad_org_id",
            "datetrx",
            "c_doctype_id",
            "c_bpartner_id",
            "c_invoice_id",
            "tendertype",
            "c_currency_id",
            "payamt",
            "isallocated",
            "duedate",
            "allocatedamt",
            "isreceipt",
            "docstatus"
        ]
        self.__FILTERS = []
        self.__FILTERS.append("ad_client_id = " + str(self.ad_client_id))

    @property
    def columns_to_use(self):
        return self.__COLUMNS_TO_USE

    @property
    def filters(self):
        return ' AND '.join(self.__FILTERS)

    def clean_df(self, df: DataFrame) -> DataFrame:
        if df.empty:
            return df
        df = df[df['isreceipt'] == "Y"]
        df = df[df['docstatus'] == "CO"]
        df.drop(columns=['isreceipt', 'docstatus'], inplace=True)
        df.fillna({'c_invoice_id': 0, 'c_bpartner_id': 0}, inplace=True)
        df[['c_invoice_id', 'c_bpartner_id']] = df[['c_invoice_id', 'c_bpartner_id']].astype(int)
        return df


class PaymentTermCleaningPipeline(AbstractPipeline):
    source_table_name = 'c_paymentterm'
    dest_table_name = 'c_paymentterm_cleaned'

    def __init__(self, ad_client_id):
        super().__init__(ad_client_id)
        self.__COLUMNS_TO_USE = [
            "c_paymentterm_id",
            "ad_client_id",
            "netdays",
        ]
        self.__FILTERS = []
        self.__FILTERS.append("ad_client_id = " + str(self.ad_client_id))

    @property
    def columns_to_use(self):
        return self.__COLUMNS_TO_USE

    @property
    def filters(self):
        if self.__FILTERS is not None:
            return ' AND '.join(self.__FILTERS)
        else:
            return None

    def clean_df(self, df: DataFrame) -> DataFrame:
        if df.empty:
            return df
        return df


# Proceseaza data_frame-ul in chunk uri
class AggregationPipeline(AbstractPipeline):
    dest_table_name = 'cleaned_aggregated_data'

    # Target for regression
    __TARGET_REG = 'daystosettle'
    # Target for classification
    __TARGET_CLS = 'paid'

    def __init__(self, ad_client_id):
        super().__init__(ad_client_id)
        self.ad_client_id = ad_client_id

    @staticmethod
    def eliminare_facturi_avans(df):
        df['datetrx'] = pd.to_datetime(df.datetrx)
        df.drop(df[df.datetrx < df.dateinvoiced].index, inplace=True)

    @staticmethod
    def unpaid_balances(df):
        # Facturile care au o balanta negativa sunt eliminate deoarece reprezinta facturi platite integral
        df.drop(df[df.balance < 0].index, inplace=True)
        # Egalam valoarea facturii cu balanta ramasa de plata si o sa le tratam ca pe niste facturi neplatite

        df['grandtotal'] = df['balance']
        df['totalopenamt'] = df['balance']
        df['paidamt'] = 0
        df['allocatedamt'] = np.nan
        df['c_payment_id_paym'] = np.nan
        df['datetrx'] = np.nan
        df['c_doctype_id'] = np.nan
        df['tendertype'] = np.nan
        df['payamt'] = np.nan
        df['isallocated'] = np.nan
        df.drop(columns=['balance'], inplace=True)

    @staticmethod
    def rename_columns(df):
        return df.rename(columns={'ad_org_id_x': 'ad_org_id', 'c_bpartner_id_x': 'c_bpartner_id',
                                  'c_payment_id_x': 'c_payment_id', 'c_cashline_id_x': 'c_cashline_id'}, inplace=True)

    @staticmethod
    def drop_columns(df):
        return df.drop(columns=['ad_org_id_y', 'c_bpartner_id_y', 'c_payment_id_y', 'c_cashline_id_y', 'amount'],
                       inplace=True)

    @staticmethod
    def eliminare_avansuri(df):
        df['datetrx_y'] = pd.to_datetime(df.datetrx_y)
        df.drop(df[df.datetrx_y < df.dateinvoiced].index, inplace=True)

    @staticmethod
    def re_normalize_data(df):
        df['datetrx_x'] = df['datetrx_y']
        df['payamt_x'] = df['allocatedamt_x']
        df['c_doctype_id_x'] = df['c_doctype_id_y']
        df['tendertype_x'] = df['tendertype_y']
        df['isallocated_x'] = df['isallocated_y']
        df.drop(columns=['ad_org_id_y', 'datetrx_y', 'c_doctype_id_y', 'c_bpartner_id_y', 'c_invoice_id_y',
                         'tendertype_y', 'c_currency_id_y', 'payamt_y', 'isallocated_y', 'duedate_y', 'allocatedamt_y'],
                inplace=True)
        df.rename(
            columns={'c_invoice_id_x': 'c_invoice_id', 'ad_org_id_x': 'ad_org_id', 'c_bpartner_id_x': 'c_bpartner_id',
                     'c_currency_id_x': 'c_currency_id', 'duedate_x': 'duedate', 'datetrx_x': 'datetrx',
                     'c_doctype_id_x': 'c_doctype_id', 'tendertype_x': 'tendertype', 'payamt_x': 'payamt',
                     'isallocated_x': 'isallocated', 'allocatedamt_x': 'allocatedamt'}, inplace=True)

    def run(self, invoice_df, allocations_df, payments_df, terms_df) -> DataFrame:

        empty_dataframes = []
        if invoice_df.empty:
            empty_dataframes.append('invoice')
        if allocations_df.empty:
            empty_dataframes.append('allocations')
        if payments_df.empty:
            empty_dataframes.append('payments')
        # we don't need to also check for payment terms, because there we will use the same data everytime
        # todo: don't run aggregation pipeline if there is no new cleaned data since last time
        if not empty_dataframes:
            return self.generate_aggregated_cleaned_df(allocations_df, invoice_df, payments_df, terms_df)
        else:
            logger.info('Empty cleaned dataframes: ' + ','.join(empty_dataframes) + ' for client: ' + self.ad_client_id)
            return pd.DataFrame()

    def generate_aggregated_cleaned_df(self, allocations_df, invoice_df, payments_df, terms_df):
        # merge invoices cu payment terms
        # Exista foarte multe facturi care au data scadenta lipsa. Vom calcula duedate in functie de termenele de plata
        start_time = time.time()
        logger.info(f'Running merge phase')
        with start_span(op="merge_phase", description="merge_phase") as span:
            span.set_data('ad_client_id', self.ad_client_id)
            invoice_and_terms_df = pd.merge(invoice_df, terms_df, how='left', on=["c_paymentterm_id", "ad_client_id"])
            del terms_df
            # Sunt multe facturi care au data scadenta inainte de data emiterii facturii. Vom elimina aceste facturi din model
            invoice_and_terms_df = invoice_and_terms_df[
                invoice_and_terms_df.dateinvoiced <= invoice_and_terms_df.duedate]
            # Adaugam o coloana calculata de tip data pentru a completa valorile lipsa in coloana duedate
            invoice_and_terms_df['dateinvoiced'] = \
                pd.to_datetime(invoice_and_terms_df.dateinvoiced)
            invoice_and_terms_df['duedate_calculated'] = \
                invoice_and_terms_df['dateinvoiced'] + pd.to_timedelta(invoice_and_terms_df['netdays'], unit='d')
            invoice_and_terms_df['duedate_calculated'] = \
                pd.to_datetime(invoice_and_terms_df.duedate_calculated)
            invoice_and_terms_df.loc[invoice_and_terms_df['duedate'] == '\\N', 'duedate'] = \
                invoice_and_terms_df['duedate_calculated']
            invoice_and_terms_df['duedate'] = \
                pd.to_datetime(invoice_and_terms_df.duedate)
            # Nu mai avem nevoie de aceste coloane in continuare
            invoice_and_terms_df.drop(columns=['c_paymentterm_id', 'netdays', 'duedate_calculated'], inplace=True)
            # merge invoices cu payments
            # Vom extrage din tabela de plati acele plati unice pentru o factura
            invoice_terms_and_payments_df = pd.merge(invoice_and_terms_df, payments_df, how='left',
                                                     on=["c_invoice_id", "ad_client_id"])
            invoice_terms_and_payments_df = invoice_terms_and_payments_df.rename(
                columns={'ad_org_id_x': 'ad_org_id', 'c_bpartner_id_x': 'c_bpartner_id',
                         'c_currency_id_x': 'c_currency_id',
                         'c_payment_id_x': 'c_payment_id', 'duedate_x': 'duedate',
                         'c_payment_id_y': 'c_payment_id_paym'})
            invoice_terms_and_payments_df.drop(
                columns=['ad_org_id_y', 'c_bpartner_id_y', 'c_currency_id_y', 'duedate_y'],
                inplace=True)
            # Impartim setul de date t1 in mai multe subseturi
            # Facturi platite integral
            # Atunci cand paidamt == grandtotal => factura a fost platita in totalitate
            # Daca se satisface conditia de mai sus si paidamt == allocatedamt => facturi achitate integral printr-o singura plata
            fully_paid = invoice_terms_and_payments_df[
                (invoice_terms_and_payments_df.paidamt == invoice_terms_and_payments_df.allocatedamt) &
                (invoice_terms_and_payments_df.paidamt == invoice_terms_and_payments_df.grandtotal)
                ]
            # Din totalul facturilor le eliminam pe cele platite integral printr-o singura tranzactie
            invoice_terms_and_payments_df = invoice_terms_and_payments_df.drop(fully_paid.index)
            # Vom elimina din setul de date facturile platite in avans si le vom pastra doar pe cele platite dupa emiterea facturii
            self.eliminare_facturi_avans(fully_paid)
            # Facturi platite partial
            # Vom extrage in continuare facturile platite partial printr-o singura tranzactie
            partially_paid = invoice_terms_and_payments_df[
                invoice_terms_and_payments_df.paidamt == invoice_terms_and_payments_df.allocatedamt]
            # Eliminam din setul de date initial facturile extrase mai sus
            invoice_terms_and_payments_df = invoice_terms_and_payments_df.drop(partially_paid.index)
            # Eliminam facturile platite in avans
            self.eliminare_facturi_avans(partially_paid)
            # Determinam restul de plata pentru facturi
            partially_paid['balance'] = partially_paid['grandtotal'] - partially_paid['paidamt']
            # Este necesara copierea setului de date deoarece facturile sunt platite partial;
            # vom extrage soldul ramas al facturilor
            unpaid_balance = partially_paid.copy()
            self.unpaid_balances(unpaid_balance)
            # Exista facturi care au fost platite cu o suma mai mare decat cea din factura
            # Vom scadea soldul suplimentar pentru a ajunge la valoarea facturii
            if not partially_paid[['allocatedamt', 'balance']].empty:
                partially_paid['allocatedamt'] = partially_paid[['allocatedamt', 'balance']].apply(
                    lambda t: (t[0] + t[1]) if t[1] < 0 else t[0], axis=1)
                partially_paid['grandtotal'] = partially_paid['allocatedamt']
                partially_paid['totalopenamt'] = partially_paid['allocatedamt']
                partially_paid['paidamt'] = partially_paid['allocatedamt']
                partially_paid['payamt'] = partially_paid['allocatedamt']
                partially_paid.drop(columns=['balance'], inplace=True)
            # Facturi platite in mai multe transe
            # Extragem toate facturile care apar cu sume platite
            partially_paid_2 = invoice_terms_and_payments_df[invoice_terms_and_payments_df.datetrx.notnull()]
            invoice_terms_and_payments_df = invoice_terms_and_payments_df.drop(partially_paid_2.index)
            # Extragem facturile platite integral, urmand sa facem merge cu allocations pentru a extrage datele referitoare la sumele alocate
            partially_paid_3 = partially_paid_2[partially_paid_2.grandtotal == partially_paid_2.paidamt]
            partially_paid_2['grandtotal'] = partially_paid_2['allocatedamt']
            partially_paid_2['totalopenamt'] = partially_paid_2['allocatedamt']
            partially_paid_2['paidamt'] = partially_paid_2['allocatedamt']
            # Extragem facturile care apar a fiind integral neplatite
            unpaid_invoices = invoice_terms_and_payments_df[invoice_terms_and_payments_df.paidamt == 0]
            invoice_terms_and_payments_df = invoice_terms_and_payments_df.drop(unpaid_invoices.index)
            # Vom lega facturile care apar ca fiind platite/partial platite, dar in mai multe transe, cu tabela de alocari
            invoice_terms_payments_allocations_df = pd.merge(invoice_terms_and_payments_df, allocations_df, how='left',
                                                             on=["c_invoice_id", "ad_client_id"])
            # eliminam facturile care apar platite dar pentru care nu avem date
            paid_nodata = invoice_terms_payments_allocations_df[invoice_terms_payments_allocations_df.amount.isna()]
            invoice_terms_payments_allocations_df = invoice_terms_payments_allocations_df.drop(paid_nodata.index)
            # Extragem facturile platite integral
            fully_paid_2 = invoice_terms_payments_allocations_df[
                (invoice_terms_payments_allocations_df.paidamt == invoice_terms_payments_allocations_df.amount) &
                (invoice_terms_payments_allocations_df.paidamt == invoice_terms_payments_allocations_df.grandtotal)
                ]
            invoice_terms_payments_allocations_df = invoice_terms_payments_allocations_df.drop(fully_paid_2.index)
            # Din facturile platite integral vom extrage acele facturi care au fost achitate cu cash
            cash_paid = fully_paid_2[((fully_paid_2.c_cashline_id_y != 0) & (fully_paid_2.c_payment_id_y == 0))]
            fully_paid_2 = fully_paid_2.drop(cash_paid.index)
            cash_paid['payamt'] = cash_paid['amount']
            cash_paid['allocatedamt'] = cash_paid['amount']
            cash_paid['c_cashline_id_x'] = cash_paid['c_cashline_id_y']
            cash_paid['datetrx'] = cash_paid['dateinvoiced']
            self.rename_columns(cash_paid)
            self.drop_columns(cash_paid)
            # Dupa ce am eliminat facturile platite cu cash, vom procesa restul facturilor platite integral.
            # Acestea vor trebui legate din nou cu tabela de plati deoarece data tranzactiei este duplicat pentru aceeasi factura.
            fully_paid_2['c_payment_id_paym'] = fully_paid_2['c_payment_id_x']
            fully_paid_2['allocatedamt'] = fully_paid_2['amount']
            fully_paid_2['c_payment_id_x'] = fully_paid_2['c_payment_id_y']
            self.rename_columns(fully_paid_2)
            self.drop_columns(fully_paid_2)
            # Extrgem facturile platite partial.
            # Pentru cele cu o valoare platita mai mare decat valoarea facturii, vom regla sumele.
            # Pentru cele platite partial vom extrage si separa facturile, calculand soldul ramas.
            paid1 = invoice_terms_payments_allocations_df[
                (invoice_terms_payments_allocations_df.paidamt == invoice_terms_payments_allocations_df.amount) & (
                        invoice_terms_payments_allocations_df.c_payment_id_y != 0)]
            invoice_terms_payments_allocations_df = invoice_terms_payments_allocations_df.drop(paid1.index)
            paid1['balance'] = paid1['grandtotal'] - paid1['paidamt']
            unpaid_balance2 = paid1.copy()
            self.unpaid_balances(unpaid_balance2)
            self.rename_columns(unpaid_balance2)
            self.drop_columns(unpaid_balance2)
            if not paid1[['amount', 'balance']].empty:
                paid1['amount'] = paid1[['amount', 'balance']].apply(lambda t: (t[0] + t[1]) if t[1] < 0 else t[0],
                                                                     axis=1)
                paid1['grandtotal'] = paid1['amount']
                paid1['paidamt'] = paid1['amount']
                paid1['totalopenamt'] = paid1['amount']
                paid1['allocatedamt'] = paid1['amount']
            paid1['c_payment_id_paym'] = paid1['c_payment_id_x']
            paid1['c_payment_id_x'] = paid1['c_payment_id_y']
            self.rename_columns(paid1)
            paid1.drop(columns=['balance'], inplace=True)
            self.drop_columns(paid1)
            # Extragem facturile platite integral prin mai multe transe.
            fully_paid_3 = invoice_terms_payments_allocations_df[
                (invoice_terms_payments_allocations_df.paidamt == invoice_terms_payments_allocations_df.grandtotal)]
            invoice_terms_payments_allocations_df = invoice_terms_payments_allocations_df.drop(fully_paid_3.index)
            # Dintre facturile platite integral prin mai multe transe, extragem facturile platite cu cash.
            cash_paid2 = fully_paid_3[fully_paid_3.c_cashline_id_y != 0]
            fully_paid_3 = fully_paid_3.drop(cash_paid2.index)
            cash_paid2['payamt'] = cash_paid2['amount']
            cash_paid2['allocatedamt'] = cash_paid2['amount']
            cash_paid2['grandtotal'] = cash_paid2['amount']
            cash_paid2['totalopenamt'] = cash_paid2['amount']
            cash_paid2['c_cashline_id_x'] = cash_paid2['c_cashline_id_y']
            cash_paid2['datetrx'] = cash_paid2['dateinvoiced']
            self.rename_columns(cash_paid2)
            self.drop_columns(cash_paid2)
            # Dupa ce am eliminat din setul de date facturile platite cu cash, vom prelucra datele.
            # Acest set de date va trebui sa fie legat din nou cu tabela de plati pentru a extrage datele tranzactiei.
            fully_paid_3['grandtotal'] = fully_paid_3['amount']
            fully_paid_3['totalopenamt'] = fully_paid_3['amount']
            fully_paid_3['paidamt'] = fully_paid_3['amount']
            fully_paid_3['allocatedamt'] = fully_paid_3['amount']
            fully_paid_3['c_payment_id_paym'] = fully_paid_3['c_payment_id_x']
            fully_paid_3['c_payment_id_x'] = fully_paid_3['c_payment_id_y']
            self.rename_columns(fully_paid_3)
            self.drop_columns(fully_paid_3)
            # Eliminam facturile care apar platite dar nu au nici o referinta a platii.
            de_eliminat = invoice_terms_payments_allocations_df[
                (invoice_terms_payments_allocations_df.c_payment_id_y == 0) & (
                        invoice_terms_payments_allocations_df.c_cashline_id_y == 0)]
            invoice_terms_payments_allocations_df = invoice_terms_payments_allocations_df.drop(de_eliminat.index)
            # Din restul facturilor ramase, vom extrage acele facturi care au fost platite cu cash.
            cash_paid3 = invoice_terms_payments_allocations_df[
                invoice_terms_payments_allocations_df.c_cashline_id_y != 0]
            invoice_terms_payments_allocations_df = invoice_terms_payments_allocations_df.drop(cash_paid3.index)
            cash_paid3['grandtotal'] = cash_paid3['amount']
            cash_paid3['totalopenamt'] = cash_paid3['amount']
            cash_paid3['paidamt'] = cash_paid3['amount']
            cash_paid3['payamt'] = cash_paid3['amount']
            cash_paid3['allocatedamt'] = cash_paid3['amount']
            cash_paid3['c_cashline_id_x'] = cash_paid3['c_cashline_id_y']
            cash_paid3['datetrx'] = cash_paid3['dateinvoiced']
            self.rename_columns(cash_paid3)
            self.drop_columns(cash_paid3)
            #
            invoice_terms_payments_allocations_df['grandtotal'] = invoice_terms_payments_allocations_df['amount']
            invoice_terms_payments_allocations_df['totalopenamt'] = invoice_terms_payments_allocations_df['amount']
            invoice_terms_payments_allocations_df['paidamt'] = invoice_terms_payments_allocations_df['amount']
            invoice_terms_payments_allocations_df['allocatedamt'] = invoice_terms_payments_allocations_df['amount']
            invoice_terms_payments_allocations_df['c_payment_id_paym'] = invoice_terms_payments_allocations_df[
                'c_payment_id_x']
            invoice_terms_payments_allocations_df['c_payment_id_x'] = invoice_terms_payments_allocations_df[
                'c_payment_id_y']
            self.rename_columns(invoice_terms_payments_allocations_df)
            self.drop_columns(invoice_terms_payments_allocations_df)
            # Merge fisiere cu alocations
            p1 = pd.merge(partially_paid_3, allocations_df, how='left', on=["c_invoice_id", "ad_client_id"])
            p1['grandtotal'] = p1['amount']
            p1['totalopenamt'] = p1['amount']
            p1['paidamt'] = p1['amount']
            p1['allocatedamt'] = p1['amount']
            cash_paid4 = p1[p1.c_cashline_id_y != 0]
            p1 = p1.drop(cash_paid4.index)
            cash_paid4['payamt'] = cash_paid4['amount']
            cash_paid4['c_cashline_id_x'] = cash_paid4['c_cashline_id_y']
            cash_paid4['datetrx'] = cash_paid4['dateinvoiced']
            cash_paid4['c_payment_id_paym'] = np.nan
            cash_paid4['c_doctype_id'] = np.nan
            cash_paid4['tendertype'] = np.nan
            cash_paid4['isallocated'] = np.nan
            self.rename_columns(cash_paid4)
            self.drop_columns(cash_paid4)
            self.rename_columns(p1)
            self.drop_columns(p1)
            fp1 = pd.merge(fully_paid_2, payments_df, how='left', on=["c_payment_id", "ad_client_id"])
            fp2 = fp1[fp1.datetrx_y.notnull()]
            self.eliminare_avansuri(fp2)
            self.re_normalize_data(fp2)
            p2 = pd.merge(paid1, payments_df, how='left', on=["c_payment_id", "ad_client_id"])
            self.eliminare_avansuri(p2)
            self.re_normalize_data(p2)
            p3 = pd.merge(fully_paid_3, payments_df, how='left', on=["c_payment_id", "ad_client_id"])
            p3 = p3[p3.datetrx_y.notnull()]
            self.eliminare_avansuri(p3)
            self.re_normalize_data(p3)
            p4 = pd.merge(invoice_terms_payments_allocations_df, payments_df, how='left',
                          on=["c_payment_id", "ad_client_id"])
            del payments_df
            p4 = p4[p4.datetrx_y.notnull()]
            self.eliminare_avansuri(p4)
            self.re_normalize_data(p4)
            df = pd.concat(
                [fully_paid, partially_paid, unpaid_balance, partially_paid_2, cash_paid, p1, unpaid_invoices,
                 cash_paid4,
                 fp2, unpaid_balance2, p2, cash_paid2, cash_paid3, p3, p4])
            df = df[df.c_currency_id == 346]
            df.drop(columns=['c_currency_id'], inplace=True)
            del fully_paid, partially_paid, unpaid_balance, partially_paid_2, cash_paid, p1, unpaid_invoices, cash_paid4, fp2, unpaid_balance2, p2, cash_paid2, cash_paid3, p3, p4
        logger.info(f'Merge phase finished in {str(datetime.timedelta(seconds=(time.time() - start_time)))} seconds')
        df['c_allocationline_id'] = df['c_allocationline_id'].astype('Int64')
        df = self.__build_training_data(df)
        return df

    def __build_training_data(self, d_param: DataFrame) -> DataFrame:
        def closed_invoices(x):
            return sum([1 if t == 1 else 0 for t in x])

        start_time = time.time()
        logger.info(f'Running normalisation phase')
        with start_span(op="normalisation_phase", description="normalisation_phase") as span:
            span.set_data('ad_client_id', self.ad_client_id)
            t1 = pd.concat(g for _, g in d_param.groupby("c_invoice_id") if len(g) > 1)
            d_param = d_param.drop(t1.index)
            t1['c_invoice_id_sintetic'] = t1['c_invoice_id'].apply(str) + '_' + t1.groupby(
                'c_invoice_id').cumcount().astype('str')
            t1['c_invoice_id'] = t1["c_invoice_id_sintetic"]
            t1.drop(columns=['c_invoice_id_sintetic'], inplace=True)
            df = pd.concat([d_param, t1])
            del d_param
            del t1
            # transformare coloane ce contin date calendaristice
            df['dateinvoiced'] = pd.to_datetime(df.dateinvoiced)
            df['duedate'] = pd.to_datetime(df.duedate)
            df['datetrx'] = pd.to_datetime(df.datetrx)

            # informare
            logger.info('Distinct c_bpartner_id values: ' + str(df.c_bpartner_id.nunique()))
            logger.info('Distinct c_invoice_id values: ' + str(df.c_invoice_id.nunique()))
            logger.info('Oldest date invoiced: {} '.format(df.dateinvoiced.min()) +
                        ' - Newest date invoiced: {}'.format(df.dateinvoiced.max()))
            total_invoices = df['grandtotal'].sum()
            total_facturi_platite = df['allocatedamt'].sum()
            total_facturi_neplatite = total_invoices - total_facturi_platite
            logger.info(
                'Percent total unpaid invoices: {0}%'.format(round(100 * total_facturi_neplatite / total_invoices, 2)))

            # Există facturi care au fost plătite în avans, data plății lor figurând înainte de data emiterii facturii.
            # Vom separa aceste facturi și le vom elimina din model

            d1 = df[df.datetrx < df.dateinvoiced]

            # Eliminăm facturile plătite în avans din df
            df = df.drop(d1.index)

            # Pentru analiză vom avea nevoie de următoarele variabile:
            # 1. dayslate - reprezintă diferența de zile dintre data plății și data scadentă;
            # 2. daystosettle - reprezintă numărul de zile în care s-a efectuat plata calculate din momentul emiterii facturii;
            # 3. Late - variabilă de tip boolean; exprimă dacă o factură s-a plătit cu întarziere (Late = 1), sau la timp (Late = 0)

            dt = pd.to_datetime('2021-02-01', format='%Y-%m-%d')
            df['dayslate'] = (df.datetrx.fillna(dt) - df.duedate).dt.days

            df['dayslate'] = df['dayslate'].apply(lambda x: 0 if x <= 0 else x)
            df['daystosettle'] = (df['datetrx'] - df['dateinvoiced']).dt.days
            df['late'] = df['dayslate'].apply(lambda x: 1 if x > 0 else 0)

            # eliminam facturile cu valoare negativa
            df = df[df.grandtotal > 0]

            # eliminam coloane nenecesare
            df.drop(columns=['c_payment_id', 'c_cashline_id', 'c_payment_id_paym', 'isallocated',
                             'c_doctype_id', 'payamt'], inplace=True)

            if 'balance' in df.columns:
                df.drop(columns=['balance'], inplace=True)

            df['paid'] = (df['grandtotal'] - df['paidamt']).apply(lambda x: 1 if x <= 0.01 else 0)

        # # Extragem facturile platite pentru a previziona in cat timp se platesc

        # > df - facturile platite si neplatite: se va folosi pentru clasificare

        logger.info(
            f'Normalisation phase finished in {str(datetime.timedelta(seconds=(time.time() - start_time)))} seconds')
        df = self.__build_derived_features(closed_invoices, df)

        del closed_invoices

        return df

    def __build_derived_features(self, closed_invoices, df):
        start_time = time.time()
        logger.info('Running aggregation phase')
        with start_span(op="aggregation_phase", description="aggregation_phase") as span:
            span.set_data('ad_client_id', self.ad_client_id)
            pd.set_option('use_inf_as_na', True)
            # stampilare total facturi pe fiecare client
            df['total_invoices'] = df.groupby(['c_bpartner_id'])['c_invoice_id'].transform('count')
            # constructie atribute derivate
            df['closed_invoices_no'] = df.groupby(['c_bpartner_id'])['paid'].transform(closed_invoices)
            # flag factura inchisa cu intarziere
            df['closed_late_invoice'] = df[['late', 'paid']].apply(lambda t: 1 if t[0] == 1 and t[1] == 1 else 0,
                                                                   axis=1)
            # stampilare numarul facturi inchise cu intarziere pe fiecare client
            df['closed_late_invoices_no'] = df.groupby(['c_bpartner_id'])['closed_late_invoice'].transform('sum')
            df['paid_late_percent'] = df['closed_late_invoices_no'] / df['closed_invoices_no']
            # stampilare numar facturi inchise cu intarziere pe fiecare client
            df['paid_total'] = df.groupby(['c_bpartner_id'])['allocatedamt'].transform('sum')
            df['paid_late_sum'] = df.allocatedamt * df.late
            df['paid_late_total'] = df.groupby(['c_bpartner_id'])['paid_late_sum'].transform('sum')
            df['paid_late_raport_percent'] = df['paid_late_total'] / df['paid_total']
            df['days_late_closed_invoices_late'] = df.dayslate * df.closed_late_invoice
            df['avg_days_paid_late'] = df.groupby(['c_bpartner_id'])['days_late_closed_invoices_late'].transform(
                'sum') / \
                                       df.groupby(['c_bpartner_id'])['days_late_closed_invoices_late'].transform(
                                           lambda x: x.ne(0).sum())
            df['avg_days_paid_late'] = df['avg_days_paid_late'].fillna(0)
            df['unpaid_invoices_no'] = df['total_invoices'] - df['closed_invoices_no']
            df['invoice_late_unpaid'] = df.late * (1 - df.paid)
            df['late_unpaid_invoices_no'] = df.groupby(['c_bpartner_id'])['invoice_late_unpaid'].transform('sum')
            df['late_unpaid_invoices_percent'] = df['late_unpaid_invoices_no'] / df['unpaid_invoices_no']
            df['late_unpaid_invoices_percent'] = df['late_unpaid_invoices_percent'].fillna(0)
            df['unpaid_sum'] = df['grandtotal'] - df['paidamt']
            df['unpaid_invoices_sum'] = df.groupby(['c_bpartner_id'])['unpaid_sum'].transform('sum')
            df['unpaid_late_sum'] = df['unpaid_sum'] * df.late
            df['unpaid_invoices_late_sum'] = df.groupby(['c_bpartner_id'])['unpaid_late_sum'].transform('sum')
            df['late_unpaid_invoices_sum_percent'] = df['unpaid_invoices_late_sum'] / df['unpaid_invoices_sum']
            df['max_paid'] = df.groupby(['c_bpartner_id'])['allocatedamt'].transform('max')
            df['min_paid'] = df.groupby(['c_bpartner_id'])['allocatedamt'].transform('min')
            df['avg_paid'] = df.groupby(['c_bpartner_id'])['allocatedamt'].transform('mean')
            df['std_paid'] = df.groupby(['c_bpartner_id'])['allocatedamt'].transform('std')
            df['max_late_paid'] = df.groupby(['c_bpartner_id'])['paid_late_sum'].transform('max')
            df['min_late_paid'] = df.groupby(['c_bpartner_id'])['paid_late_sum'].transform('min')
            df['avg_late_paid'] = df.groupby(['c_bpartner_id'])['paid_late_sum'].transform('mean')
            df['std_late_paid'] = df.groupby(['c_bpartner_id'])['paid_late_sum'].transform('std')
            df['max_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_sum'].transform('max')
            df['min_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_sum'].transform('min')
            df['avg_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_sum'].transform('mean')
            df['std_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_sum'].transform('std')
            df['max_late_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_late_sum'].transform('max')
            df['min_late_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_late_sum'].transform('min')
            df['avg_late_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_late_sum'].transform('mean')
            df['std_late_unpaid'] = df.groupby(['c_bpartner_id'])['unpaid_late_sum'].transform('std')
        # df.fillna(value=0, inplace=True)
        logger.info(
            f'Aggregation phase finished in {str(datetime.timedelta(seconds=(time.time() - start_time)))} seconds')
        return df

    def clean_df(self, df: DataFrame, conn) -> DataFrame:
        return df
