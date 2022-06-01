from enum import Enum, unique
from service.pipeline_impl import InvoiceCleaningPipeline, AllocationCleaningPipeline, PaymentCleaningPipeline, \
    PaymentTermCleaningPipeline, AggregationPipeline


@unique
class Pipeline(Enum):
    """ (DB TableName - Cleaning AbstractPipeline) Mappings """
    INVOICE = InvoiceCleaningPipeline
    PAYMENT = PaymentCleaningPipeline
    PAYMENT_TERM = PaymentTermCleaningPipeline
    ALLOCATION = AllocationCleaningPipeline
    AGGREGATION = AggregationPipeline
