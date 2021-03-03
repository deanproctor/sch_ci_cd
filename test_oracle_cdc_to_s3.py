import logging
import time
import pytest

logger = logging.getLogger(__name__)

def test_record_count_matches(oracle_cdc_metrics):
    assert oracle_cdc_metrics['counters']['pipeline.batchInputRecords.counter']['count'] == oracle_cdc_metrics['counters']['pipeline.batchOutputRecords.counter']['count'] 

@pytest.fixture(scope='module')
def oracle_cdc_metrics(sch, pipeline):
    try:
        logger.info('Creating test job ...')
        job_builder = sch.get_job_builder()
        job = job_builder.build('Test job for {} pipeline'.format(pipeline.name),
                                pipeline=pipeline)
        job.description = 'CI/CD test job'
        sch.add_job(job)
        sch.start_job(job)

        # Wait for records to be written.
        time.sleep(10)

        metrics = sch.api_client.get_job_metrics(job.job_id).response.json()
        yield metrics
    finally:
        sch.stop_job(job)

