job('beam_PerformanceTests_Dataflow'){
    triggers {
        cron('0 */6 * * *')
    }
    steps {
        shell('git clone -b apache --single-branch https://github.com/jasonkuster/PerfKitBenchmarker.git')
        python{
            command('PerfKitBenchmarker/pkb.py --project=google.com:clouddfe --benchmarks=dpb_wordcount_benchmark --dpb_dataflow_staging_location=gs://temp-storage-for-perf-tests/staging --dpb_dataflow_jar=./ --dpb_wordcount_input=dataflow-samples/shakespeare/kinglear.txt --dpb_log_level=INFO --config_override=dpb_wordcount_benchmark.dpb_service.service_type=dataflow --bigquery_table=beam_performance.pkb_results --official=true')
        }
    }
}
