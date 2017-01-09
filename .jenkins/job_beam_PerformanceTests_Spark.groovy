job('beam_PerformanceTests_Spark'){
    triggers {
        cron('0 */6 * * *')
    }
    steps {
        shell('git clone -b apache --single-branch https://github.com/jasonkuster/PerfKitBenchmarker.git')
        python{
            command('PerfKitBenchmarker/pkb.py --project=google.com:clouddfe --benchmarks=dpb_wordcount_benchmark --dpb_wordcount_input=/etc/hosts --dpb_log_level=INFO --config_override=dpb_wordcount_benchmark.dpb_service.service_type=dataproc')
        }
    }
}
