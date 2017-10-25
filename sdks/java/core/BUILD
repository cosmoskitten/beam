java_library(
  name = "beam-sdks-java-core",
  visibility = ["//visibility:public"],

  srcs = glob(["src/main/java/**/*.java"]),
  deps = [
      "@com_google_code_findbugs_jsr305//jar",
      "@org_slf4j_slf4j_api//jar",
      "@joda_time_joda_time//jar",
      "@com_github_stephenc_findbugs_findbugs_annotations//jar",
      "@org_ow2_asm_asm//jar",
      "@com_google_auto_value_auto_value//jar",
      "@com_google_auto_service_auto_service//jar",
      "@org_xerial_snappy_snappy_java//jar",
      "@net_bytebuddy_byte_buddy//jar",
      "@com_google_guava_guava//jar",
      "@org_apache_avro_avro//jar",
      "@com_fasterxml_jackson_core_jackson_core//jar",
      "@com_fasterxml_jackson_core_jackson_databind//jar",
      "@com_fasterxml_jackson_core_jackson_annotations//jar",
      "@org_hamcrest_hamcrest_all//jar",
      "@junit_junit//jar",
      "@com_google_protobuf_protobuf_java//jar",
      "@org_apache_commons_commons_compress//jar",
      "@org_apache_commons_commons_lang3//jar",
      ":auto",
  ]
)

java_plugin(
    name = "auto_plugin",
    processor_class = "com.google.auto.value.processor.AutoValueProcessor",
    deps = ["@com_google_auto_value_auto_value//jar"],
)

java_library(
    name = "auto",
    exported_plugins = [":auto_plugin"],
    exports = ["@com_google_auto_value_auto_value//jar"],
)

