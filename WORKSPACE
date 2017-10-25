# This statement defines the @com_google_protobuf repo
#
# proto_library rules implicitly depend on @com_google_protobuf//:protoc,
# which is the proto-compiler.
#
# proto_library does not respect package decls in proto files;
# you have to manually map them to their location on the filesystem
# https://github.com/bazelbuild/bazel/issues/3867
http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-ee8a0911cbaca2a1849a847bbdc523120d003a31",
    urls = ["https://github.com/google/protobuf/archive/ee8a0911cbaca2a1849a847bbdc523120d003a31.zip"],
)


maven_jar(
    name = "com_google_code_findbugs_jsr305",
    artifact = "com.google.code.findbugs:jsr305:2.0.2",
    sha1 = "516c03b21d50a644d538de0f0369c620989cd8f0",
)

maven_jar(
    name = "joda_time_joda_time",
    artifact = "joda-time:joda-time:2.4",
    sha1 = "89e9725439adffbbd41c5f5c215c136082b34a7f",
)

maven_jar(
    name = "com_esotericsoftware_reflectasm_reflectasm",
    artifact = "com.esotericsoftware.reflectasm:reflectasm:1.07",
    sha1 = "761028ef46da8ec16a16b25ce942463eb1a9f3d5",
)

maven_jar(
    name = "com_google_protobuf_protobuf_java",
    artifact = "com.google.protobuf:protobuf-java:3.2.0",
    sha1 = "62ccf171a106ff6791507f2d5364c275f9a3131d",
)

maven_jar(
    name = "com_github_stephenc_findbugs_findbugs_annotations",
    artifact = "com.github.stephenc.findbugs:findbugs-annotations:1.3.9-1",
    sha1 = "a6b11447635d80757d64b355bed3c00786d86801",
)

maven_jar(
    name = "org_ow2_asm_asm",
    artifact = "org.ow2.asm:asm:4.0",
    sha1 = "659add6efc75a4715d738e73f07505246edf4d66",
)

maven_jar(
    name = "com_google_auto_value_auto_value",
    artifact = "com.google.auto.value:auto-value:1.4.1",
    sha1 = "8172ebbd7970188aff304c8a420b9f17168f6f48",
)

maven_jar(
    name = "com_google_auto_service_auto_service",
    artifact = "com.google.auto.service:auto-service:1.0-rc2",
)

maven_jar(
    name = "com_google_guava_guava_testlib",
    artifact = "com.google.guava:guava-testlib:20.0",
    sha1 = "e3666edd0d7b10ddfa5242b998efd831e4b264ff",
)

maven_jar(
    name = "com_thoughtworks_paranamer_paranamer",
    artifact = "com.thoughtworks.paranamer:paranamer:2.7",
    sha1 = "3ed64c69e882a324a75e890024c32a28aff0ade8",
)

maven_jar(
    name = "com_google_errorprone_error_prone_annotations",
    artifact = "com.google.errorprone:error_prone_annotations:2.0.12",
    sha1 = "8530d22d4ae8419e799d5a5234e0d2c0dcf15d4b",
)

maven_jar(
    name = "net_bytebuddy_byte_buddy",
    artifact = "net.bytebuddy:byte-buddy:1.6.8",
    sha1 = "e84a4da03cfb0a76a0cca8d7b19db4d6ad6d3397",
)

maven_jar(
    name = "org_xerial_snappy_snappy_java",
    artifact = "org.xerial.snappy:snappy-java:1.1.1.3",
    sha1 = "fbd7b0b8400ebd0d6a2c61493f39530a93d9c4b6",
)

maven_jar(
    name = "org_objenesis_objenesis",
    artifact = "org.objenesis:objenesis:1.2",
    sha1 = "bfcb0539a071a4c5a30690388903ac48c0667f2a",
)

maven_jar(
    name = "junit_junit",
    artifact = "junit:junit:4.12",
)

maven_jar(
    name = "org_apache_avro_avro",
    artifact = "org.apache.avro:avro:1.8.2",
    sha1 = "91e3146dfff4bd510181032c8276a3a0130c0697",
)

maven_jar(
    name = "org_tukaani_xz",
    artifact = "org.tukaani:xz:1.5",
    sha1 = "9c64274b7dbb65288237216e3fae7877fd3f2bee",
)

maven_jar(
    name = "org_slf4j_slf4j_api",
    artifact = "org.slf4j:slf4j-api:1.7.25",
    sha1 = "da76ca59f6a57ee3102f8f9bd9cee742973efa8a",
)

maven_jar(
    name = "org_apache_commons_commons_compress",
    artifact = "org.apache.commons:commons-compress:1.14",
)

maven_jar(
    name = "com_esotericsoftware_kryo_kryo",
    artifact = "com.esotericsoftware.kryo:kryo:2.21",
    sha1 = "09a4e69cff8d225729656f7e97e40893b23bffef",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_annotations",
    artifact = "com.fasterxml.jackson.core:jackson-annotations:2.8.9",
    sha1 = "e0e758381a6579cb2029dace23a7209b90ac7232",
)

maven_jar(
    name = "org_mockito_mockito_all",
    artifact = "org.mockito:mockito-all:1.9.5",
    sha1 = "79a8984096fc6591c1e3690e07d41be506356fa5",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_databind",
    artifact = "com.fasterxml.jackson.core:jackson-databind:2.8.9",
    sha1 = "4dfca3975be3c1a98eacb829e70f02e9a71bc159",
)

maven_jar(
    name = "com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
    artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.8.9",
    sha1 = "607f3253c20267e385c85f60c859760a73a29e37",
)

maven_jar(
    name = "org_apache_commons_commons_lang3",
    artifact = "org.apache.commons:commons-lang3:3.6",
    sha1 = "9d28a6b23650e8a7e9063c04588ace6cf7012c17",
)

maven_jar(
    name = "com_google_guava_guava",
    artifact = "com.google.guava:guava:20.0",
    sha1 = "89507701249388e1ed5ddcf8c41f4ce1be7831ef",
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_core",
    artifact = "com.fasterxml.jackson.core:jackson-core:2.8.9",
    sha1 = "569b1752705da98f49aabe2911cc956ff7d8ed9d",
)

# com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:bundle:2.8.9
maven_jar(
    name = "org_yaml_snakeyaml",
    artifact = "org.yaml:snakeyaml:1.17",
    sha1 = "7a27ea250c5130b2922b86dea63cbb1cc10a660c",
)

maven_jar(
    name = "org_hamcrest_hamcrest_all",
    artifact = "org.hamcrest:hamcrest-all:1.3",
    sha1 = "63a21ebc981131004ad02e0434e799fd7f3a8d5a",
)

maven_jar(
    name = "com_esotericsoftware_minlog_minlog",
    artifact = "com.esotericsoftware.minlog:minlog:1.2",
    sha1 = "59bfcd171d82f9981a5e242b9e840191f650e209",
)
