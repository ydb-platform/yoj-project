load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "repository",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//databind",
        "//util",
        "@java_contribs_stable//:com_google_code_findbugs_jsr305",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:io_prometheus_simpleclient",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
    ],
)

java_test_suite(
    name = "repository-tests",
    srcs = glob(["src/test/java/**/*.java"]),
    package_prefixes = [".tech"],
    resources = ["src/test/resources/log4j2.yaml"],
    runner = "junit4",
    deps = [
        "repository",
        "//bom:lombok",
        "//databind",
        "@java_contribs_stable//:com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
        "@java_contribs_stable//:org_apache_logging_log4j_log4j_api",
        "@java_contribs_stable//:org_apache_logging_log4j_log4j_core",
        "@java_contribs_stable//:org_apache_logging_log4j_log4j_slf4j2_impl",
        "@java_contribs_stable//:org_assertj_assertj_core",
        "@java_contribs_stable//:org_mockito_mockito_core",
        "@java_contribs_stable//:org_yaml_snakeyaml",
    ],
)
