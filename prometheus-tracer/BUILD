load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "prometheus-tracer",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//repository",
        "//util",
        "@java_contribs_stable//:com_google_code_findbugs_jsr305",
        "@java_contribs_stable//:io_prometheus_prometheus_metrics_core",
        "@java_contribs_stable//:io_prometheus_prometheus_metrics_model",
        "@java_contribs_stable//:org_slf4j_slf4j_api",
    ],
)

java_test_suite(
    name = "prometheus-tracer-tests",
    srcs = glob(["src/test/java/**/*.java"]),
    package_prefixes = [".tech"],
    runner = "junit4",
    deps = [
        ":prometheus-tracer",
        "//repository",
        "@java_contribs_stable//:io_prometheus_prometheus_metrics_model",
        "@java_contribs_stable//:junit_junit",
        "@java_contribs_stable//:org_assertj_assertj_core",
    ],
)
