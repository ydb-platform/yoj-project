load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "repository-ydb-common",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//databind",
        "//repository",
        "//util",
        "@java_contribs_stable//:io_prometheus_simpleclient",
    ],
)
