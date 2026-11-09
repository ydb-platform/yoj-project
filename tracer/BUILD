load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "tracer",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
)

java_test_suite(
    name = "tracer-tests",
    srcs = glob(["src/test/java/**/*.java"]),
    package_prefixes = [".tech"],
    runner = "junit4",
    deps = [
        ":tracer",
        "@java_contribs_stable//:junit_junit",
        "@java_contribs_stable//:org_assertj_assertj_core",
    ],
)
