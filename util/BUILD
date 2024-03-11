load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "util",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "@java_contribs_stable//:com_google_code_findbugs_jsr305",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
        "@java_contribs_stable//:org_slf4j_slf4j_api",
    ],
)

java_test_suite(
    name = "util-test",
    srcs = glob(["src/test/java/**/*.java"]),
    package_prefixes = [".tech"],
    runner = "junit4",
    visibility = ["//visibility:public"],
    deps = [
        ":util",
        "//bom:lombok",
        "@java_contribs_stable//:junit_junit",
        "@java_contribs_stable//:org_apache_logging_log4j_log4j_slf4j2_impl",
        "@java_contribs_stable//:org_assertj_assertj_core",
    ],
)
