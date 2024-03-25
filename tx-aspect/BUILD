load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "tx-aspect",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//repository",
        "@java_contribs_stable//:org_aspectj_aspectjweaver",
    ],
)
