load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "repository-test",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//databind",
        "//repository",
        "//util",
        "@java_contribs_stable//:com_fasterxml_jackson_core_jackson_annotations",
        "@java_contribs_stable//:com_fasterxml_jackson_core_jackson_core",
        "@java_contribs_stable//:com_fasterxml_jackson_core_jackson_databind",
        "@java_contribs_stable//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
        "@java_contribs_stable//:com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
        "@java_contribs_stable//:junit_junit",
        "@java_contribs_stable//:org_assertj_assertj_core",
    ],
)
