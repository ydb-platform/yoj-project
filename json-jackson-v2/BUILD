load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "json-jackson-v2",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//repository",
        "@java_contribs_stable//:com_fasterxml_jackson_core_jackson_annotations",
        "@java_contribs_stable//:com_fasterxml_jackson_core_jackson_databind",
        "@java_contribs_stable//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
        "@java_contribs_stable//:com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
        "@java_contribs_stable//:com_google_code_findbugs_jsr305",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
    ],
)
