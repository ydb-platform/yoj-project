load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "databind",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//util",
        "@java_contribs_stable//:com_google_code_findbugs_jsr305",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
        "@java_contribs_stable//:org_jetbrains_annotations",
        "@java_contribs_stable//:org_jetbrains_kotlin_kotlin_reflect",
        "@java_contribs_stable//:org_jetbrains_kotlin_kotlin_stdlib",
        "@java_contribs_stable//:org_slf4j_slf4j_api",
    ],
)

java_test_suite(
    name = "databind-tests",
    srcs = glob(["src/test/java/**/*.java"]),
    package_prefixes = [".tech"],
    resources = glob(["src/test/resources/**"]),
    runner = "junit4",
    deps = [
        ":databind",
        "//bom:lombok",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
        "@java_contribs_stable//:org_assertj_assertj_core",
    ],
)
