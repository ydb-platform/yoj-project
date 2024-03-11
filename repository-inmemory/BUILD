load("@contrib_rules_jvm//java:defs.bzl", "java_library", "java_test_suite")

java_library(
    name = "repository-inmemory",
    srcs = glob(["src/main/**/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//bom:lombok",
        "//databind",
        "//repository",
        "//util",
        "@java_contribs_stable//:com_google_code_findbugs_jsr305",
        "@java_contribs_stable//:com_google_guava_guava",
        "@java_contribs_stable//:javax_annotation_javax_annotation_api",
        "@java_contribs_stable//:org_eclipse_collections_eclipse_collections",
        "@java_contribs_stable//:org_eclipse_collections_eclipse_collections_api",
    ],
)

java_test_suite(
    name = "repository-inmemory-tests",
    srcs = glob(["src/test/java/**/*.java"]),
    package_prefixes = [".tech"],
    resources = glob(["src/test/resources/**"]),
    runner = "junit4",
    deps = [
        "repository-inmemory",
        "//bom:lombok",
        "//json-jackson-v2",
        "//repository",
        "//repository-test",
    ],
)
