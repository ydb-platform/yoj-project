load("@contrib_rules_jvm//java:defs.bzl", "java_library")

java_plugin(
    name = "lombok_plugin",
    generates_api = True,
    processor_class = "lombok.launch.AnnotationProcessorHider$AnnotationProcessor",
    deps = ["@java_contribs_stable//:org_projectlombok_lombok"],
)

java_library(
    name = "lombok",
    data = ["lombok.config"],
    exported_plugins = [":lombok_plugin"],
    neverlink = True,
    visibility = ["//visibility:public"],
    exports = [
        "@java_contribs_stable//:org_projectlombok_lombok",
        "@java_contribs_stable//:org_slf4j_slf4j_api",
    ],
)
