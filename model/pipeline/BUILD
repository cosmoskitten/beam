proto_library(
  name = "pipeline_proto",
  visibility = ["//visibility:public"],
  srcs = glob(["beam_runner_api.proto"]),
  deps = [
    "@com_google_protobuf//:any_proto",
    "@com_google_protobuf//:duration_proto",
    "@com_google_protobuf//:timestamp_proto",
  ],
)

proto_library(
  name = "endpoints_proto",
  visibility = ["//visibility:public"],
  srcs = glob(["endpoints.proto"]),
)

proto_library(
  name = "standard_window_fns_proto",
  srcs = glob(["standard_window_fns.proto"]),
  deps = [
    "@com_google_protobuf//:duration_proto",
    "@com_google_protobuf//:timestamp_proto",
  ],
)
