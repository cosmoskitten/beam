This recreates the proto descriptor set included in this resource directory.

```bash
export PROTO_INCLUDE=<proto_include_dir>
```
Execute the following command to create the pb files, in the beam root folder:

```bash
protoc \
 -Isdks/java/extensions/protobuf/src/test/resources/ \
 -I$PROTO_INCLUDE \
 --descriptor_set_out=sdks/java/extensions/protobuf/src/test/resources/org/apache/beam/sdk/extensions/protobuf/test_option_v1.pb \
 --include_imports \
 sdks/java/extensions/protobuf/src/test/resources/test/option/v1/simple.proto
```
