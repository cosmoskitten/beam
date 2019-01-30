[1mdiff --git i/sdks/java/core/src/main/java/org/apache/beam/sdk/io/AvroIO.java w/sdks/java/core/src/main/java/org/apache/beam/sdk/io/AvroIO.java[m
[1mindex 2cf6027931..41bfb0984b 100644[m
[1m--- i/sdks/java/core/src/main/java/org/apache/beam/sdk/io/AvroIO.java[m
[1m+++ w/sdks/java/core/src/main/java/org/apache/beam/sdk/io/AvroIO.java[m
[36m@@ -383,6 +383,19 @@[m [mpublic class AvroIO {[m
         .build();[m
   }[m
 [m
[32m+[m[32m  /**[m
[32m+[m[32m   * Like {@link #parseGenericRecords(SerializableFunction)}, but reads each {@link[m
[32m+[m[32m   * FileIO.ReadableFile} in the input {@link PCollection}.[m
[32m+[m[32m   */[m
[32m+[m[32m  public static <T> ParseFiles<T> parseFilesGenericRecords([m
[32m+[m[32m      SerializableFunction<GenericRecord, T> parseFn) {[m
[32m+[m[32m    return new AutoValue_AvroIO_ParseFiles.Builder<T>()[m
[32m+[m[32m        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))[m
[32m+[m[32m        .setParseFn(parseFn)[m
[32m+[m[32m        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)[m
[32m+[m[32m        .build();[m
[32m+[m[32m  }[m
[32m+[m
   /**[m
    * Like {@link #parseGenericRecords(SerializableFunction)}, but reads each filepattern in the[m
    * input {@link PCollection}.[m
[36m@@ -636,9 +649,10 @@[m [mpublic class AvroIO {[m
 [m
   /////////////////////////////////////////////////////////////////////////////[m
 [m
[31m-  /** Implementation of {@link #readAll}. */[m
[32m+[m[32m  /** Implementation of {@link #readFiles}. */[m
   @AutoValue[m
[31m-  public abstract static class ReadAll<T> extends PTransform<PCollection<String>, PCollection<T>> {[m
[32m+[m[32m  public abstract static class ReadFiles<T>[m
[32m+[m[32m      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {[m
     abstract MatchConfiguration getMatchConfiguration();[m
 [m
     @Nullable[m
[36m@@ -665,29 +679,29 @@[m [mpublic class AvroIO {[m
 [m
       abstract Builder<T> setInferBeamSchema(boolean infer);[m
 [m
[31m-      abstract ReadAll<T> build();[m
[32m+[m[32m      abstract ReadFiles<T> build();[m
     }[m
 [m
     /** Sets the {@link MatchConfiguration}. */[m
[31m-    public ReadAll<T> withMatchConfiguration(MatchConfiguration configuration) {[m
[32m+[m[32m    public ReadFiles<T> withMatchConfiguration(MatchConfiguration configuration) {[m
       return toBuilder().setMatchConfiguration(configuration).build();[m
     }[m
 [m
     /** Like {@link Read#withEmptyMatchTreatment}. */[m
[31m-    public ReadAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {[m
[32m+[m[32m    public ReadFiles<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {[m
       return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));[m
     }[m
 [m
     /** Like {@link Read#watchForNewFiles}. */[m
     @Experimental(Kind.SPLITTABLE_DO_FN)[m
[31m-    public ReadAll<T> watchForNewFiles([m
[32m+[m[32m    public ReadFiles<T> watchForNewFiles([m
         Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {[m
       return withMatchConfiguration([m
           getMatchConfiguration().continuously(pollInterval, terminationCondition));[m
     }[m
 [m
     @VisibleForTesting[m
[31m-    ReadAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {[m
[32m+[m[32m    ReadFiles<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {[m
       return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();[m
     }[m
 [m
[36m@@ -696,18 +710,20 @@[m [mpublic class AvroIO {[m
      * to be used by SQL and by the schema-transform library.[m
      */[m
     @Experimental(Kind.SCHEMAS)[m
[31m-    public ReadAll<T> withBeamSchemas(boolean withBeamSchemas) {[m
[32m+[m[32m    public ReadFiles<T> withBeamSchemas(boolean withBeamSchemas) {[m
       return toBuilder().setInferBeamSchema(withBeamSchemas).build();[m
     }[m
 [m
     @Override[m
[31m-    public PCollection<T> expand(PCollection<String> input) {[m
[32m+[m[32m    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {[m
       checkNotNull(getSchema(), "schema");[m
       PCollection<T> read =[m
[31m-          input[m
[31m-              .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))[m
[31m-              .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))[m
[31m-              .apply(readFiles(getRecordClass()));[m
[32m+[m[32m          input.apply([m
[32m+[m[32m              "Read all via FileBasedSource",[m
[32m+[m[32m              new ReadAllViaFileBasedSource<>([m
[32m+[m[32m                  getDesiredBundleSizeBytes(),[m
[32m+[m[32m                  new CreateSourceFn<>(getRecordClass(), getSchema().toString()),[m
[32m+[m[32m                  AvroCoder.of(getRecordClass(), getSchema())));[m
       return getInferBeamSchema() ? setBeamSchema(read, getRecordClass(), getSchema()) : read;[m
     }[m
 [m
[36m@@ -718,32 +734,11 @@[m [mpublic class AvroIO {[m
     }[m
   }[m
 [m
[31m-  private static class CreateSourceFn<T>[m
[31m-      implements SerializableFunction<String, FileBasedSource<T>> {[m
[31m-    private final Class<T> recordClass;[m
[31m-    private final Supplier<Schema> schemaSupplier;[m
[31m-[m
[31m-    public CreateSourceFn(Class<T> recordClass, String jsonSchema) {[m
[31m-      this.recordClass = recordClass;[m
[31m-      this.schemaSupplier = AvroUtils.serializableSchemaSupplier(jsonSchema);[m
[31m-    }[m
[31m-[m
[31m-    @Override[m
[31m-    public FileBasedSource<T> apply(String input) {[m
[31m-      return Read.createSource([m
[31m-          StaticValueProvider.of(input),[m
[31m-          EmptyMatchTreatment.DISALLOW,[m
[31m-          recordClass,[m
[31m-          schemaSupplier.get());[m
[31m-    }[m
[31m-  }[m
[31m-[m
   /////////////////////////////////////////////////////////////////////////////[m
 [m
[31m-  /** Implementation of {@link #readFiles}. */[m
[32m+[m[32m  /** Implementation of {@link #readAll}. */[m
   @AutoValue[m
[31m-  public abstract static class ReadFiles<T>[m
[31m-      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {[m
[32m+[m[32m  public abstract static class ReadAll<T> extends PTransform<PCollection<String>, PCollection<T>> {[m
     abstract MatchConfiguration getMatchConfiguration();[m
 [m
     @Nullable[m
[36m@@ -770,29 +765,29 @@[m [mpublic class AvroIO {[m
 [m
       abstract Builder<T> setInferBeamSchema(boolean infer);[m
 [m
[31m-      abstract ReadFiles<T> build();[m
[32m+[m[32m      abstract ReadAll<T> build();[m
     }[m
 [m
     /** Sets the {@link MatchConfiguration}. */[m
[31m-    public ReadFiles<T> withMatchConfiguration(MatchConfiguration configuration) {[m
[32m+[m[32m    public ReadAll<T> withMatchConfiguration(MatchConfiguration configuration) {[m
       return toBuilder().setMatchConfiguration(configuration).build();[m
     }[m
 [m
     /** Like {@link Read#withEmptyMatchTreatment}. */[m
[31m-    public ReadFiles<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {[m
[32m+[m[32m    public ReadAll<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {[m
       return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));[m
     }[m
 [m
     /** Like {@link Read#watchForNewFiles}. */[m
     @Experimental(Kind.SPLITTABLE_DO_FN)[m
[31m-    public ReadFiles<T> watchForNewFiles([m
[32m+[m[32m    public ReadAll<T> watchForNewFiles([m
         Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {[m
       return withMatchConfiguration([m
           getMatchConfiguration().continuously(pollInterval, terminationCondition));[m
     }[m
 [m
     @VisibleForTesting[m
[31m-    ReadFiles<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {[m
[32m+[m[32m    ReadAll<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {[m
       return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();[m
     }[m
 [m
[36m@@ -801,20 +796,18 @@[m [mpublic class AvroIO {[m
      * to be used by SQL and by the schema-transform library.[m
      */[m
     @Experimental(Kind.SCHEMAS)[m
[31m-    public ReadFiles<T> withBeamSchemas(boolean withBeamSchemas) {[m
[32m+[m[32m    public ReadAll<T> withBeamSchemas(boolean withBeamSchemas) {[m
       return toBuilder().setInferBeamSchema(withBeamSchemas).build();[m
     }[m
 [m
     @Override[m
[31m-    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {[m
[32m+[m[32m    public PCollection<T> expand(PCollection<String> input) {[m
       checkNotNull(getSchema(), "schema");[m
       PCollection<T> read =[m
[31m-          input.apply([m
[31m-              "Read all via FileBasedSource",[m
[31m-              new ReadAllViaFileBasedSource<>([m
[31m-                  getDesiredBundleSizeBytes(),[m
[31m-                  new CreateSourceFn<>(getRecordClass(), getSchema().toString()),[m
[31m-                  AvroCoder.of(getRecordClass(), getSchema())));[m
[32m+[m[32m          input[m
[32m+[m[32m              .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))[m
[32m+[m[32m              .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))[m
[32m+[m[32m              .apply(readFiles(getRecordClass()));[m
       return getInferBeamSchema() ? setBeamSchema(read, getRecordClass(), getSchema()) : read;[m
     }[m
 [m
[36m@@ -825,6 +818,26 @@[m [mpublic class AvroIO {[m
     }[m
   }[m
 [m
[32m+[m[32m  private static class CreateSourceFn<T>[m
[32m+[m[32m      implements SerializableFunction<String, FileBasedSource<T>> {[m
[32m+[m[32m    private final Class<T> recordClass;[m
[32m+[m[32m    private final Supplier<Schema> schemaSupplier;[m
[32m+[m
[32m+[m[32m    public CreateSourceFn(Class<T> recordClass, String jsonSchema) {[m
[32m+[m[32m      this.recordClass = recordClass;[m
[32m+[m[32m      this.schemaSupplier = AvroUtils.serializableSchemaSupplier(jsonSchema);[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    @Override[m
[32m+[m[32m    public FileBasedSource<T> apply(String input) {[m
[32m+[m[32m      return Read.createSource([m
[32m+[m[32m          StaticValueProvider.of(input),[m
[32m+[m[32m          EmptyMatchTreatment.DISALLOW,[m
[32m+[m[32m          recordClass,[m
[32m+[m[32m          schemaSupplier.get());[m
[32m+[m[32m    }[m
[32m+[m[32m  }[m
[32m+[m
   /////////////////////////////////////////////////////////////////////////////[m
 [m
   /** Implementation of {@link #parseGenericRecords}. */[m
[36m@@ -947,6 +960,101 @@[m [mpublic class AvroIO {[m
 [m
   /////////////////////////////////////////////////////////////////////////////[m
 [m
[32m+[m[32m  /** Implementation of {@link #parseFilesGenericRecords}. */[m
[32m+[m[32m  @AutoValue[m
[32m+[m[32m  public abstract static class ParseFiles<T>[m
[32m+[m[32m      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {[m
[32m+[m[32m    abstract MatchConfiguration getMatchConfiguration();[m
[32m+[m
[32m+[m[32m    abstract SerializableFunction<GenericRecord, T> getParseFn();[m
[32m+[m
[32m+[m[32m    @Nullable[m
[32m+[m[32m    abstract Coder<T> getCoder();[m
[32m+[m
[32m+[m[32m    abstract long getDesiredBundleSizeBytes();[m
[32m+[m
[32m+[m[32m    abstract Builder<T> toBuilder();[m
[32m+[m
[32m+[m[32m    @AutoValue.Builder[m
[32m+[m[32m    abstract static class Builder<T> {[m
[32m+[m[32m      abstract Builder<T> setMatchConfiguration(MatchConfiguration matchConfiguration);[m
[32m+[m
[32m+[m[32m      abstract Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);[m
[32m+[m
[32m+[m[32m      abstract Builder<T> setCoder(Coder<T> coder);[m
[32m+[m
[32m+[m[32m      abstract Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes);[m
[32m+[m
[32m+[m[32m      abstract ParseFiles<T> build();[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    /** Sets the {@link MatchConfiguration}. */[m
[32m+[m[32m    public ParseFiles<T> withMatchConfiguration(MatchConfiguration configuration) {[m
[32m+[m[32m      return toBuilder().setMatchConfiguration(configuration).build();[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    /** Like {@link Read#withEmptyMatchTreatment}. */[m
[32m+[m[32m    public ParseFiles<T> withEmptyMatchTreatment(EmptyMatchTreatment treatment) {[m
[32m+[m[32m      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    /** Like {@link Read#watchForNewFiles}. */[m
[32m+[m[32m    @Experimental(Kind.SPLITTABLE_DO_FN)[m
[32m+[m[32m    public ParseFiles<T> watchForNewFiles([m
[32m+[m[32m        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {[m
[32m+[m[32m      return withMatchConfiguration([m
[32m+[m[32m          getMatchConfiguration().continuously(pollInterval, terminationCondition));[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    /** Specifies the coder for the result of the {@code parseFn}. */[m
[32m+[m[32m    public ParseFiles<T> withCoder(Coder<T> coder) {[m
[32m+[m[32m      return toBuilder().setCoder(coder).build();[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    @VisibleForTesting[m
[32m+[m[32m    ParseFiles<T> withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {[m
[32m+[m[32m      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    @Override[m
[32m+[m[32m    public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {[m
[32m+[m[32m      final Coder<T> coder =[m
[32m+[m[32m          Parse.inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());[m
[32m+[m[32m      final SerializableFunction<GenericRecord, T> parseFn = getParseFn();[m
[32m+[m[32m      final SerializableFunction<String, FileBasedSource<T>> createSource =[m
[32m+[m[32m          new CreateParseSourceFn<>(parseFn, coder);[m
[32m+[m[32m      return input.apply([m
[32m+[m[32m          "Parse Files via FileBasedSource",[m
[32m+[m[32m          new ReadAllViaFileBasedSource<>(getDesiredBundleSizeBytes(), createSource, coder));[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    @Override[m
[32m+[m[32m    public void populateDisplayData(DisplayData.Builder builder) {[m
[32m+[m[32m      super.populateDisplayData(builder);[m
[32m+[m[32m      builder[m
[32m+[m[32m          .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"))[m
[32m+[m[32m          .include("matchConfiguration", getMatchConfiguration());[m
[32m+[m[32m    }[m
[32m+[m
[32m+[m[32m    private static class CreateParseSourceFn<T>[m
[32m+[m[32m        implements SerializableFunction<String, FileBasedSource<T>> {[m
[32m+[m[32m      private final SerializableFunction<GenericRecord, T> parseFn;[m
[32m+[m[32m      private final Coder<T> coder;[m
[32m+[m
[32m+[m[32m      public CreateParseSourceFn(SerializableFunction<GenericRecord, T> parseFn, Coder<T> coder) {[m
[32m+[m[32m        this.parseFn = parseFn;[m
[32m+[m[32m        this.coder = coder;[m
[32m+[m[32m      }[m
[32m+[m
[32m+[m[32m      @Override[m
[32m+[m[32m      public FileBasedSource<T> apply(String input) {[m
[32m+[m[32m        return AvroSource.from(input).withParseFn(parseFn, coder);[m
[32m+[m[32m      }[m
[32m+[m[32m    }[m
[32m+[m[32m  }[m
[32m+[m
[32m+[m[32m  /////////////////////////////////////////////////////////////////////////////[m
[32m+[m
   /** Implementation of {@link #parseAllGenericRecords}. */[m
   @AutoValue[m
   public abstract static class ParseAll<T> extends PTransform<PCollection<String>, PCollection<T>> {[m
[36m@@ -1004,17 +1112,10 @@[m [mpublic class AvroIO {[m
 [m
     @Override[m
     public PCollection<T> expand(PCollection<String> input) {[m
[31m-      final Coder<T> coder =[m
[31m-          Parse.inferCoder(getCoder(), getParseFn(), input.getPipeline().getCoderRegistry());[m
[31m-      final SerializableFunction<GenericRecord, T> parseFn = getParseFn();[m
[31m-      final SerializableFunction<String, FileBasedSource<T>> createSource =[m
[31m-          new CreateParseSourceFn<>(parseFn, coder);[m
       return input[m
           .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))[m
           .apply(FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))[m
[31m-          .apply([m
[31m-              "Parse all via FileBasedSource",[m
[31m-              new ReadAllViaFileBasedSource<>(getDesiredBundleSizeBytes(), createSource, coder));[m
[32m+[m[32m          .apply("Parse all via FileBasedSource", parseFilesGenericRecords(getParseFn()));[m
     }[m
 [m
     @Override[m
[36m@@ -1042,7 +1143,7 @@[m [mpublic class AvroIO {[m
     }[m
   }[m
 [m
[31m-  // ///////////////////////////////////////////////////////////////////////////[m
[32m+[m[32m  /////////////////////////////////////////////////////////////////////////////[m
 [m
   /** Implementation of {@link #write}. */[m
   @AutoValue[m
