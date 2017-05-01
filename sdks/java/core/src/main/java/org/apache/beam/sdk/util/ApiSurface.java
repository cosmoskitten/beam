/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.anyOf;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSource;
import com.google.common.io.Resources;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.Parameter;
import com.google.common.reflect.Reflection;
import com.google.common.reflect.TypeToken;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the API surface of a package prefix. Used for accessing public classes, methods, and
 * the types they reference, to control what dependencies are re-exported.
 *
 * <p>For the purposes of calculating the public API surface, exposure includes any public or
 * protected occurrence of:
 *
 * <ul>
 * <li>superclasses
 * <li>interfaces implemented
 * <li>actual type arguments to generic types
 * <li>array component types
 * <li>method return types
 * <li>method parameter types
 * <li>type variable bounds
 * <li>wildcard bounds
 * </ul>
 *
 * <p>Exposure is a transitive property. The resulting map excludes primitives and array classes
 * themselves.
 *
 * <p>It is prudent (though not required) to prune prefixes like "java" via the builder method
 * {@link #pruningPrefix} to halt the traversal so it does not uselessly catalog references that are
 * not interesting.
 */
@SuppressWarnings("rawtypes")
public class ApiSurface {

  /**
   * Scans the source of a {@link ClassLoader} and finds all loadable classes and resources.
   *
   * <p><b>Warning:</b> Currently only {@link URLClassLoader} and only {@code file://} urls are
   * supported.
   * </p>
   *
   * <p>Based on Ben Yu's implementation in
   * <a href="https://github.com/google/guava/blob/896c51abd32e136621c13d56b6130d0a72f4957a/guava/src/com/google/common/reflect/ClassPath.java">Guava</a>.
   * </p>
   *
   * <p><b>Note:</b> Internalised here to avoid a forced upgrade to
   * <a href="https://github.com/google/guava/releases/tag/v21.0">Guava 21.0 which requires
   * Java 8.</a>
   * </p>
   */
  @Beta
  private static final class ClassPath {

    private static final Logger logger = LoggerFactory.getLogger(ClassPath.class.getName());

    private static final Predicate<ClassInfo> IS_TOP_LEVEL =
        new Predicate<ClassInfo>() {

          @Override
          public boolean apply(ClassInfo info) {
            return info != null && info.className.indexOf('$') == -1;
          }
        };

    /** Separator for the Class-Path manifest attribute value in jar files. */
    private static final Splitter CLASS_PATH_ATTRIBUTE_SEPARATOR =
        Splitter.on(" ").omitEmptyStrings();

    private static final String CLASS_FILE_NAME_EXTENSION = ".class";

    private final ImmutableSet<ResourceInfo> resources;

    private ClassPath(ImmutableSet<ResourceInfo> resources) {
      this.resources = resources;
    }

    /**
     * Returns a {@code ClassPath} representing all classes and resources loadable from {@code
     * classloader} and its parent class loaders.
     *
     * <p><b>Warning:</b> Currently only {@link URLClassLoader} and only {@code file://} urls are
     * supported.
     *
     * @throws IOException if the attempt to read class path resources (jar files or directories)
     *     failed.
     */
    public static ClassPath from(ClassLoader classloader) throws IOException {
      DefaultScanner scanner = new DefaultScanner();
      scanner.scan(classloader);
      return new ClassPath(scanner.getResources());
    }

    /**
     * Returns all resources loadable from the current class path, including the class files of all
     * loadable classes but excluding the "META-INF/MANIFEST.MF" file.
     */
    public ImmutableSet<ResourceInfo> getResources() {
      return resources;
    }

    /**
     * Returns all classes loadable from the current class path.
     *
     * @since 16.0
     */
    public ImmutableSet<ClassInfo> getAllClasses() {
      return FluentIterable.from(resources).filter(ClassInfo.class).toSet();
    }

    /** Returns all top level classes loadable from the current class path. */
    public ImmutableSet<ClassInfo> getTopLevelClasses() {
      return FluentIterable.from(resources).filter(ClassInfo.class).filter(IS_TOP_LEVEL).toSet();
    }

    /** Returns all top level classes whose package name is {@code packageName}. */
    public ImmutableSet<ClassInfo> getTopLevelClasses(String packageName) {
      checkNotNull(packageName);
      ImmutableSet.Builder<ClassInfo> builder = ImmutableSet.builder();
      for (ClassInfo classInfo : getTopLevelClasses()) {
        if (classInfo.getPackageName().equals(packageName)) {
          builder.add(classInfo);
        }
      }
      return builder.build();
    }

    /**
     * Returns all top level classes whose package name is {@code packageName} or starts with
     * {@code packageName} followed by a '.'.
     */
    public ImmutableSet<ClassInfo> getTopLevelClassesRecursive(String packageName) {
      checkNotNull(packageName);
      String packagePrefix = packageName + '.';
      ImmutableSet.Builder<ClassInfo> builder = ImmutableSet.builder();
      for (ClassInfo classInfo : getTopLevelClasses()) {
        if (classInfo.getName().startsWith(packagePrefix)) {
          builder.add(classInfo);
        }
      }
      return builder.build();
    }

    /**
     * Represents a class path resource that can be either a class file or any other resource file
     * loadable from the class path.
     *
     * @since 14.0
     */
    @Beta
    public static class ResourceInfo {

      private final String resourceName;

      final ClassLoader loader;

      static ResourceInfo of(String resourceName, ClassLoader loader) {
        if (resourceName.endsWith(CLASS_FILE_NAME_EXTENSION)) {
          return new ClassInfo(resourceName, loader);
        } else {
          return new ResourceInfo(resourceName, loader);
        }
      }

      ResourceInfo(String resourceName, ClassLoader loader) {
        this.resourceName = checkNotNull(resourceName);
        this.loader = checkNotNull(loader);
      }

      /**
       * Returns the url identifying the resource.
       *
       * <p>See {@link ClassLoader#getResource}
       *
       * @throws NoSuchElementException if the resource cannot be loaded through the class loader,
       *     despite physically existing in the class path.
       */
      public final URL url() {
        URL url = loader.getResource(resourceName);
        if (url == null) {
          throw new NoSuchElementException(resourceName);
        }
        return url;
      }

      /**
       * Returns a {@link ByteSource} view of the resource from which its bytes can be read.
       *
       * @throws NoSuchElementException if the resource cannot be loaded through the class loader,
       *     despite physically existing in the class path.
       * @since 20.0
       */
      public final ByteSource asByteSource() {
        return Resources.asByteSource(url());
      }

      /**
       * Returns a {@link CharSource} view of the resource from which its bytes can be read as
       * characters decoded with the given {@code charset}.
       *
       * @throws NoSuchElementException if the resource cannot be loaded through the class loader,
       *     despite physically existing in the class path.
       * @since 20.0
       */
      public final CharSource asCharSource(Charset charset) {
        return Resources.asCharSource(url(), charset);
      }

      /** Returns the fully qualified name of the resource. Such as "com/mycomp/foo/bar.txt". */
      public final String getResourceName() {
        return resourceName;
      }

      @Override
      public int hashCode() {
        return resourceName.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof ResourceInfo) {
          ResourceInfo that = (ResourceInfo) obj;
          return resourceName.equals(that.resourceName) && loader == that.loader;
        }
        return false;
      }

      // Do not change this arbitrarily. We rely on it for sorting ResourceInfo.
      @Override
      public String toString() {
        return resourceName;
      }
    }

    /**
     * Represents a class that can be loaded through {@link #load}.
     *
     * @since 14.0
     */
    @Beta
    private static final class ClassInfo extends ResourceInfo {

      private final String className;

      ClassInfo(String resourceName, ClassLoader loader) {
        super(resourceName, loader);
        this.className = getClassName(resourceName);
      }

      /**
       * Returns the package name of the class, without attempting to load the class.
       *
       * <p>Behaves identically to {@link Package#getName()} but does not require the class (or
       * package) to be loaded.
       */
      public String getPackageName() {
        return Reflection.getPackageName(className);
      }

      /**
       * Returns the simple name of the underlying class as given in the source code.
       *
       * <p>Behaves identically to {@link Class#getSimpleName()} but does not require the class
       * to be
       * loaded.
       */
      public String getSimpleName() {
        int lastDollarSign = className.lastIndexOf('$');
        if (lastDollarSign != -1) {
          String innerClassName = className.substring(lastDollarSign + 1);
          // local and anonymous classes are prefixed with number (1,2,3...), anonymous classes are
          // entirely numeric whereas local classes have the user supplied name as a suffix
          return CharMatcher.digit().trimLeadingFrom(innerClassName);
        }
        String packageName = getPackageName();
        if (packageName.isEmpty()) {
          return className;
        }

        // Since this is a top level class, its simple name is always the part after package name.
        return className.substring(packageName.length() + 1);
      }

      /**
       * Returns the fully qualified name of the class.
       *
       * <p>Behaves identically to {@link Class#getName()} but does not require the class to be
       * loaded.
       */
      public String getName() {
        return className;
      }

      /**
       * Loads (but doesn't link or initialize) the class.
       *
       * @throws LinkageError when there were errors in loading classes that this class depends on.
       *     For example, {@link NoClassDefFoundError}.
       */
      public Class<?> load() {
        try {
          return loader.loadClass(className);
        } catch (ClassNotFoundException e) {
          // Shouldn't happen, since the class name is read from the class path.
          throw new IllegalStateException(e);
        }
      }

      @Override
      public String toString() {
        return className;
      }
    }

    /**
     * Abstract class that scans through the class path represented by a {@link ClassLoader} and
     * calls
     * {@link #scanDirectory} and {@link #scanJarFile} for directories and jar files on the class
     * path
     * respectively.
     */
    abstract static class Scanner {

      // We only scan each file once independent of the classloader that resource might be
      // associated
      // with.
      private final Set<File> scannedUris = Sets.newHashSet();

      public final void scan(ClassLoader classloader) throws IOException {
        for (Map.Entry<File, ClassLoader> entry : getClassPathEntries(classloader).entrySet()) {
          scan(entry.getKey(), entry.getValue());
        }
      }

      /** Called when a directory is scanned for resource files. */
      protected abstract void scanDirectory(ClassLoader loader, File directory) throws IOException;

      /** Called when a jar file is scanned for resource entries. */
      protected abstract void scanJarFile(ClassLoader loader, JarFile file) throws IOException;

      @VisibleForTesting
      final void scan(File file, ClassLoader classloader) throws IOException {
        if (scannedUris.add(file.getCanonicalFile())) {
          scanFrom(file, classloader);
        }
      }

      private void scanFrom(File file, ClassLoader classloader) throws IOException {
        try {
          if (!file.exists()) {
            return;
          }
        } catch (SecurityException e) {
          logger.warn("Cannot access " + file + ": " + e);
          return;
        }
        if (file.isDirectory()) {
          scanDirectory(classloader, file);
        } else {
          scanJar(file, classloader);
        }
      }

      private void scanJar(File file, ClassLoader classloader) throws IOException {
        JarFile jarFile;
        try {
          jarFile = new JarFile(file);
        } catch (IOException e) {
          // Not a jar file
          return;
        }
        try {
          for (File path : getClassPathFromManifest(file, jarFile.getManifest())) {
            scan(path, classloader);
          }
          scanJarFile(classloader, jarFile);
        } finally {
          try {
            jarFile.close();
          } catch (IOException ignored) {
          }
        }
      }

      /**
       * Returns the class path URIs specified by the {@code Class-Path} manifest attribute,
       * according
       * to
       * <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Main_Attributes">
       * JAR File Specification</a>. If {@code manifest} is null, it means the jar file has no
       * manifest, and an empty set will be returned.
       */
      @VisibleForTesting
      static ImmutableSet<File> getClassPathFromManifest(File jarFile,
                                                         @Nullable Manifest manifest) {
        if (manifest == null) {
          return ImmutableSet.of();
        }
        ImmutableSet.Builder<File> builder = ImmutableSet.builder();
        String classpathAttribute =
            manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH.toString());
        if (classpathAttribute != null) {
          for (String path : CLASS_PATH_ATTRIBUTE_SEPARATOR.split(classpathAttribute)) {
            URL url;
            try {
              url = getClassPathEntry(jarFile, path);
            } catch (MalformedURLException e) {
              // Ignore bad entry
              logger.warn("Invalid Class-Path entry: " + path);
              continue;
            }
            if (url.getProtocol().equals("file")) {
              builder.add(toFile(url));
            }
          }
        }
        return builder.build();
      }

      @VisibleForTesting
      static ImmutableMap<File, ClassLoader> getClassPathEntries(ClassLoader classloader) {
        LinkedHashMap<File, ClassLoader> entries = Maps.newLinkedHashMap();
        // Search parent first, since it's the order ClassLoader#loadClass() uses.
        ClassLoader parent = classloader.getParent();
        if (parent != null) {
          entries.putAll(getClassPathEntries(parent));
        }
        if (classloader instanceof URLClassLoader) {
          URLClassLoader urlClassLoader = (URLClassLoader) classloader;
          for (URL entry : urlClassLoader.getURLs()) {
            if (entry.getProtocol().equals("file")) {
              File file = toFile(entry);
              if (!entries.containsKey(file)) {
                entries.put(file, classloader);
              }
            }
          }
        }
        return ImmutableMap.copyOf(entries);
      }

      /**
       * Returns the absolute uri of the Class-Path entry value as specified in
       * <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Main_Attributes">
       * JAR File Specification</a>. Even though the specification only talks about relative urls,
       * absolute urls are actually supported too (for example, in Maven surefire plugin).
       */
      @VisibleForTesting
      static URL getClassPathEntry(File jarFile, String path) throws MalformedURLException {
        return new URL(jarFile.toURI().toURL(), path);
      }
    }

    @VisibleForTesting
    static final class DefaultScanner extends Scanner {

      private final SetMultimap<ClassLoader, String> resources =
          MultimapBuilder.hashKeys().linkedHashSetValues().build();

      ImmutableSet<ResourceInfo> getResources() {
        ImmutableSet.Builder<ResourceInfo> builder = ImmutableSet.builder();
        for (Map.Entry<ClassLoader, String> entry : resources.entries()) {
          builder.add(ResourceInfo.of(entry.getValue(), entry.getKey()));
        }
        return builder.build();
      }

      @Override
      protected void scanJarFile(ClassLoader classloader, JarFile file) {
        Enumeration<JarEntry> entries = file.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          if (entry.isDirectory() || entry.getName().equals(JarFile.MANIFEST_NAME)) {
            continue;
          }
          resources.get(classloader).add(entry.getName());
        }
      }

      @Override
      protected void scanDirectory(ClassLoader classloader, File directory) throws IOException {
        scanDirectory(directory, classloader, "");
      }

      private void scanDirectory(File directory, ClassLoader classloader, String packagePrefix)
          throws IOException {
        File[] files = directory.listFiles();
        if (files == null) {
          logger.warn("Cannot read directory " + directory);
          // IO error, just skip the directory
          return;
        }
        for (File f : files) {
          String name = f.getName();
          if (f.isDirectory()) {
            scanDirectory(f, classloader, packagePrefix + name + "/");
          } else {
            String resourceName = packagePrefix + name;
            if (!resourceName.equals(JarFile.MANIFEST_NAME)) {
              resources.get(classloader).add(resourceName);
            }
          }
        }
      }
    }

    @VisibleForTesting
    static String getClassName(String filename) {
      int classNameEnd = filename.length() - CLASS_FILE_NAME_EXTENSION.length();
      return filename.substring(0, classNameEnd).replace('/', '.');
    }

    @VisibleForTesting
    static File toFile(URL url) {
      checkArgument(url.getProtocol().equals("file"));
      try {
        return new File(url.toURI());  // Accepts escaped characters like %20.
      } catch (URISyntaxException e) {  // URL.toURI() doesn't escape chars.
        return new File(url.getPath());  // Accepts non-escaped chars like space.
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ApiSurface.class);

  /** A factory method to create a {@link Class} matcher for classes residing in a given package. */
  public static Matcher<Class<?>> classesInPackage(final String packageName) {
    return new Matchers.ClassInPackage(packageName);
  }

  /**
   * A factory method to create an {@link ApiSurface} matcher, producing a positive match if the
   * queried api surface contains ONLY classes described by the provided matchers.
   */
  public static Matcher<ApiSurface> containsOnlyClassesMatching(
      final Set<Matcher<Class<?>>> classMatchers) {
    return new Matchers.ClassesInSurfaceMatcher(classMatchers);
  }

  /** See {@link ApiSurface#containsOnlyClassesMatching(Set)}. */
  @SafeVarargs
  public static Matcher<ApiSurface> containsOnlyClassesMatching(
      final Matcher<Class<?>>... classMatchers) {
    return new Matchers.ClassesInSurfaceMatcher(Sets.newHashSet(classMatchers));
  }

  /** See {@link ApiSurface#containsOnlyPackages(Set)}. */
  public static Matcher<ApiSurface> containsOnlyPackages(final String... packageNames) {
    return containsOnlyPackages(Sets.newHashSet(packageNames));
  }

  /**
   * A factory method to create an {@link ApiSurface} matcher, producing a positive match if the
   * queried api surface contains classes ONLY from specified package names.
   */
  public static Matcher<ApiSurface> containsOnlyPackages(final Set<String> packageNames) {

    final Function<String, Matcher<Class<?>>> packageNameToClassMatcher =
        new Function<String, Matcher<Class<?>>>() {

          @Override
          public Matcher<Class<?>> apply(@Nonnull final String packageName) {
            return classesInPackage(packageName);
          }
        };

    final ImmutableSet<Matcher<Class<?>>> classesInPackages =
        FluentIterable.from(packageNames).transform(packageNameToClassMatcher).toSet();

    return containsOnlyClassesMatching(classesInPackages);
  }

  /**
   * {@link Matcher}s for use in {@link ApiSurface} related tests that aim to keep the public API
   * conformant to a hard-coded policy by controlling what classes are allowed to be exposed by an
   * API surface.
   */
  // based on previous code by @kennknowles and others.
  private static class Matchers {

    private static class ClassInPackage extends TypeSafeDiagnosingMatcher<Class<?>> {

      private final String packageName;

      private ClassInPackage(final String packageName) {
        this.packageName = packageName;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("Classes in package \"");
        description.appendText(packageName);
        description.appendText("\"");
      }

      @Override
      protected boolean matchesSafely(final Class<?> clazz, final Description mismatchDescription) {
        return clazz.getName().startsWith(packageName + ".");
      }
    }

    private static class ClassesInSurfaceMatcher extends TypeSafeDiagnosingMatcher<ApiSurface> {

      private final Set<Matcher<Class<?>>> classMatchers;

      private ClassesInSurfaceMatcher(final Set<Matcher<Class<?>>> classMatchers) {
        this.classMatchers = classMatchers;
      }

      private boolean verifyNoAbandoned(
          final ApiSurface checkedApiSurface,
          final Set<Matcher<Class<?>>> allowedClasses,
          final Description mismatchDescription) {

        // <helper_lambdas>

        final Function<Matcher<Class<?>>, String> toMessage =
            new Function<Matcher<Class<?>>, String>() {

              @Override
              public String apply(@Nonnull final Matcher<Class<?>> abandonedClassMacther) {
                final StringDescription description = new StringDescription();
                description.appendText("No ");
                abandonedClassMacther.describeTo(description);
                return description.toString();
              }
            };

        final Predicate<Matcher<Class<?>>> matchedByExposedClasses =
            new Predicate<Matcher<Class<?>>>() {

              @Override
              public boolean apply(@Nonnull final Matcher<Class<?>> classMatcher) {
                return FluentIterable.from(checkedApiSurface.getExposedClasses())
                                     .anyMatch(
                                         new Predicate<Class<?>>() {

                                           @Override
                                           public boolean apply(@Nonnull final Class<?> aClass) {
                                             return classMatcher.matches(aClass);
                                           }
                                         });
              }
            };

        // </helper_lambdas>

        final ImmutableSet<Matcher<Class<?>>> matchedClassMatchers =
            FluentIterable.from(allowedClasses).filter(matchedByExposedClasses).toSet();

        final Sets.SetView<Matcher<Class<?>>> abandonedClassMatchers =
            Sets.difference(allowedClasses, matchedClassMatchers);

        final ImmutableList<String> messages =
            FluentIterable.from(abandonedClassMatchers)
                          .transform(toMessage)
                          .toSortedList(Ordering.<String>natural());

        if (!messages.isEmpty()) {
          mismatchDescription.appendText(
              "The following white-listed scopes did not have matching classes on the API surface:"
                  + "\n\t"
                  + Joiner.on("\n\t").join(messages));
        }

        return messages.isEmpty();
      }

      private boolean verifyNoDisallowed(
          final ApiSurface checkedApiSurface,
          final Set<Matcher<Class<?>>> allowedClasses,
          final Description mismatchDescription) {

        /* <helper_lambdas> */

        final Function<Class<?>, List<Class<?>>> toExposure =
            new Function<Class<?>, List<Class<?>>>() {

              @Override
              public List<Class<?>> apply(@Nonnull final Class<?> aClass) {
                return checkedApiSurface.getAnyExposurePath(aClass);
              }
            };

        final Maps.EntryTransformer<Class<?>, List<Class<?>>, String> toMessage =
            new Maps.EntryTransformer<Class<?>, List<Class<?>>, String>() {

              @Override
              public String transformEntry(
                  @Nonnull final Class<?> aClass, @Nonnull final List<Class<?>> exposure) {
                return aClass + " exposed via:\n\t\t" + Joiner.on("\n\t\t").join(exposure);
              }
            };

        final Predicate<Class<?>> disallowed =
            new Predicate<Class<?>>() {

              @Override
              public boolean apply(@Nonnull final Class<?> aClass) {
                return !classIsAllowed(aClass, allowedClasses);
              }
            };

        /* </helper_lambdas> */

        final FluentIterable<Class<?>> disallowedClasses =
            FluentIterable.from(checkedApiSurface.getExposedClasses()).filter(disallowed);

        final ImmutableMap<Class<?>, List<Class<?>>> exposures =
            Maps.toMap(disallowedClasses, toExposure);

        final ImmutableList<String> messages =
            FluentIterable.from(Maps.transformEntries(exposures, toMessage).values())
                          .toSortedList(Ordering.<String>natural());

        if (!messages.isEmpty()) {
          mismatchDescription.appendText(
              "The following disallowed classes appeared on the API surface:\n\t"
                  + Joiner.on("\n\t").join(messages));
        }

        return messages.isEmpty();
      }

      @SuppressWarnings({ "rawtypes", "unchecked" })
      private boolean classIsAllowed(
          final Class<?> clazz, final Set<Matcher<Class<?>>> allowedClasses) {
        // Safe cast inexpressible in Java without rawtypes
        return anyOf((Iterable) allowedClasses).matches(clazz);
      }

      @Override
      protected boolean matchesSafely(
          final ApiSurface apiSurface, final Description mismatchDescription) {
        final boolean noDisallowed =
            verifyNoDisallowed(apiSurface, classMatchers, mismatchDescription);

        final boolean noAbandoned =
            verifyNoAbandoned(apiSurface, classMatchers, mismatchDescription);

        return noDisallowed & noAbandoned;
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText("API surface to include only:" + "\n\t");
        for (final Matcher<Class<?>> classMatcher : classMatchers) {
          classMatcher.describeTo(description);
          description.appendText("\n\t");
        }
      }
    }
  }

  ///////////////

  /** Returns an empty {@link ApiSurface}. */
  public static ApiSurface empty() {
    LOG.debug("Returning an empty ApiSurface");
    return new ApiSurface(Collections.<Class<?>>emptySet(), Collections.<Pattern>emptySet());
  }

  /** Returns an {@link ApiSurface} object representing the given package and all subpackages. */
  public static ApiSurface ofPackage(String packageName, ClassLoader classLoader)
      throws IOException {
    return ApiSurface.empty().includingPackage(packageName, classLoader);
  }

  /** Returns an {@link ApiSurface} object representing the given package and all subpackages. */
  public static ApiSurface ofPackage(Package aPackage, ClassLoader classLoader) throws IOException {
    return ofPackage(aPackage.getName(), classLoader);
  }

  /** Returns an {@link ApiSurface} object representing just the surface of the given class. */
  public static ApiSurface ofClass(Class<?> clazz) {
    return ApiSurface.empty().includingClass(clazz);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but also including the named package and all of
   * its subpackages.
   */
  public ApiSurface includingPackage(String packageName, ClassLoader classLoader)
      throws IOException {
    ClassPath classPath = ClassPath.from(classLoader);

    Set<Class<?>> newRootClasses = Sets.newHashSet();
    for (ClassPath.ClassInfo classInfo : classPath.getTopLevelClassesRecursive(packageName)) {
      Class clazz = classInfo.load();
      if (exposed(clazz.getModifiers())) {
        newRootClasses.add(clazz);
      }
    }
    LOG.debug("Including package {} and subpackages: {}", packageName, newRootClasses);
    newRootClasses.addAll(rootClasses);

    return new ApiSurface(newRootClasses, patternsToPrune);
  }

  /** Returns an {@link ApiSurface} like this one, but also including the given class. */
  public ApiSurface includingClass(Class<?> clazz) {
    Set<Class<?>> newRootClasses = Sets.newHashSet();
    LOG.debug("Including class {}", clazz);
    newRootClasses.add(clazz);
    newRootClasses.addAll(rootClasses);
    return new ApiSurface(newRootClasses, patternsToPrune);
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning transitive references from classes
   * whose full name (including package) begins with the provided prefix.
   */
  public ApiSurface pruningPrefix(String prefix) {
    return pruningPattern(Pattern.compile(Pattern.quote(prefix) + ".*"));
  }

  /** Returns an {@link ApiSurface} like this one, but pruning references from the named class. */
  public ApiSurface pruningClassName(String className) {
    return pruningPattern(Pattern.compile(Pattern.quote(className)));
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning references from the provided class.
   */
  public ApiSurface pruningClass(Class<?> clazz) {
    return pruningClassName(clazz.getName());
  }

  /**
   * Returns an {@link ApiSurface} like this one, but pruning transitive references from classes
   * whose full name (including package) begins with the provided prefix.
   */
  public ApiSurface pruningPattern(Pattern pattern) {
    Set<Pattern> newPatterns = Sets.newHashSet();
    newPatterns.addAll(patternsToPrune);
    newPatterns.add(pattern);
    return new ApiSurface(rootClasses, newPatterns);
  }

  /** See {@link #pruningPattern(Pattern)}. */
  public ApiSurface pruningPattern(String patternString) {
    return pruningPattern(Pattern.compile(patternString));
  }

  /** Returns all public classes originally belonging to the package in the {@link ApiSurface}. */
  public Set<Class<?>> getRootClasses() {
    return rootClasses;
  }

  /** Returns exposed types in this set, including arrays and primitives as specified. */
  public Set<Class<?>> getExposedClasses() {
    return getExposedToExposers().keySet();
  }

  /**
   * Returns a path from an exposed class to a root class. There may be many, but this gives only
   * one.
   *
   * <p>If there are only cycles, with no path back to a root class, throws IllegalStateException.
   */
  public List<Class<?>> getAnyExposurePath(Class<?> exposedClass) {
    Set<Class<?>> excluded = Sets.newHashSet();
    excluded.add(exposedClass);
    List<Class<?>> path = getAnyExposurePath(exposedClass, excluded);
    if (path == null) {
      throw new IllegalArgumentException(
          "Class "
              + exposedClass
              + " has no path back to any root class."
              + " It should never have been considered exposed.");
    } else {
      return path;
    }
  }

  /**
   * Returns a path from an exposed class to a root class. There may be many, but this gives only
   * one. It will not return a path that crosses the excluded classes.
   *
   * <p>If there are only cycles or paths through the excluded classes, returns null.
   *
   * <p>If the class is not actually in the exposure map, throws IllegalArgumentException
   */
  private List<Class<?>> getAnyExposurePath(Class<?> exposedClass, Set<Class<?>> excluded) {
    List<Class<?>> exposurePath = Lists.newArrayList();
    exposurePath.add(exposedClass);

    Collection<Class<?>> exposers = getExposedToExposers().get(exposedClass);
    if (exposers.isEmpty()) {
      throw new IllegalArgumentException("Class " + exposedClass + " is not exposed.");
    }

    for (Class<?> exposer : exposers) {
      if (excluded.contains(exposer)) {
        continue;
      }

      // A null exposer means this is already a root class.
      if (exposer == null) {
        return exposurePath;
      }

      List<Class<?>> restOfPath =
          getAnyExposurePath(exposer, Sets.union(excluded, Sets.newHashSet(exposer)));

      if (restOfPath != null) {
        exposurePath.addAll(restOfPath);
        return exposurePath;
      }
    }
    return null;
  }

  ////////////////////////////////////////////////////////////////////

  // Fields initialized upon construction
  private final Set<Class<?>> rootClasses;
  private final Set<Pattern> patternsToPrune;

  // Fields computed on-demand
  private Multimap<Class<?>, Class<?>> exposedToExposers = null;
  private Pattern prunedPattern = null;
  private Set<Type> visited = null;

  private ApiSurface(Set<Class<?>> rootClasses, Set<Pattern> patternsToPrune) {
    this.rootClasses = rootClasses;
    this.patternsToPrune = patternsToPrune;
  }

  /**
   * A map from exposed types to place where they are exposed, in the sense of being a part of a
   * public-facing API surface.
   *
   * <p>This map is the adjencency list representation of a directed graph, where an edge from type
   * {@code T1} to type {@code T2} indicates that {@code T2} directly exposes {@code T1} in its API
   * surface.
   *
   * <p>The traversal methods in this class are designed to avoid repeatedly processing types, since
   * there will almost always be cyclic references.
   */
  private Multimap<Class<?>, Class<?>> getExposedToExposers() {
    if (exposedToExposers == null) {
      constructExposedToExposers();
    }
    return exposedToExposers;
  }

  /** See {@link #getExposedToExposers}. */
  private void constructExposedToExposers() {
    visited = Sets.newHashSet();
    exposedToExposers =
        Multimaps.newSetMultimap(
            Maps.<Class<?>, Collection<Class<?>>>newHashMap(),
            new Supplier<Set<Class<?>>>() {

              @Override
              public Set<Class<?>> get() {
                return Sets.newHashSet();
              }
            });

    for (Class<?> clazz : rootClasses) {
      addExposedTypes(clazz, null);
    }
  }

  /** A combined {@code Pattern} that implements all the pruning specified. */
  private Pattern getPrunedPattern() {
    if (prunedPattern == null) {
      constructPrunedPattern();
    }
    return prunedPattern;
  }

  /** See {@link #getPrunedPattern}. */
  private void constructPrunedPattern() {
    Set<String> prunedPatternStrings = Sets.newHashSet();
    for (Pattern patternToPrune : patternsToPrune) {
      prunedPatternStrings.add(patternToPrune.pattern());
    }
    prunedPattern = Pattern.compile("(" + Joiner.on(")|(").join(prunedPatternStrings) + ")");
  }

  /** Whether a type and all that it references should be pruned from the graph. */
  private boolean pruned(Type type) {
    return pruned(TypeToken.of(type).getRawType());
  }

  /** Whether a class and all that it references should be pruned from the graph. */
  private boolean pruned(Class<?> clazz) {
    return clazz.isPrimitive()
        || clazz.isArray()
        || getPrunedPattern().matcher(clazz.getName()).matches();
  }

  /** Whether a type has already beens sufficiently processed. */
  private boolean done(Type type) {
    return visited.contains(type);
  }

  private void recordExposure(Class<?> exposed, Class<?> cause) {
    exposedToExposers.put(exposed, cause);
  }

  private void recordExposure(Type exposed, Class<?> cause) {
    exposedToExposers.put(TypeToken.of(exposed).getRawType(), cause);
  }

  private void visit(Type type) {
    visited.add(type);
  }

  /** See {@link #addExposedTypes(Type, Class)}. */
  private void addExposedTypes(TypeToken type, Class<?> cause) {
    LOG.debug(
        "Adding exposed types from {}, which is the type in type token {}", type.getType(), type);
    addExposedTypes(type.getType(), cause);
  }

  /**
   * Adds any references learned by following a link from {@code cause} to {@code type}. This will
   * dispatch according to the concrete {@code Type} implementation. See the other overloads of
   * {@code addExposedTypes} for their details.
   */
  private void addExposedTypes(Type type, Class<?> cause) {
    if (type instanceof TypeVariable) {
      LOG.debug("Adding exposed types from {}, which is a type variable", type);
      addExposedTypes((TypeVariable) type, cause);
    } else if (type instanceof WildcardType) {
      LOG.debug("Adding exposed types from {}, which is a wildcard type", type);
      addExposedTypes((WildcardType) type, cause);
    } else if (type instanceof GenericArrayType) {
      LOG.debug("Adding exposed types from {}, which is a generic array type", type);
      addExposedTypes((GenericArrayType) type, cause);
    } else if (type instanceof ParameterizedType) {
      LOG.debug("Adding exposed types from {}, which is a parameterized type", type);
      addExposedTypes((ParameterizedType) type, cause);
    } else if (type instanceof Class) {
      LOG.debug("Adding exposed types from {}, which is a class", type);
      addExposedTypes((Class) type, cause);
    } else {
      throw new IllegalArgumentException("Unknown implementation of Type");
    }
  }

  /**
   * Adds any types exposed to this set. These will come from the (possibly absent) bounds on the
   * type variable.
   */
  private void addExposedTypes(TypeVariable type, Class<?> cause) {
    if (done(type)) {
      return;
    }
    visit(type);
    for (Type bound : type.getBounds()) {
      LOG.debug("Adding exposed types from {}, which is a type bound on {}", bound, type);
      addExposedTypes(bound, cause);
    }
  }

  /**
   * Adds any types exposed to this set. These will come from the (possibly absent) bounds on the
   * wildcard.
   */
  private void addExposedTypes(WildcardType type, Class<?> cause) {
    visit(type);
    for (Type lowerBound : type.getLowerBounds()) {
      LOG.debug(
          "Adding exposed types from {}, which is a type lower bound on wildcard type {}",
          lowerBound,
          type);
      addExposedTypes(lowerBound, cause);
    }
    for (Type upperBound : type.getUpperBounds()) {
      LOG.debug(
          "Adding exposed types from {}, which is a type upper bound on wildcard type {}",
          upperBound,
          type);
      addExposedTypes(upperBound, cause);
    }
  }

  /**
   * Adds any types exposed from the given array type. The array type itself is not added. The cause
   * of the exposure of the underlying type is considered whatever type exposed the array type.
   */
  private void addExposedTypes(GenericArrayType type, Class<?> cause) {
    if (done(type)) {
      return;
    }
    visit(type);
    LOG.debug(
        "Adding exposed types from {}, which is the component type on generic array type {}",
        type.getGenericComponentType(),
        type);
    addExposedTypes(type.getGenericComponentType(), cause);
  }

  /**
   * Adds any types exposed to this set. Even if the root type is to be pruned, the actual type
   * arguments are processed.
   */
  private void addExposedTypes(ParameterizedType type, Class<?> cause) {
    // Even if the type is already done, this link to it may be new
    boolean alreadyDone = done(type);
    if (!pruned(type)) {
      visit(type);
      recordExposure(type, cause);
    }
    if (alreadyDone) {
      return;
    }

    // For a parameterized type, pruning does not take place
    // here, only for the raw class.
    // The type parameters themselves may not be pruned,
    // for example with List<MyApiType> probably the
    // standard List is pruned, but MyApiType is not.
    LOG.debug(
        "Adding exposed types from {}, which is the raw type on parameterized type {}",
        type.getRawType(),
        type);
    addExposedTypes(type.getRawType(), cause);
    for (Type typeArg : type.getActualTypeArguments()) {
      LOG.debug(
          "Adding exposed types from {}, which is a type argument on parameterized type {}",
          typeArg,
          type);
      addExposedTypes(typeArg, cause);
    }
  }

  /**
   * Adds a class and all of the types it exposes. The cause of the class being exposed is given,
   * and the cause of everything within the class is that class itself.
   */
  private void addExposedTypes(Class<?> clazz, Class<?> cause) {
    if (pruned(clazz)) {
      return;
    }
    // Even if `clazz` has been visited, the link from `cause` may be new
    boolean alreadyDone = done(clazz);
    visit(clazz);
    recordExposure(clazz, cause);
    if (alreadyDone || pruned(clazz)) {
      return;
    }

    TypeToken<?> token = TypeToken.of(clazz);
    for (TypeToken<?> superType : token.getTypes()) {
      if (!superType.equals(token)) {
        LOG.debug(
            "Adding exposed types from {}, which is a super type token on {}", superType, clazz);
        addExposedTypes(superType, clazz);
      }
    }
    for (Class innerClass : clazz.getDeclaredClasses()) {
      if (exposed(innerClass.getModifiers())) {
        LOG.debug(
            "Adding exposed types from {}, which is an exposed inner class of {}",
            innerClass,
            clazz);
        addExposedTypes(innerClass, clazz);
      }
    }
    for (Field field : clazz.getDeclaredFields()) {
      if (exposed(field.getModifiers())) {
        LOG.debug("Adding exposed types from {}, which is an exposed field on {}", field, clazz);
        addExposedTypes(field, clazz);
      }
    }
    for (Invokable invokable : getExposedInvokables(token)) {
      LOG.debug(
          "Adding exposed types from {}, which is an exposed invokable on {}", invokable, clazz);
      addExposedTypes(invokable, clazz);
    }
  }

  private void addExposedTypes(Invokable<?, ?> invokable, Class<?> cause) {
    addExposedTypes(invokable.getReturnType(), cause);
    for (Annotation annotation : invokable.getAnnotations()) {
      LOG.debug(
          "Adding exposed types from {}, which is an annotation on invokable {}",
          annotation,
          invokable);
      addExposedTypes(annotation.annotationType(), cause);
    }
    for (Parameter parameter : invokable.getParameters()) {
      LOG.debug(
          "Adding exposed types from {}, which is a parameter on invokable {}",
          parameter,
          invokable);
      addExposedTypes(parameter, cause);
    }
    for (TypeToken<?> exceptionType : invokable.getExceptionTypes()) {
      LOG.debug(
          "Adding exposed types from {}, which is an exception type on invokable {}",
          exceptionType,
          invokable);
      addExposedTypes(exceptionType, cause);
    }
  }

  private void addExposedTypes(Parameter parameter, Class<?> cause) {
    LOG.debug(
        "Adding exposed types from {}, which is the type of parameter {}",
        parameter.getType(),
        parameter);
    addExposedTypes(parameter.getType(), cause);
    for (Annotation annotation : parameter.getAnnotations()) {
      LOG.debug(
          "Adding exposed types from {}, which is an annotation on parameter {}",
          annotation,
          parameter);
      addExposedTypes(annotation.annotationType(), cause);
    }
  }

  private void addExposedTypes(Field field, Class<?> cause) {
    addExposedTypes(field.getGenericType(), cause);
    for (Annotation annotation : field.getDeclaredAnnotations()) {
      LOG.debug(
          "Adding exposed types from {}, which is an annotation on field {}", annotation, field);
      addExposedTypes(annotation.annotationType(), cause);
    }
  }

  /** Returns an {@link Invokable} for each public methods or constructors of a type. */
  private Set<Invokable> getExposedInvokables(TypeToken<?> type) {
    Set<Invokable> invokables = Sets.newHashSet();

    for (Constructor constructor : type.getRawType().getConstructors()) {
      if (0 != (constructor.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED))) {
        invokables.add(type.constructor(constructor));
      }
    }

    for (Method method : type.getRawType().getMethods()) {
      if (0 != (method.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED))) {
        invokables.add(type.method(method));
      }
    }

    return invokables;
  }

  /** Returns true of the given modifier bitmap indicates exposure (public or protected access). */
  private boolean exposed(int modifiers) {
    return 0 != (modifiers & (Modifier.PUBLIC | Modifier.PROTECTED));
  }

  ////////////////////////////////////////////////////////////////////////////

  /**
   * All classes transitively reachable via only public method signatures of the SDK.
   *
   * <p>Note that our idea of "public" does not include various internal-only APIs.
   */
  public static ApiSurface getSdkApiSurface(final ClassLoader classLoader) throws IOException {
    return ApiSurface.ofPackage("org.apache.beam", classLoader)
                     .pruningPattern("org[.]apache[.]beam[.].*Test")
                     // Exposes Guava, but not intended for users
                     .pruningClassName("org.apache.beam.sdk.util.common.ReflectHelpers")
                     .pruningPrefix("java");
  }
}
