package org.apache.beam.sdk.io.common.retry;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * An interface used to control if retry call when a {@link Throwable} occurs. If {@link
 * RetryPredicate#test(Object)} returns true.
 */
@FunctionalInterface
public interface RetryPredicate extends Predicate<Throwable>, Serializable {}
