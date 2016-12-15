package org.apache.beam.runners.spark.translation;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPCollectionView implements Serializable {

	// Holds the view --> broadcast mapping. Transient so it will be null from resume
	private transient Map<PCollectionView<?>, BroadcastHelper> broadcastHelperMap = null;

	// Holds the Actual data of the views in serialize form
	private Map<PCollectionView<?>,
			    Pair<byte[], Coder<Iterable<WindowedValue<?>>>>> pviews = new LinkedHashMap<>();

	// Driver only - during evaluation stage
	void putPView(PCollectionView<?> view,
			      Iterable<WindowedValue<?>> value,
			      Coder<Iterable<WindowedValue<?>>> coder) {

		pviews.put(view, Pair.of(CoderHelpers.toByteArray(value, coder), coder));
	}

	// Driver only
	BroadcastHelper getPCollectionView(PCollectionView<?> view, JavaSparkContext context) {
		// initialize broadcastHelperMap if needed
		if (broadcastHelperMap == null) {
			synchronized (SparkPCollectionView.class) {
				if (broadcastHelperMap == null) {
					broadcastHelperMap = new LinkedHashMap<>();
				}
			}
		}

		//lazily broadcast views
		BroadcastHelper helper = broadcastHelperMap.get(view);
		if (helper == null) {
			synchronized (SparkPCollectionView.class) {
				helper = broadcastHelperMap.get(view);
				if (helper == null) {
					Pair<byte[], Coder<Iterable<WindowedValue<?>>>> pair = pviews.get(view);
					helper = BroadcastHelper.create(pair.getKey(), pair.getValue());
					helper.broadcast(context);
					broadcastHelperMap.put(view,helper);
				}
			}
		}
		return helper;
	}
}
