package clustering.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import tools.Element;

/**
 * Sums and counts point coordinates.
 */
public class CentroidAccumulator implements ReduceFunction<Tuple4<String, String, Element, Long>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple4<String, String, Element, Long> reduce(Tuple4<String, String, Element, Long> val1, Tuple4<String,
			String, Element, Long>
			val2) {
		return new Tuple4<>(val1.f0, val1.f1, val1.f2.add(val2.f2), val1.f3 + val2.f3);
	}
}
