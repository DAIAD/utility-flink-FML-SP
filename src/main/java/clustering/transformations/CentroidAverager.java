package clustering.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import tools.Element;

/**
 * Computes new centroid from coordinate sum and count of points.
 */
public class CentroidAverager implements MapFunction<Tuple4<String, String, Element, Long>, Tuple2<String, Element>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, Element> map(Tuple4<String, String, Element, Long> value) {

		return new Tuple2<>(value.f0, new Element(value.f1, value.f2.div(value.f3)));
	}

}