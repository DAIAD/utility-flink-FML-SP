package clustering.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import tools.Element;

/**
 * Converts a Tuple2<Double,Double> into a Point.
 */
public class TupleConverter implements MapFunction<Tuple2<String, String>, Element> {

	private static final long serialVersionUID = 1L;

	@Override
	public Element map(Tuple2<String, String> t) throws Exception {
		return new Element(t.f0, t.f1);
	}
}
