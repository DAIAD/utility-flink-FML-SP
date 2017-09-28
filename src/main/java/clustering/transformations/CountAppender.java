package clustering.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import tools.Element;

/**
 * Appends a count variable to the tuple.
 */
public class CountAppender implements MapFunction<Tuple3<String, String, Element>, Tuple4<String, String, Element,
		Long>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple4<String, String, Element, Long> map(Tuple3<String, String, Element> t) {
		return new Tuple4<>(t.f0, t.f1, t.f2, 1L);
	}
}