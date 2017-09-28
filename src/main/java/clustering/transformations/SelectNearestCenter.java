package clustering.transformations;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.ExecConf;
import tools.Functions;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Determines the closest cluster center for a data point.
 */
public class SelectNearestCenter extends RichGroupReduceFunction<Tuple2<String, Element>, Tuple3<String, String,
		Element>> {

	private static final long serialVersionUID = 1L;
	private Collection<Tuple2<String, Element>> centroids;
	private ExecConf conf;

	public SelectNearestCenter(ExecConf conf) {
		this.conf = conf;
	}

	/**
	 * Reads the centroid values from a broadcast variable into a collection.
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.centroids = getRuntimeContext().getBroadcastVariable("tsCentroids");
	}

	@Override
	public void reduce(Iterable<Tuple2<String, Element>> input, Collector<Tuple3<String, String, Element>> output)
			throws Exception {

		// Read the data to an in-memory list
		LinkedList<Tuple2<String, Element>> dataList = new LinkedList<>();
		Iterator<Tuple2<String, Element>> iterator = input.iterator();
		// Remove header
		iterator.next();
		while (iterator.hasNext()) {
			dataList.add(iterator.next());
		}

		for (Tuple2<String, Element> record : dataList) {
			double minDistance = Double.MAX_VALUE;
			String closestCentroidId = "";

			// check all cluster centers
			for (Tuple2<String, Element> centroid : centroids) {
				if (centroid.f0.equals(record.f0)) {
					// compute distance
					double distance = 0;
					if (conf.getDistanceMetric() == 1) {
						distance = Functions.calculateEuclideanDistance(record.f1, centroid.f1);
					} else if (conf.getDistanceMetric() == 2) {
						distance = Functions.calculateDTWDistance(record.f1, centroid.f1);
					} else {
						System.out.println("Error: Wrong distance metric number. Check arguments");
						System.exit(0);
					}

					// update nearest cluster if necessary
					if (distance < minDistance) {
						minDistance = distance;
						closestCentroidId = centroid.f1.id;
					}
				}
			}

			// emit a new record with the center id and the data point.
			output.collect(new Tuple3<String, String, Element>(record.f0, closestCentroidId, record.f1));
		}

	}
}
