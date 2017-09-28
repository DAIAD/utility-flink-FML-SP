package clustering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import clustering.transformations.*;
import tools.Element;
import tools.ExecConf;

public class KMeansClustering {

	private ExecConf conf;

	public KMeansClustering(ExecConf conf) {
		this.conf = conf;
	}

	public DataSet<Tuple3<String, String, Element>> kMeans(DataSet<Tuple2<String, Element>> tsDataset) throws
			Exception {

		Path fpt = new Path(conf.getExecutePath() + "ClusteringResults");
		FileSystem fs = fpt.getFileSystem();
		fs.delete(fpt, true);

		DataSet<Tuple2<String, Element>> tsCentroids = tsDataset.groupBy(0).reduceGroup(new InitialCentroidCalculator(conf));

		// set number of bulk iterations for Setup algorithm
		IterativeDataSet<Tuple2<String, Element>> loop = tsCentroids.iterate(conf.getNumIterations());

		DataSet<Tuple2<String, Element>> newCentroids = tsDataset.groupBy(0)
				// compute closest centroid for each point
				.reduceGroup(new SelectNearestCenter(conf)).withBroadcastSet(loop, "tsCentroids")
				// count and sum point coordinates for each centroid
				.map(new CountAppender())
				.groupBy(0,1).reduce(new CentroidAccumulator())
				// compute new centroids from point counts and coordinate sums
				.map(new CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Tuple2<String, Element>> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple3<String, String, Element>> clusteredElements = tsDataset.groupBy(0)
				// assign points to final clusters
				.reduceGroup(new SelectNearestCenter(conf)).withBroadcastSet(finalCentroids, "tsCentroids");

		// write result
		//finalCentroids.writeAsText(conf.getDataPath() + "FinalCentroids", FileSystem.WriteMode.OVERWRITE)
		//		.setParallelism(1);
		return clusteredElements;
	}
}