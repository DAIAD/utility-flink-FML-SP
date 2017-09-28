package clustering.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.ExecConf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class InitialCentroidCalculator implements GroupReduceFunction<Tuple2<String, Element>, Tuple2<String,
		Element>> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;

	public InitialCentroidCalculator(ExecConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<Tuple2<String, Element>> input, Collector<Tuple2<String, Element>> output) throws
			Exception {

		// Read the data to an in-memory list
		List<Element> dataList = new ArrayList<>();
		Iterator<Tuple2<String, Element>> iterator = input.iterator();
		String month = "";
		while (iterator.hasNext()) {
			Tuple2<String, Element> item = iterator.next();
			month = item.f0;
			Element newItem = new Element(item.f1.id, item.f1.getValues());
			dataList.add(newItem);
		}

		Random r = new Random();
		int prevRand = r.nextInt(dataList.size()), rand;
		int clusterNo = 1;
		for (int i = 0; i < conf.getNoOfClusters(); i++) {
			rand = r.nextInt(dataList.size());
			if (rand != prevRand) {
				Element centroid = new Element("Cluster" + clusterNo, dataList.get(rand).getValues());
				output.collect(new Tuple2<>(month, centroid));
				clusterNo++;
			} else i--;
			prevRand = rand;
		}
	}
}
