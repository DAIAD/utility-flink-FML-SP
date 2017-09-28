package savingspotential.transformations;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import tools.ExecConf;

public class CalculateSP implements GroupReduceFunction<Tuple5<String, String, String, String, String>, Tuple3<String, String,
String>> {

private static final long serialVersionUID = 1L;
ExecConf conf;

public CalculateSP(ExecConf conf) {
	this.conf = conf;
}

@Override
public void reduce(Iterable<Tuple5<String, String, String, String, String>> input, Collector<Tuple3<String, String, String>>
	output)
	throws Exception {

		// Read the data to an in-memory list
		LinkedList<Tuple5<String, String, String, String, String>> dataList = new LinkedList<>();
		Iterator<Tuple5<String, String, String, String, String>> iterator = input.iterator();
		while (iterator.hasNext()) {
			dataList.add(iterator.next());
		}
		
		Double diffFromAvg = 0.0;
		String cluster = null;
		int month = 0;
		double savingsPotential = 0.0;
		for (Tuple5<String, String, String, String, String> record : dataList) {
			month = Integer.parseInt(record.f0);
			cluster = record.f1;
			diffFromAvg = Double.parseDouble(record.f4);
			if (diffFromAvg > 0) {
				savingsPotential += diffFromAvg;
			}
		}
		
		output.collect(new Tuple3<>(Integer.toString(month), cluster, Double.toString(savingsPotential)));
	}
}