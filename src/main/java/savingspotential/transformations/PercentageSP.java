package savingspotential.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import tools.ExecConf;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.LinkedList;

public class PercentageSP implements GroupReduceFunction<Tuple3<String, String, String>, Tuple4<String, String,
		String, String>> {

	private static final long serialVersionUID = 1L;
	ExecConf conf;

	public PercentageSP(ExecConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<Tuple3<String, String, String>> input, Collector<Tuple4<String, String,
			String, String>> output)
			throws Exception {

		// Read the data to an in-memory list
		LinkedList<Tuple3<String, String, String>> dataList = new LinkedList<>();
		Iterator<Tuple3<String, String, String>> iterator = input.iterator();
		while (iterator.hasNext()) {
			dataList.add(iterator.next());
		}

		double totalPerMonth = 0.0;
		for (Tuple3<String, String, String> record : dataList) {
			totalPerMonth += Double.parseDouble(record.f2);
		}

		Double percentageSP;
		DecimalFormat df = new DecimalFormat("0.00##");
		for (Tuple3<String, String, String> record : dataList) {
			percentageSP = Double.parseDouble(record.f2) / totalPerMonth;
			output.collect(new Tuple4<>(record.f1, record.f0, df.format(Double.parseDouble(record.f2)), df.format(percentageSP*100)+"%"));
		}
	}
}
