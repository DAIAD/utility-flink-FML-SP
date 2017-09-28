package savingspotential.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.ExecConf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class AvgWeeklyTS implements GroupReduceFunction<Tuple5<String, String, String, String, String>,
		Tuple2<String, Element>> {

	private static final long serialVersionUID = 1L;

	public AvgWeeklyTS(ExecConf conf) {
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void reduce(Iterable<Tuple5<String, String, String, String, String>> input, Collector<Tuple2<String,
			Element>> output)
			throws
			Exception {

		// Read the data to an in-memory list
		LinkedList<Tuple5<String, String, String, String, String>> dataList = new LinkedList<>();
		Iterator<Tuple5<String, String, String, String, String>> iterator = input.iterator();
		while (iterator.hasNext()) {
			dataList.add(iterator.next());
		}

		// For each month, calculate the average daily consumption in the form of time series.
		String user = "";
		HashMap<Integer, HashMap> perMonthAvgWeeklyTS = new HashMap<Integer, HashMap>();
		HashMap<Integer, HashMap> perMontHhourCount = new HashMap<Integer, HashMap>();
		HashMap<Integer, Double[]> avgWeeklyTS = null;
		HashMap<Integer, int[]> hourCount = null;
		for (Tuple5<String, String, String, String, String> record : dataList) {
			avgWeeklyTS = perMonthAvgWeeklyTS.get(Integer.parseInt(record.f1));
			hourCount = perMontHhourCount.get(Integer.parseInt(record.f1));
			if (avgWeeklyTS == null) {
				avgWeeklyTS = new HashMap<>();
				hourCount = new HashMap<>();
				for (int i = 0; i < 7; i++) {
					Double[] tmp1 = new Double[24];
					int[] tmp2 = new int[24];
					for (int k = 0; k < 24; k++) {
						tmp1[k] = 0.0;
						tmp2[k] = 0;
					}
					avgWeeklyTS.put(i, tmp1);
					hourCount.put(i, tmp2);
				}
			}
			user = record.f0;
			int dayOfWeek = Integer.parseInt(record.f2);
			int hour = Integer.parseInt(record.f3);
			Double[] consValues = avgWeeklyTS.get(dayOfWeek);
			int[] countValues = hourCount.get(dayOfWeek);
			consValues[hour] += Double.parseDouble(record.f4);
			countValues[hour] += 1;
			avgWeeklyTS.put(dayOfWeek, consValues);
			hourCount.put(dayOfWeek, countValues);
			perMonthAvgWeeklyTS.put(Integer.parseInt(record.f1), avgWeeklyTS);
			perMontHhourCount.put(Integer.parseInt(record.f1), hourCount);
		}

		for (Integer key : perMonthAvgWeeklyTS.keySet()) {
			avgWeeklyTS = perMonthAvgWeeklyTS.get(key);
			hourCount = perMontHhourCount.get(key);
			Double[] values = new Double[7 * 24];
			int count = 0;
			for (int i = 0; i < 7; i++) {
				for (int k = 0; k < 24; k++) {
					values[count] = avgWeeklyTS.get(i)[k] / hourCount.get(i)[k];
					count++;
				}
			}

			Element element = new Element(user, values);
			output.collect(new Tuple2<>(Integer.toString(key), element));
		}
	}
}
