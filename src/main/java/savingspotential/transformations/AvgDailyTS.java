package savingspotential.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import tools.ExecConf;
import tools.Functions;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class AvgDailyTS implements GroupReduceFunction<Tuple4<String, String, String, String>, Tuple5<String, String,
		String, String, String>> {

	private static final long serialVersionUID = 1L;
	ExecConf conf;

	public AvgDailyTS(ExecConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<Tuple4<String, String, String, String>> input, Collector<Tuple5<String, String,
			String, String, String>> output) throws Exception {

		// Read the data to an in-memory list
		LinkedList<Tuple4<String, String, String, String>> dataList = new LinkedList<>();
		Iterator<Tuple4<String, String, String, String>> iterator = input.iterator();
		// Remove header
		iterator.next();
		while (iterator.hasNext()) {
			dataList.add(iterator.next());
		}

		// Initialize the hash map
		HashMap<Integer, Double[][]> perMonthDailyCons = new HashMap<>();
		HashMap<Integer, int[][]> perMonthDailyCount = new HashMap<>();
		for (int i = 0; i < 12; i++) {
			Double[][] tmp1 = new Double[7][24];
			int[][] tmp2 = new int[7][24];
			for (int j = 0; j < 7; j++) {
				for (int k = 0; k < 24; k++) {
					tmp1[j][k] = 0.0;
					tmp2[j][k] = 0;
				}
			}
			perMonthDailyCons.put(i + 1, tmp1);
			perMonthDailyCount.put(i + 1, tmp2);
		}

		// For each month, calculate the average daily consumption in the form of time series.
		String user = "";
		Double[][] consValues;
		int[][] countValues;
		double totalMonthly[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		for (Tuple4<String, String, String, String> record : dataList) {
			user = record.f0;
			String[] date = record.f1.split(" ");
			int year = Integer.parseInt(date[0].split("/")[2]);
			int month = Integer.parseInt(date[0].split("/")[1]);
			int day = Integer.parseInt(date[0].split("/")[0]);
			int hour = Integer.parseInt(date[1].split(":")[0]);
			int dayOfWeek = Functions.getDayOfWeek(year, month, day);
			consValues = perMonthDailyCons.get(month);
			countValues = perMonthDailyCount.get(month);
			consValues[dayOfWeek][hour] += Double.parseDouble(record.f3);
			countValues[dayOfWeek][hour] += 1;
			perMonthDailyCons.put(month, consValues);
			perMonthDailyCount.put(month, countValues);
			totalMonthly[month - 1] += Double.parseDouble(record.f3);
		}

		if (!user.equals("")) {
			Path pt = new Path(conf.getExecutePath() + "monthlyCons/" + user);
			FileSystem fs = pt.getFileSystem();
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			for (int i = 0; i < 12; i++) {
				if (totalMonthly[i] != 0)
					bw.append(i + 1 + "," + totalMonthly[i] + "\n");
			}
			bw.close();
		}

		for (int i = 0; i < 12; i++) {
			if (totalMonthly[i] == 0) continue; 
			for (int j = 0; j < 7; j++) {
				for (int k = 0; k < 24; k++) {
					if (user.equals("")) continue;
					// If there was no consumption ata ll during an hour, return zero
					if (perMonthDailyCount.get(i + 1)[j][k] == 0) {
						output.collect(new Tuple5<>(user, Integer.toString(i + 1), Integer.toString(j),
								Integer.toString(k), Double.toString(0.0)));
					}
					else {
						output.collect(new Tuple5<>(user, Integer.toString(i + 1), Integer.toString(j),
								Integer.toString(k), Double.toString(perMonthDailyCons.get(i + 1)[j][k]
								/ perMonthDailyCount.get(i + 1)[j][k])));
					}
				}
			}
		}
	}
}
