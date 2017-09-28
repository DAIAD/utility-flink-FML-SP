package savingspotential.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import tools.Element;
import tools.ExecConf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class CalculateWaterIQ implements GroupReduceFunction<Tuple3<String, String, Element>, Tuple5<String,
		String, String, String, String>> {

	private static final long serialVersionUID = 1L;
	ExecConf conf;

	public CalculateWaterIQ(ExecConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<Tuple3<String, String, Element>> input, Collector<Tuple5<String, String, String, String, String>>
			output)
			throws Exception {

		// Read the data to an in-memory list
		LinkedList<Tuple3<String, String, Element>> dataList = new LinkedList<>();
		Iterator<Tuple3<String, String, Element>> iterator = input.iterator();
		while (iterator.hasNext()) {
			dataList.add(iterator.next());
		}

		String line;
		HashMap<String, Double> perUserMonthly = new HashMap<>();
		Double averageMonthly = 0.0;
		String cluster = null;
		int month = 0;
		Path pt;
		FileSystem fs;
		BufferedReader br;
		for (Tuple3<String, String, Element> record : dataList) {
			cluster = record.f1;
			month = Integer.parseInt(record.f0);

			// Read the file with user
			pt = new Path(conf.getExecutePath()  + "monthlyCons/" + record.f2.id);
			fs = pt.getFileSystem();
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			Double perUsertotalMonthCons;
			double tmp = 0.0;
			perUserMonthly.put(record.f2.id, tmp);
			while ((line = br.readLine()) != null) {
				String[] parts = line.split(",");
				if (month == Integer.parseInt(parts[0])) {
					perUsertotalMonthCons = Double.parseDouble(parts[1]);
					averageMonthly += perUsertotalMonthCons;
					tmp = perUserMonthly.get(record.f2.id);
					tmp += perUsertotalMonthCons;
					perUserMonthly.put(record.f2.id, tmp);
				}
			}
		}

		// Calculate WaterIQ score. Keep the necessary data and pass them to the next transformation for the 
		// caclulation of the per cluster water savings potential
		averageMonthly = averageMonthly / dataList.size();
		HashMap<String, Double> perUserDiffFromAvg = new HashMap<>();
		double minDiff=0, maxDiff=0;
		
		for (String user : perUserMonthly.keySet()) {
			double diffFromAvg = perUserMonthly.get(user) - averageMonthly;
			perUserDiffFromAvg.put(user, diffFromAvg);
			if (diffFromAvg < minDiff) minDiff = diffFromAvg;
			if (diffFromAvg > maxDiff) maxDiff = diffFromAvg;
		}
		
		double binInterval = (double)(maxDiff-minDiff)/perUserMonthly.size();
		DecimalFormat df = new DecimalFormat("0.00##");
		for (String user : perUserDiffFromAvg.keySet()) {
			double diffFromAvg = perUserDiffFromAvg.get(user);
			if (diffFromAvg <= minDiff+binInterval) {
				output.collect(new Tuple5<>(Integer.toString(month), cluster, user, "A", df.format(diffFromAvg)));
			}
			else if (diffFromAvg <= minDiff+(2*binInterval)){
				output.collect(new Tuple5<>(Integer.toString(month), cluster, user, "B", df.format(diffFromAvg)));
			}
			else if (diffFromAvg <= minDiff+(3*binInterval)){
				output.collect(new Tuple5<>(Integer.toString(month), cluster, user, "C", df.format(diffFromAvg)));
			}
			else if (diffFromAvg <= minDiff+(4*binInterval)){
				output.collect(new Tuple5<>(Integer.toString(month), cluster, user, "D", df.format(diffFromAvg)));
			}
			else if (diffFromAvg <= minDiff+(5*binInterval)){
				output.collect(new Tuple5<>(Integer.toString(month), cluster, user, "E", df.format(diffFromAvg)));
			}
			else {
				output.collect(new Tuple5<>(Integer.toString(month), cluster, user, "F", df.format(diffFromAvg)));
			}
		}
	}
}
