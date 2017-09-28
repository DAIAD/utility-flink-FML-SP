package savingspotential;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import clustering.KMeansClustering;
import savingspotential.transformations.AvgDailyTS;
import savingspotential.transformations.AvgWeeklyTS;
import savingspotential.transformations.CalculateSP;
import savingspotential.transformations.CalculateWaterIQ;
import savingspotential.transformations.PercentageSP;
import tools.Element;
import tools.ExecConf;

public class SavingsPotential {

	private ExecConf conf;

	public SavingsPotential(ExecConf conf) {
		this.conf = conf;
	}

	public void execute() throws Exception {

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Read the values as an element: string 1: household id, string 2[0]: timestamp, string 2[1]: consumption
		@SuppressWarnings("deprecation")
		DataSet<Tuple4<String, String, String, String>> dataset = env
				.readCsvFile(conf.getInputFilePath())
				.fieldDelimiter(';')
				.types(String.class, String.class, String.class, String.class);

		// Initially, for each month, gather the mean per day of week consumption ....
		// Group by household_id
		// Returns <household_id, month, day_of_week, hour, per_month_avg_hourly_cons>
		DataSet<Tuple5<String, String, String, String, String>> avgPerDay = dataset.groupBy(0).reduceGroup(new
				AvgDailyTS(conf));

		// ...then concatenate the lists to create the average weekly consumption time-series for each user
		// Group by household_id
		// Returns <month, element_representing_avg_weekly_TS>
		DataSet<Tuple2<String, Element>> tsDataset = avgPerDay.groupBy(0).reduceGroup(new AvgWeeklyTS(conf));

		KMeansClustering clust = new KMeansClustering(conf);
		// Group by month
		// Returns <month, cluster_id, clustered_element>
		DataSet<Tuple3<String, String, Element>> clusteredElements = clust.kMeans(tsDataset);

		// Write the clustering result
		//clusteredElements.writeAsCsv(conf.getExecutePath() + "ClusteringResults", "\n", " ",
		//		FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// Calculate the WaterIQ score
		// Group by cluster and month
		DataSet<Tuple5<String, String, String, String, String>> waterIQ = clusteredElements.groupBy(0,1).reduceGroup(new
				CalculateWaterIQ(conf));
		
		// Write the final WaterIQ score result
		waterIQ.writeAsCsv(conf.getExecutePath() + "WaterIQ", "\n", ";",
				FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// Calculate the per cluster water savings potential
		// Group by cluster
		DataSet<Tuple3<String, String, String>> savingsPotential = waterIQ.groupBy(0,1).reduceGroup(new
				CalculateSP(conf));

		// Calculate the final savings potential percentage
		DataSet<Tuple4<String, String, String, String>> finalSP = savingsPotential.groupBy(0).reduceGroup(new
				PercentageSP
				(conf));

		// Write the final savingsPotential result
		finalSP.writeAsCsv(conf.getExecutePath() + "SavingsPotential", "\n", ";",
				FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		// execute program
		long startExecuteTime = System.currentTimeMillis();
		env.execute("SavingsPotential");
		long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;
		
		// Delete residual files
		Path pt = new Path(conf.getExecutePath() + "monthlyCons/");
		pt.getFileSystem().delete(pt, true);

		/******** Wall-clock execution time and clustering evaluation ********/
		int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
		int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60;
		int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000 * 60)) % 60);
		int ExecuteHours = (int) ((totalElapsedExecuteTime / (1000 * 60 * 60)) % 24);
		System.out.println("Savings Potential total time: " + ExecuteHours + "h " + ExecuteMinutes
				+ "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

		/**
		 String finalCentroidFilename = conf.getDataPath() + "FinalCentroids";
		 double daviesBouldin = Functions.calculateDaviesBouldin(conf, finalCentroidFilename);
		 System.out.println("Clustering Davies Bouldin Index: " + daviesBouldin);
		 **/
	}

}
