package tools;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import clustering.transformations.TupleConverter;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class Functions {

	public static void generateCentroidFile(ExecConf conf, String datasetFilename, String centroidFilename) throws
			IOException {
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(datasetFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		List<String> inputList = new ArrayList<>();
		List<String> centroidList = new ArrayList<>();

		String line;
		while ((line = br.readLine()) != null) {
			inputList.add(line);
		}

		Random r = new Random();
		int prevRand = r.nextInt(inputList.size()), rand;
		for (int i = 0; i < conf.getNoOfClusters(); i++) {
			rand = r.nextInt(inputList.size());
			if (rand != prevRand) {
				centroidList.add(inputList.get(r.nextInt(inputList.size())));
			}
			prevRand = rand;
		}

		FileWriter writer = new FileWriter(centroidFilename);
		for (String str : centroidList) {
			writer.write(str + "\n");
		}
		writer.close();
		br.close();
	}

	@SuppressWarnings("deprecation")
	public static DataSet<Element> getTimeSeriesDataSet(ExecutionEnvironment env, String
			filename) throws IOException {

		return env.readCsvFile(filename)
				.fieldDelimiter(',')
				.types(String.class, String.class)
				.map(new TupleConverter());
	}

	public static double calculateDTWDistance(Element e1, Element e2) {
		DynamicTimeWarping dtw = new DynamicTimeWarping(e1.getValues(), e2.getValues());
		return dtw.getDistance();
	}

	public static double calculateEuclideanDistance(Element e1, Element e2) {
		Double[] values1 = e1.getValues();
		Double[] values2 = e2.getValues();
		double dist = 0;
		for (int i = 0; i < values1.length; i++) {
			dist += (values1[i] - values2[i]) * (values1[i] - values2[i]);
		}
		return Math.sqrt(dist);
	}

	/**
	 * Method that implements the Davies Bouldin Index metric in order to determine the quality of the clustering
	 */
	public static double calculateDaviesBouldin(ExecConf conf, String centroidFilename) throws IOException {
		Path pt = new Path(centroidFilename);
		FileSystem fs = pt.getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Double[]> centroids = new HashMap<>();

		String line;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split(",");
			String[] strValues = parts[1].split(" ");
			Double[] values = new Double[strValues.length];
			for (int i = 0; i < strValues.length; i++) {
				values[i] = Double.parseDouble(strValues[i]);
			}
			centroids.put(parts[0], values);
		}

		String lineResult;
		BufferedReader resultBr = null;
		HashMap<String, String> clusters = new HashMap<>();
		try {
			pt = new Path(conf.getExecutePath() + "ClusteringResults");
			fs = pt.getFileSystem();
			resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		} catch (Exception e) {
			System.out.println("Result file not found!");
			System.exit(0);
		}

		while ((lineResult = resultBr.readLine()) != null) {
			String[] parts = lineResult.split(",");
			String centroidID = parts[0].split(" ")[0];
			String elementID = parts[0].split(" ")[1];
			clusters.put(centroidID, elementID + "," + parts[1] + "\n");
		}

		resultBr.close();

		double totalValue = 0;
		for (String centroidID : centroids.keySet()) {

			double theValue = 0;
			Double[] centroidValues = centroids.get(centroidID);
			String clusterElements = clusters.get(centroidID);
			String[] elements = clusterElements.split("\n");
			double totalDistance = 0;
			Element outerCentroid = null;
			for (int i = 0; i < elements.length; i++) {
				String[] parts = elements[i].split(",");
				String[] strValues = parts[1].split(" ");
				Double[] elementValues = new Double[strValues.length];
				for (int j = 0; j < strValues.length; j++) {
					elementValues[j] = Double.parseDouble(strValues[j]);
				}
				outerCentroid = new Element(centroidID, centroidValues);
				Element element = new Element(parts[0], elementValues);
				totalDistance += calculateEuclideanDistance(outerCentroid, element);
			}
			double sigmaI = totalDistance / elements.length;

			for (String cId : centroids.keySet()) {
				if (cId.equals(centroidID)) continue;

				centroidValues = centroids.get(cId);
				clusterElements = clusters.get(cId);
				try {
					elements = clusterElements.split("\n");
				} catch (Exception e) {
					System.out.println();
				}
				totalDistance = 0;
				Element innerCentroid = null;
				for (int i = 0; i < elements.length; i++) {
					String[] parts = elements[i].split(",");
					String[] strValues = parts[1].split(" ");
					Double[] elementValues = new Double[strValues.length];
					for (int j = 0; j < strValues.length; j++) {
						elementValues[j] = Double.parseDouble(strValues[j]);
					}
					innerCentroid = new Element(cId, centroidValues);
					Element element = new Element(parts[0], elementValues);
					totalDistance += calculateEuclideanDistance(innerCentroid, element);
				}
				double sigmaJ = totalDistance / elements.length;
				double centroidDistance = calculateEuclideanDistance(outerCentroid, innerCentroid);

				double innerValue = (sigmaI + sigmaJ) / centroidDistance;

				if (innerValue > theValue) theValue = innerValue;
			}

			totalValue += theValue;
		}

		return totalValue / centroids.size();
	}

	/**
	 * Get the day of week given a date
	 */
	public static int getDayOfWeek(int year, int month, int day) {
		int[] offset = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

		int afterFeb = 1;
		if (month > 2) afterFeb = 0;
		int aux = year - 1700 - afterFeb;

		// dayOfWeek for 1700 / 1 / 1 = 5, Friday
		int day_of_week = 5;

		// partial sum of days betweem current date and 1700 / 1 / 1
		day_of_week += (aux + afterFeb) * 365;

		//leap year correction
		day_of_week += aux / 4 - aux / 100 + (aux + 100) / 400;

		//sum monthly and day offsets
		day_of_week += offset[month - 1] + (day - 1);
		day_of_week %= 7;
		return day_of_week;
	}
}