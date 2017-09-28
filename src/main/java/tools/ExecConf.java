package tools;

import java.io.Serializable;

public class ExecConf implements Serializable {

	private static final long serialVersionUID = 1L;
	private int distanceMetric = 2; // 1 for Euclidean distance, 2, for DTW
	//private int basis = 1; // The basis on which the algorithm will run (1 = monthly, 2 =  ...)
	private int numIterations = 200;
	private int noOfClusters = 5;
	private String executePath = "";
	private String inputFilePath = "";

	public ExecConf() {}

	public int getNumIterations() {
		return numIterations;
	}

	public void setNumIterations(int numIterations) {
		this.numIterations = numIterations;
	}

	public int getNoOfClusters() {
		return noOfClusters;
	}

	public void setNoOfClusters(int noOfClusters) {
		this.noOfClusters = noOfClusters;
	}

	public int getDistanceMetric() {
		return distanceMetric;
	}

	public void setDistanceMetric(int distanceMetric) {
		this.distanceMetric = distanceMetric;
	}


	public String getExecutePath() {
		return executePath;
	}


	public void setExecutePath(String executePath) {
		this.executePath = executePath;
	}

	public String getInputFilePath() {
		return inputFilePath;
	}

	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}
}