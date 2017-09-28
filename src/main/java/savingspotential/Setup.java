package savingspotential;

import tools.ExecConf;

public class Setup {

	public static void main(String[] args) throws Exception {

		// Create the configuration
		ExecConf conf = new ExecConf();

		if (args.length > 0) {
			try {
				conf.setNumIterations(Integer.parseInt(args[0]));
				conf.setNoOfClusters(Integer.parseInt(args[1]));
				conf.setDistanceMetric(Integer.parseInt(args[2]));
				conf.setExecutePath(args[3]);
				conf.setInputFilePath(args[4]);
			} catch (Exception e) {
				System.out.println("Error! Please check arguments.");
				System.exit(0);
			}
		} else {
			System.out.println("Executing K-Means example with default parameters.");
			System.out.println("Usage: Setup <num iterations> <num clusters> <distance metric>");
		}

		SavingsPotential savingsPotential = new SavingsPotential(conf);
		savingsPotential.execute();
	}
}