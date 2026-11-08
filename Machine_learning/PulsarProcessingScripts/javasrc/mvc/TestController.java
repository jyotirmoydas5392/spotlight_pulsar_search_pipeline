/**
 *
 * This file is part of AutomatedTreeTester.
 *
 * AutomatedTreeTester is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AutomatedTreeTester is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AutomatedTreeTester.  If not, see <http://www.gnu.org/licenses/>.
 *
 * File name: 	TestController.java
 * Package: cs.man.ac.uk.mvc
 * Created:	February 19th, 2014
 * Author:	Rob Lyon
 * 
 * Contact:	rob@scienceguyrob.com or robert.lyon@cs.man.ac.uk
 * Web:		<http://www.scienceguyrob.com> or <http://www.cs.manchester.ac.uk> 
 *          or <http://www.jb.man.ac.uk>
 */
package cs.man.ac.uk.mvc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Random;
import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.trees.J48;
import cs.man.ac.uk.classifiers.ClassifierStatistics;
import cs.man.ac.uk.common.Common;
import cs.man.ac.uk.io.Reader;
import cs.man.ac.uk.io.Writer;

/**
 * The class TestController is responsible for executing tests.
 *
 * @author Rob Lyon
 *
 * @version 1.0, 19/02/14
 */
public class TestController
{
	//*****************************************
	//*****************************************
	//              Variables
	//*****************************************
	//*****************************************

	/**
	 * The name of the classifier.
	 */
	private String name = "J48";

	/**
	 * Path to the ARFF file containing the data to classify.
	 */
	private String dataPath = "";

	/**
	 * Path to the file to write the results of the classification to.
	 */
	private String outputPath = "";

	/**
	 * Path to the directory in which we should look for images.
	 */
	private String imageSearchPath = "";

	//*****************************************
	//*****************************************
	//             Constructor
	//*****************************************
	//*****************************************

	/**
	 * Default constructor.
	 */
	public TestController(String dataPath,String outputPath,String imageSearchPath)
	{ 
		this.dataPath=dataPath;
		this.outputPath=outputPath;
		this.imageSearchPath=imageSearchPath;
	}

	//*****************************************
	//*****************************************
	//              Methods
	//*****************************************
	//*****************************************

	/**
	 * Executes the test.
	 * @return true if the tests are executed successfully, else false.
	 */
	private boolean exexuteTests()
	{
		// Actually run the test.
		testStatic(this.dataPath,this.outputPath);

		/*
		 * Once the test is complete, process the data ready for use. This is not straight forward
		 * since when weka classifies data, it is very hard to determine which candidates
		 * were incorrectly classified. For example, if you classify 10,000 candidates,
		 * WEKA will report a level of classification accuracy. Lets say 250 of these 10,000
		 * were classified incorrectly, you obviously want to know which candidates make
		 * up the 250. WEKA does not provide this information!!!!!!!! WEKA may tell you
		 * that candidate 10 was incorrectly classified, but when using cross validation - which
		 * randomly samples candidates - there's no way to know which candidate that is! So the
		 * testStatic(String path,String out) method below executes the first step in solving this
		 * problem. When a candidate is incorrectly classified, it obtains the data belonging.
		 * to that candidate.
		 * 
		 * However this data needs to be cross referenced in order to determine which candidate
		 * it belongs to. This is done below. The scores are cross referenced, and a new
		 * output file created that identifies: 1) the path to the PNG image for a candidate,
		 * 2) the candidate's scores, 3) its true class label, and 4) the reason it was misclassified i.e.
		 * FP = false positive or FN = false negative.
		 * 
		 */

		// Prepare new ARFF file to write misclassified instances to.
		String arffPath=this.outputPath.substring(0,this.outputPath.lastIndexOf("."))+".arff";
		prepareARFF(arffPath);
		
		// Obtain data from original ARFF file.
		String arffContent=Reader.getContents(this.dataPath);
		String[] arffLines = arffContent.split("\r");

		// Now extract the data from the output file produced by this app,
		// then store the data in a hashmap. This contains the scores of those
		// candidates which were incorrectly classified.
		String outputContent=Reader.getContents(this.outputPath);
		String[] outputLines = outputContent.split("\r");
		Common.fileDelete(this.outputPath);//No longer needed - data will be written to it again shortly though.
		HashMap<String,String> map = new HashMap<String,String>();

		for(int i=0;i<outputLines.length;i++)
		{
			String line = outputLines[i];
			// If it was a false positive, store this information.
			if(line.contains("FP"))
				map.put(line.replace(",FP",",0"), "FP");
			// If it was a false negative, store this information.
			else if(line.contains("FN"))
				map.put(line.replace(",FN",",1"), "FN");
		}
		
		// Now cross reference data from the output file with the original ARFF file.
		// I know this seems completely crazy and pointless, but I'm doing this since
		// WEKA does not give detailed information on misclassified instances. It will
		// take to long to explain here why this is a problem, but trust me, all this
		// ridiculous code is necessary.

		
		// Find all image files in the directory specified by the user.
		File[] files=Common.getFilesRecursive(new File(this.imageSearchPath));
		HashMap<String,String> imageFileMap = new HashMap<String,String>();
		for(int i=0;i<files.length;i++)
		{
			String file = files[i].toString();

			if(file.endsWith(".png"))
			{
				String filename=file.substring(file.lastIndexOf("/")+1,file.length());
				imageFileMap.put(filename, file);
			}
		}
		
		
		// NOW CROSS REFERENCE.
		String currentCandidate="";

		for(int i=0;i<arffLines.length;i++)
		{
			String line = arffLines[i];

			if(line.startsWith("@"))
				continue;
			else if(line.startsWith("%"))
				currentCandidate=line.replace("%", "").trim();
			else // Data line. 
			{
				if(map.containsKey(line))
				{
					String error = map.get(line);
					String filename =currentCandidate.substring(currentCandidate.lastIndexOf("/")+1,currentCandidate.length());
					String pngFileName =filename.replace(".dat", "")+".png";
					
					if(imageFileMap.containsKey(pngFileName))
					{
						String imgPath=imageFileMap.get(pngFileName);
						
						String output=imgPath+","+line+","+error+"\n";
						Writer.append(this.outputPath, output);
						Writer.append(arffPath,"%"+filename+"\n"+line+ ","+ error +"\n");
					}
				}
			}

		}

		return true;
	}


	/**
	 * Performs 10-fold cross validation on the ARFF file specified by the supplied
	 * path. Writes out all mistakes made during classification.
	 * @param path the path to the ARFF file.
	 * @param out the path to the output file where mistakes will be written to.
	 * @return true if classified successfully, else false.
	 */
	@SuppressWarnings("unused")
	public boolean testStatic(String path,String out)
	{
		System.out.println("Testing data on "+name);

		try
		{
			// Test meta information and important variables.
			int correctPositiveClassifications = 0;
			int correctNegativeClassifications = 0;

			ClassifierStatistics stats = new ClassifierStatistics();

			// Prepare data for testing
			BufferedReader reader = new BufferedReader( new FileReader(path));
			Instances data = new Instances(reader);
			data.setClassIndex(data.numAttributes() - 1);

			System.out.println(name+ " Classifier is ready.");
			System.out.println(name+" Testing on all instances avaialable.");
			System.out.println("Test set instances: "+data.numInstances());

			int runs  = 1;
			int folds = 10;

			long startTime = System.nanoTime();

			// perform cross-validation
			for (int i = 0; i < runs; i++) 
			{
				// randomize data
				int seed = i + 1;
				Random rand = new Random(seed);
				Instances randData = new Instances(data);
				randData.randomize(rand);

				if (randData.classAttribute().isNominal())
					randData.stratify(folds);


				for (int n = 0; n < folds; n++) 
				{
					Instances train = randData.trainCV(folds, n);
					Instances test = randData.testCV(folds, n);

					// build and evaluate classifier
					J48 learner=new J48();
					learner.buildClassifier(train);


					for (int j = 0; j < test.numInstances(); j++) 
					{
						double classification = learner.classifyInstance(test.instance(j));
						String instanceClass= Double.toString(test.instance(j).classValue());


						if(classification==1 && instanceClass.startsWith("0"))// Predicted positive, actually negative
						{	
							stats.incrementFP();
							Writer.append(out, getScores(test.instance(j))+"FP\n");
						}
						else if(classification==1 && instanceClass.startsWith("1"))// Predicted positive, actually positive
						{
							correctPositiveClassifications+=1;
							stats.incrementTP();
						}
						else if(classification==0 && instanceClass.startsWith("1"))// Predicted negative, actually positive
						{	
							stats.incrementFN();
							Writer.append(out, getScores(test.instance(j))+"FN\n");
						}
						else if(classification==0 && instanceClass.startsWith("0"))// Predicted negative, actually negative
						{
							correctNegativeClassifications+=1;
							stats.incrementTN();
						}	
					}
				}
			}

			long endTime = System.nanoTime();
			long duration = endTime - startTime;
			double seconds = (double) duration / 1000000000.0;

			stats.calculate();

			System.out.println("Testing "+name+" completed in "+duration+" (ns) or "+seconds+" (s)");
			System.out.println(name+" Performance");
			System.out.println(stats.toString());

			return true;
		}
		catch (Exception e) { System.out.println("Could not test " +name+ " classifier due to an error\n"+e.toString()); return false; }
	}

	/**
	 * Obtains the scores belonging to a instance within the ARFF
	 * file being classified. Returns these scores in a comma delimited
	 * string.
	 * @param instance the instance whose score values should be obtained. 
	 * @return a comma delimited string containing the scores.
	 */
	private String getScores(Instance instance)
	{
		String data="";
		for(int i=0;i<22;i++)
			data+=instance.value(i)+",";

		return data;
	}
	
	/**
	 * Prepares an ARFF file.
	 * @param path the path at which to create/prepare the ARFF file.
	 */
	private void prepareARFF(String path)
	{
		String header = "@relation LabelledPulsarCandidates\n";

		for(int i=1;i < 23;i++)
		{
			header += "@attribute Score";
			header += i;
			header += " numeric\n";
		}

		header += "@attribute class {0,1}\n@attribute reason {FN,FP}\n@data\n";

		Writer.append(path, header);
	}
	//*****************************************
	//*****************************************
	//        Initialisation Methods
	//*****************************************
	//*****************************************

	/**
	 * Initialises the model and the user interface.
	 * @return true if the initialisation was successful, else false.
	 */
	public boolean initialise()
	{
		System.out.println("Initialising Controller");
		return exexuteTests();
	}

	/**
	 * Exits the application.
	 */
	public void exit()
	{
		System.out.println("Exiting");
		System.exit(0);
	}
}
