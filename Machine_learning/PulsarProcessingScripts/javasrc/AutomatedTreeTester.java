/**
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
 * File name: 	AutomatedTreeTester.java
 * Package: 
 * Created:	February 19, 2014
 * Author:	Rob Lyon
 * 
 * Contact:	rob@scienceguyrob.com or robert.lyon@cs.man.ac.uk
 * Web:		<http://www.scienceguyrob.com> or <http://www.cs.manchester.ac.uk> 
 *          or <http://www.jb.man.ac.uk>
 */

import java.awt.EventQueue;
import javax.swing.UIManager;

import cs.man.ac.uk.cli.CLI;
import cs.man.ac.uk.cli.CLParameter;
import cs.man.ac.uk.common.Common;
import cs.man.ac.uk.mvc.TestController;

/**
 * This application is used to classify pulsar data, and determine which candidates
 * were incorrectly classified.
 * 
 * It is needed since when using weka to classify data, it is very hard to determine which
 * candidates were incorrectly classified. For example, if you classify 10,000 candidates,
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
 * In order to cross reference the data this program relies on being given an ARFF data file
 * containing the original information, such that each entry in the ARFF file is proceeded by
 * an ARFF comment containing the path to the candidate. For example (note the @ symbols have
 * been removed below):
 * 
 * relation LabelledPulsarCandidates
 * attribute Score1 numeric
 * attribute Score2 numeric
 * attribute Score3 numeric
 * attribute Score4 numeric
 * attribute Score5 numeric
 * attribute Score6 numeric
 * attribute Score7 numeric
 * attribute Score8 numeric
 * attribute Score9 numeric
 * attribute Score10 numeric
 * attribute Score11 numeric
 * attribute Score12 numeric
 * attribute Score13 numeric
 * attribute Score14 numeric
 * attribute Score15 numeric
 * attribute Score16 numeric
 * attribute Score17 numeric
 * attribute Score18 numeric
 * attribute Score19 numeric
 * attribute Score20 numeric
 * attribute Score21 numeric
 * attribute Score22 numeric
 * attribute class {0,1}
 * data
 * %/Users/rob/Documents/Aptana/PulsarProcessingScripts/test/2009-10-24-00:27:11.08.fil_sigproc_049.phcx.gz.dat
 * 198.646144443,275.167846338,11.0,1766.0,6.39159409357,0.980037368832,109.585651138,5.71288513321,1729.88773373
 * %/Users/rob/Documents/Aptana/PulsarProcessingScripts/test/2010-12-04-14:48:36.10.fil_sigproc_059.phcx.gz.dat
 * 122.90494339,211.907416543,13.0,1361.0,0.571157629653,0.999884610751,113.794173788,2.5873038022,1879.30546253,
 * %/Users/rob/Documents/Aptana/PulsarProcessingScripts/test/2010-01-23-01:09:11.04.fil_sigproc_099.phcx.gz.dat
 * 59.1886461161,96.8891221019,9.0,-3681.0,60.146670496,0.516788226645,167.880045315,15.1025819427,1320.56939416,
 * ...
 * ...
 * @author Rob Lyon
 *
 * @version 1.0, 05/01/13
 */
public class AutomatedTreeTester
{
	//*****************************************
	//*****************************************
	//              Variables
	//*****************************************
	//*****************************************

	/**
	 * Object responsible for dealing with command line parameters.
	 */
	private static CLI commandLineParser = new CLI();
	
	/**
	 * The main entry point to the application.
	 * @param args the command line arguments.
	 */
	public static void main(String[] args)
	{
		// OUTPUT APPLICATION INFO
		System.out.println("Runnung AutomatedTreeTester.\n");
		
		// Define Command line parameters to expect here:
		// -t is the path to the training set file.
		// -o is the path to the output file.
		commandLineParser.addParameter("-t", CLParameter.STRING_PARAM_TYPE);
		commandLineParser.addParameter("-o", CLParameter.STRING_PARAM_TYPE);
		commandLineParser.addParameter("-i", CLParameter.STRING_PARAM_TYPE);

		// Process any command line arguments.
		if (args != null)
		{		
			if (args.length > 0)
			{
				for(int i =0; i<args.length;i++)
					commandLineParser.processArgument(args[i]);
			}
		}

		// Write out parameters.
		System.out.println(commandLineParser.toString());
		
		
		if(commandLineParser.getParameter("-t") == null | 
		   commandLineParser.getParameter("-o") == null | 
		   commandLineParser.getParameter("-i") == null)
		{
			System.out.println("Command line parameters invalid.");
			System.exit(0);
		}
			
		final String dataPath = commandLineParser.getParameter("-t").toString();
		final String outputPath = commandLineParser.getParameter("-o").toString();
		final String imgDir = commandLineParser.getParameter("-i").toString();
		
		if(!Common.fileExist(dataPath) | !Common.isPathValid(outputPath) | !Common.dirExist(imgDir))
		{
			System.out.println("Command line parameters invalid.");
			System.exit(0);
		}
		
		if(Common.fileExist(outputPath))
			Common.fileDelete(outputPath);

		EventQueue.invokeLater(new Runnable()
		{
			public void run()
			{	
				// This is included as in the future I may develop
				// an accompanying UI.
				try 
				{
					// Set the system look and feel
					UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
				} 
				catch (Exception e) {} // Don't worry about the exceptions.

				try
				{
					// Initialise controller that will build view/model.
					TestController c = new TestController(dataPath,outputPath,imgDir);

					if(!c.initialise()) // if we can't initialise the controller.
						System.out.println("Could not initialise Controller exiting");
				}
				catch (Exception e)
				{ 
					System.out.println("Error intialising Controller"); 
					e.printStackTrace();
				}
			}
		});
		
		System.out.println("Done.");
	}
}