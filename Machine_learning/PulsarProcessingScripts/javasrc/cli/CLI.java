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
 * File name: 	CLI.java
 * Package: cs.man.ac.uk.mvc
 * Created:	February 19th, 2014
 * Author:	Rob Lyon
 * 
 * Contact:	rob@scienceguyrob.com or robert.lyon@cs.man.ac.uk
 * Web:		<http://www.scienceguyrob.com> or <http://www.cs.manchester.ac.uk> 
 *          or <http://www.jb.man.ac.uk>
 */
package cs.man.ac.uk.cli;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cs.man.ac.uk.common.Strings;

/**
 * This class is used to process and store the command line
 * input to the application. It is simply a wrapper that tries
 * to simplify the process of passing these in, without using 
 * external API's.
 * 
 * @author Rob Lyon
 *
 * @version 1.0, 05/01/13
 */
public class CLI
{
	//*****************************************
	//*****************************************
	//                Variables
	//*****************************************
	//*****************************************

	/**
	 * The command line parameters to expect.
	 */
	Map<String,Integer> expected_parameters = new HashMap<String, Integer>();

	/**
	 * The command line parameters.
	 */
	Map<String,CLParameter> parameters = new HashMap<String, CLParameter>();

	//*****************************************
	//*****************************************
	//             Constructor
	//*****************************************
	//*****************************************

	/**
	 * Default constructor.
	 */
	public CLI(){}

	//*****************************************
	//*****************************************
	//               Methods
	//*****************************************
	//*****************************************

	/**
	 * Adds a new command line parameter.
	 * @param flag the flag to use for the parameter.
	 * @param type the data type, either: bool, string, int, float, double.
	 */
	public void addParameter(String flag, int type) 
	{
		if(!expected_parameters.containsKey(flag))
		{
			System.out.println("Adding expected parameter: flag= "+flag + " Type= "+type);
			expected_parameters.put(flag, type);
		}
		else
			System.out.println("Parameter with flag: "+ flag + " already added!");

	}

	/**
	 * Gets the parameter specified by the flag.
	 * @param flg the flag f the parameter to return.
	 * @return the specified parameter.
	 */
	public CLParameter getParameter(String flg) 
	{
		Iterator<Entry<String, CLParameter>> iterator = parameters.entrySet().iterator();

		while (iterator.hasNext()) 
		{
			Map.Entry<String, CLParameter> pairs = (Map.Entry<String, CLParameter>)iterator.next();

			String flag =  pairs.getKey();
			CLParameter value = pairs.getValue();

			if(flag.equals(flg))
				return value;

		}

		System.out.println("Parameter with flag: "+ flg + " not provided.");
		return null;
	}

	/**
	 * Processes the command line arguments one by one.
	 * @param arg the command line arguments.
	 */
	public void processArguments(String[] arg) 
	{
		for(int i = 0; i < arg.length; i++)
			this.processArgument(arg[i]);
	}

	/**
	 * Processes the individual arguments passed at the command line.
	 * @param arg the argument to process.
	 * @return true if the parameter passed in is as expected, else false.
	 */
	public boolean processArgument(String arg)
	{
		Iterator<Entry<String, Integer>> iterator = expected_parameters.entrySet().iterator();

		while (iterator.hasNext()) 
		{
			Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)iterator.next();

			String flag =  pairs.getKey();
			int type = pairs.getValue();

			if (arg.startsWith(flag) || arg.startsWith(flag.toUpperCase()))
			{
				String p = Strings.trimArgument(arg, flag);
				//System.out.println("Flag: "+ flag + " Type:" + type +" Parameter = "+ p);
				CLParameter parameter = new CLParameter(p.trim(),type);

				// Check parameter is valid
				if(type == CLParameter.INT_PARAM_TYPE && parameter.toInt() != null )
				{
					this.parameters.put(flag, parameter);
					return true;
				}
				else if(type == CLParameter.FLOAT_PARAM_TYPE && parameter.toFloat() != null )
				{
					this.parameters.put(flag, parameter);
					return true;
				}
				else if(type == CLParameter.DOUBLE_PARAM_TYPE && parameter.toDouble() != null )
				{
					this.parameters.put(flag, parameter);
					return true;
				}
				else if(type == CLParameter.BOOL_PARAM_TYPE && parameter.toBool() != null )
				{
					if(parameter.toBool()==false)
					{
						parameter.setValue("false");
						this.parameters.put(flag, parameter);
					}
					else 
					{
						parameter.setValue("true");
						this.parameters.put(flag,parameter);
					}
								
					return true;
				}
				else if(type == CLParameter.STRING_PARAM_TYPE && parameter.toString() != null )
				{
					this.parameters.put(flag, parameter);
					return true;
				}
				else
				{
					System.out.println("Provided parameter " + flag + " " + p + " data type invalid!");			
					return false;
				}

			}
		}
		return true;
	}

	/**
	 * Over-ridden toString method.
	 */   
	public String toString()
	{
		String representation = "Provided command line parameters:\n";

		Iterator<Entry<String, CLParameter>> iterator = parameters.entrySet().iterator();

		int count = 1;

		while (iterator.hasNext()) 
		{
			Map.Entry<String, CLParameter> pairs = (Map.Entry<String, CLParameter>)iterator.next();

			String flag =  pairs.getKey();
			CLParameter value = pairs.getValue();


			representation += (count + ".\t" + flag + "\t" + value.toString() + "\n");
			count += 1;
		}

		return representation;
	}
}
