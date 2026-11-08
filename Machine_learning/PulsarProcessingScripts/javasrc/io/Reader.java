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
 * File name: 	Reader.java
 * Package: cs.man.ac.uk.mvc
 * Created:	February 19th, 2014
 * Author:	Rob Lyon
 * 
 * Contact:	rob@scienceguyrob.com or robert.lyon@cs.man.ac.uk
 * Web:		<http://www.scienceguyrob.com> or <http://www.cs.manchester.ac.uk> 
 *          or <http://www.jb.man.ac.uk>
 */
package cs.man.ac.uk.io;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import cs.man.ac.uk.common.Common;

/**
 * This class is used to read files in different ways.
 *
 * @author Rob Lyon
 *
 * @version 1.0, 05/01/13
 */
public class Reader
{
	//*****************************************
	//*****************************************
	//                Methods
	//*****************************************
	//*****************************************

	/**
	 * Gets the full string contents of a file and returns them.
	 * 
	 * Returns null if there is an IOException, or if the file is empty.
	 * 
	 * @param path The file to extract the contents from.
	 * @return The contents of the file as a string, or null if the file is empty.
	 */
	public static String getContents(String path)
	{
		//Firstly try to create the file
		File file = new File(path);

		//if the file exists
		if(file.exists())
		{
			String line = "";
			StringBuilder builder = new StringBuilder();

			int counter = 0;

			// Read the file and display it line by line. 
			BufferedReader in = null;

			try
			{
				//open stream to file
				in = new BufferedReader(new FileReader(file));

				try
				{   
					while ((line = in.readLine()) != null)
					{
						if (counter != 0)//if we are not on the first line
						{
							builder.append("\r"+line);
						}
						else//we are on the first line
						{
							//no need for new line character
							builder.append(line);
							counter++;
						}
					}
				}
				catch(IOException e){return null;}
				finally{in.close();}

				if(counter == 0){ return null; }
				else{ return builder.toString();}
			}
			catch (FileNotFoundException e) { return null; }
			catch (IOException e) { return null; }
		}
		else{ return null; }
	}

	/**
	 * Looks for a specified string in a file, returns true if string is present,
	 * and the case is the same.
	 * 
	 * @param path The path to the file to check.
	 * @param keyword The string to look for in the file.
	 * @return True if the file contains the specified string, else false.
	 */
	public static boolean checkStringIsInFile(String path, String keyword)
	{
		//Firstly try to create the file
		File file = new File(path);

		//if the file exists
		if(file.exists())
		{
			String line = "";
			boolean result = false;

			// Read the file and display it line by line. 
			BufferedReader in = null;

			try
			{
				//open stream to file
				in = new BufferedReader(new FileReader(file));

				try
				{		    
					while ((line = in.readLine()) != null)
					{
						if(line.contains(keyword))
						{
							result = true;
							break;
						}
					}
				}
				catch(IOException e){return false;}
				finally{in.close();}

				return result;
			}
			catch (FileNotFoundException e) { return false; }
			catch (IOException e) { return false; }
		}
		else{ return false; }
	}

	/**
	 * Gets the full string contents of a file and returns them.
	 * 
	 * Returns null if there is an IOException, or if the file is empty.
	 * 
	 * @param f The file to extract the contents from.
	 * @return The contents of the file as a string, or null if the file is empty.
	 */
	public static String getContents(File f)
	{
		//Firstly try to create the file
		File file = f;

		//if the file exists
		if(file.exists())
		{
			String line = "";
			StringBuilder builder = new StringBuilder();

			int counter = 0;

			// Read the file and display it line by line. 
			BufferedReader in = null;

			try
			{
				//open stream to file
				in = new BufferedReader(new FileReader(file));

				try
				{   
					while ((line = in.readLine()) != null)
					{
						if (counter != 0)//if we are not on the first line
						{
							builder.append("\r"+line);
						}
						else//we are on the first line
						{
							//no need for new line character
							builder.append(line);
							counter++;
						}
					}
				}
				catch(IOException e){return null;}
				finally{in.close();}

				if(counter == 0){ return null; }
				else{ return builder.toString();}
			}
			catch (FileNotFoundException e) { return null; }
			catch (IOException e) { return null; }
		}
		else{ return null; }
	}

	/**
	 * Counts the number of lines in a text file.
	 * 
	 * Returns -1 if the file is empty, or if there are any errors encountered.
	 * 
	 * @param path The file to retrieve the line count for.
	 * 
	 * @return an integer representation of the line count, 0 if the file is empty.
	 */
	public static int getLineCount(String path)
	{
		//Firstly try to create the file
		File file = new File(path);

		//if the file exists
		if(file.exists())
		{
			@SuppressWarnings("unused")
			String line = "";
			int counter = -1;

			// Read the file and display it line by line. 
			BufferedReader in = null;

			try
			{
				//open stream to file
				in = new BufferedReader(new FileReader(file));

				try
				{
					// Prepare counter
					counter = 0;

					while ((line = in.readLine()) != null){ counter++; }
				}
				catch(IOException e){return -1;}
				finally{in.close();}

				if(counter == 0){ return -1; }
				else{ return counter; }
			}
			catch (FileNotFoundException e) { return -1; }
			catch (IOException e) { return -1; }
		}
		else
		{
			return -1;
		}
	}

	/**
	 * Counts the number of lines in a text file. Fast method.
	 * 
	 * Returns 0 if the file is empty, or if there are any errors encountered.
	 * 
	 * @param path The file to retrieve the line count for.
	 * 
	 * @return an integer representation of the line count, 0 if the file is empty.
	 */
	public static int count(String path)
	{
		try
		{

			InputStream is = new BufferedInputStream(new FileInputStream(path));

			try 
			{

				byte[] c = new byte[1024];
				int count = 0;
				int readChars = 0;
				boolean empty = true;

				while ((readChars = is.read(c)) != -1) 
				{
					empty = false;

					for (int i = 0; i < readChars; ++i) 
					{
						if (c[i] == '\n')
							++count;
					}
				}
				return (count == 0 && !empty) ? 1 : count;
			} 
			finally 
			{
				is.close();
			}
		}
		catch(IOException ioe){return 0;}
	}

	/**
	 * Reads a specific line from a file.
	 * 
	 * Line numbering begins at 1.
	 * 
	 * Entering a line number less than or equal to zero will return an empty
	 * string. If there aren't enough lines in the file, an empty string will be
	 * returned.
	 * 
	 * If any errors are encountered, null will be returned.
	 * 
	 * @param path The file to read.
	 * @param lineNumber The line number to read in the file.
	 * @return The specified line from the file as a string.
	 */
	public static String readLine(String path, int lineNumber)
	{
		if(lineNumber > 0)
		{
			//Firstly try to create the file
			File file = new File(path);

			//if the file exists
			if(file.exists())
			{
				String line = "";
				String content = "";
				int counter = 1;

				// Read the file and display it line by line. 
				BufferedReader in = null;

				try
				{
					//open stream to file
					in = new BufferedReader(new FileReader(file));

					try
					{
						while ((line = in.readLine()) != null)
						{

							if(counter == lineNumber)
							{
								content = line;
								break;
							}   
							counter++;
						}
					}
					catch(IOException e){ return null; }
					finally{ in.close(); }

					return content;
				}
				catch (FileNotFoundException e){ return null; }
				catch (IOException e){ return null; }
			}
			else{ return null; }
		}
		else{ return ""; }
	}

	/**
	 * Tests to see if a file is empty.
	 * @param path
	 * @return true if file is empty, else false.
	 */
	public static boolean isEmpty(String path)
	{
		if(Common.isFile(path))
		{
			try
			{
				FileInputStream stream = new FileInputStream(new File(path));  
				int b = stream.read(); 

				if (b == -1)  
				{  
					stream.close();
					return true;
				}

				stream.close();
				return false;
			}
			catch(IOException e){return false;}
		}
		else{return true;}
	}
}
