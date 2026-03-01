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
 * File name: 	IStats.java
 * Package: cs.man.ac.uk.mvc
 * Created:	February 19th, 2014
 * Author:	Rob Lyon
 * 
 * Contact:	rob@scienceguyrob.com or robert.lyon@cs.man.ac.uk
 * Web:		<http://www.scienceguyrob.com> or <http://www.cs.manchester.ac.uk> 
 *          or <http://www.jb.man.ac.uk>
 */
package cs.man.ac.uk.common;

/**
 * The class IStats.java is a part of the package cs.man.ac.uk.common, in the 
 * project "STFUD".
 *
 * This class describes an interface for collecting statisitics.
 * 
 * @author Rob Lyon
 *
 */
public interface IStats
{
    public abstract double getMin();
    
    public abstract double getMax();
    
    public abstract double getVarience();
    
    public abstract double getSTDEV();
    
    public abstract double getMean();
    
    public abstract double getMode();
    
    public abstract double getMedian();
    
    public abstract double getRange();
    
    public abstract double getIQR();
    
    public abstract double getQ1();
    
    public abstract double getQ3();
    
    public abstract void addObservation();
    
    public abstract void clear();
}
