package cs181;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	/* TODO - Implement the reduce function.
	 *
	 *
	 * Input :    Adjacency Matrix Format       ->	( j   ,   M  \t  i	\t value
	 * 			  Vector Format					->	( j   ,   V  \t   value )
	 *
	 * Output :   Key-Value Pairs
	 * 			  Key ->   	i
	 * 			  Value -> 	M_ij * V_j
	 *
	 */

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Loop through values, to add m_ij term (M value) to mList and save v_j (V Value) to variable v_j
		// Then Iterate through the terms in mList, to multiply each term by variable v_j.
		// Each output is a key-value pair  ( i  ,   m_ij * v_j)

		// initialize variables
		double vVal = 0;	// keep track of the vector value v_j
		ArrayList<String> mList = new ArrayList<String> (); // keep track of matrix values

		// iterate through the values
		for(Text val: values){

			// delimit a value so it can read
			String input  = val.toString();
			String[] indicesAndValue = input.split("\t"); // tab delimited

			// the zero-th element is either V or M
			if(indicesAndValue[0].equals("V")){	// Vector
				vVal += Double.parseDouble(indicesAndValue[1]);	//set the vector value
			}
			else{	// Matrix
				mList.add(indicesAndValue[1]);				//keep track of row
				mList.add(indicesAndValue[2]);				//keep track of m_ij value
			}
		// iterate through the number of values read
		// the xth element in mList corresponds to the xth element in ilist
			Text k = new Text();
			Text v = new Text();
		for( int x= 0; x < mList.size(); x = x+2 ){
			k.set(mList.get(x));	// row value
			v.set(Double.toString((Double.parseDouble(mList.get(x+1)) * vVal)));	// m_ij* v_j
			context.write(k ,v);
		}
		}
		


	}

}
