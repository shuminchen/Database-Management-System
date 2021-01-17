import java.io.BufferedOutputStream;
import java.io.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.*;

public class Optimizer{
	 public HashMap<String, Table> dbmetaData ;
	 public HashMap<String, int[]> queryThirdLine;
//	 public HashMap<String, int[]> tableColMinMax;
	
	 /**
	 * This method calculates and returns an optimal join order
	 */
	public String calculateJoinOrder(HashMap<String, int[]> queryThirdLinep, HashMap<String, Table> metaData, HashMap<String, String> tablePairs) {
		this.dbmetaData = metaData;
		this.queryThirdLine = queryThirdLinep;
		
		// pick the first two table to join at the lowest cost
		String nextTable = pickTwoTableToJoin(tablePairs) ;

		return nextTable;		
	}
	/**
	 * next two table to join: result--> table1,   tablePairs.get(result)---> table2
	 * @param tablePairs
	 * @return
	 */
	public String pickTwoTableToJoin (HashMap<String, String> tablePairs) {
		double joinCost = Integer.MAX_VALUE;
		String result = null;
		boolean haveJoined = false;
		for(String firstT:tablePairs.keySet()) {
			Table table1 = findTable(firstT);// get table object using table name eg, A
			Table table2 = findTable(tablePairs.get(firstT));// get the name of another table object
			if(table1.tablename.length()>1 || table2.tablename.length()>1) {// check to see if left table is a combined table already,  do the same for right table
				haveJoined = true;
			}
		} 
		
		for(String firstT: tablePairs.keySet()) {
			Table table1 = findTable(firstT);
			Table table2 = findTable(tablePairs.get(firstT));
			double Cardi = 0.00;// the cost of joining table1 and table2
			if(queryThirdLine.containsKey(table1.tablename+table2.tablename)) {
				Cardi = getCardi(table1, table2);
			}else {
				if (queryThirdLine.containsKey(table2.tablename+table1.tablename)) {
					Cardi = getCardi(table2, table1);	
				}
			}
			
			if(Cardi<joinCost) {
				if(haveJoined == true) {
					if(  findTable(table1.tablename).tablename.length()>1 || findTable(table2.tablename).tablename.length()>1) {
						result =firstT ;
						joinCost = Cardi;
					}
				}else {
					result = firstT;
					joinCost = Cardi;
				}
			}
		}
		return result;
	}

	public double getCardi(Table leftT, Table rightT) {
		String leftTable = leftT.tablename;
		String rightTable = rightT.tablename;
		int leftTableTargetedCol = queryThirdLine.get(leftTable+rightTable)[0];
		int leftTableUniqueVal =  this.dbmetaData.get(leftTable).columnInfo.get(leftTable+Integer.toString(leftTableTargetedCol))[2];
			
		int rightTableTargetedCol = queryThirdLine.get(leftTable+rightTable)[1];
		int rightTableUniqueVal =  this.dbmetaData.get(rightTable).columnInfo.get(rightTable+Integer.toString(rightTableTargetedCol))[2];
		
		double Cardi = leftT.rowNumber*rightT.rowNumber*Math.min(leftTableUniqueVal, rightTableUniqueVal) / (leftTableUniqueVal*rightTableUniqueVal);
		                   // ArowNum x BrowNum x min(V(A), V(B)) / [V(A) x V (B) ]
		return Cardi;
	}
	
	public Table findTable(String tableName) {
		for(String tablesName:dbmetaData.keySet()) {
			if(tablesName.contains(tableName)) {
				return dbmetaData.get(tablesName);
			}
		} 
		return null;
	}
	/*
	public  ArrayList<String> pickBestTwoJoin(ArrayList<String> tableList   ) {
		int minCardi = Integer.MAX_VALUE;
		ArrayList<String> joinOrder = new ArrayList<String>();
		String leftTable = "";
		String rightTable = "";
		
		for (int i=0; i<tableList.size()-1;i++) {
			for (int j=i+1;j<tableList.size();j++) {
				leftTable = tableList.get(i);
				rightTable = tableList.get(j);
				if(queryThirdLine.containsKey(leftTable+rightTable)) {
					int Cardi = getCardi(leftTable, rightTable);
					if (Cardi < minCardi) {
						minCardi= Cardi;
						joinOrder.add(0,leftTable+rightTable);
					}
				}else {
					leftTable =tableList.get(j);
					rightTable = tableList.get(i);
					if (queryThirdLine.containsKey(leftTable+rightTable)) {
						int Cardi = getCardi(leftTable, rightTable);
						if(Cardi< minCardi) {
							minCardi=Cardi;
							joinOrder.add(0, leftTable+rightTable);
						}
					}else {
						int Cardi = this.gmetaData.get(leftTable)[0]*this.gmetaData.get(rightTable)[0];// 
					}
				}
			}
		}
		//add the tables that we have not decide order on to the joinOrder arrayList: eg if we pick AD to join first, the list would be [<AD>, <B>, <C>, <D>]
		for (String t: tableList) {
			if (!joinOrder.get(0).contains(t)) {
				joinOrder.add(t);
			}
		}
		
		if(tableList.size()==1) {
			return joinOrder;
		}
		return pickBestTwoJoin(joinOrder);
	}
	
	*/
	

	

	
	
}