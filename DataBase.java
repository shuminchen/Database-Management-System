import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.util.*;


public class DataBase{
	public int blocksize = 20000;
	public String path;
	public HashMap<String, String> joinPairs;
	public ArrayList<String> queryFirstLine ; // eg: A12, B2...
	public Character [] querySecondLine;   // an array of the name of relation tables that will need to be joint
	public HashMap<String, int[]> queryThirdLine;	// String= tableAname+tableBname, int[0]--> table1 column, int[1]-->table2column
	public HashMap<Character, ArrayList<String>> queryFourthLine;//  Character--> table name; String: columnNum+corresponding criteria
	public HashMap<String, ArrayList<String>> neededColumn = new HashMap<String, ArrayList<String>>();// this map store the columnes which will be used for each table
																																								//String-->table name, ArrayList<Stringr>--> tableName+columnbNum
																																							
	
	public ArrayList<int[]> resultTable= new ArrayList<int[]>();//the result of the join table

	//the hashmap below store info of meta Data, String--> table name, could be A, or AB  ;    value: info of the table including rowNum, colNum, columInfo
	public HashMap<String, Table> databaseMetaData  = new HashMap<String, Table>();
	// key: table name; value: an array of column object storing its column info (max, min, and UniqueValueNum)

	public HashMap<String, Boolean> onDisk = new HashMap<String, Boolean>();
	//key: table name;  value: to store it on disk or not
		
	
	public DataBase(String path) throws IOException  {
		String[] allTables = path.split(",");
		//data/xxxs/A.csv,
		this.path = allTables[0].substring(0, allTables[0].length()-5);
		for(int i=0;i<allTables.length;i++) {
			String[] temp = allTables[i].split("/");
			String tableName = Character.toString(temp[2].charAt(0));
			Table table = new Table(tableName);
			table.onDisk=true;
			this.databaseMetaData.put(tableName, table);
			loadCsvFile(tableName);
		}
	}
	
	public void clear() {
		
		this.queryFirstLine = new ArrayList<String>(); // eg: A12, B2...
		this.querySecondLine= new Character [1] ;   // an array of the name of relation tables that will need to be joint
		this.queryThirdLine= new HashMap<String, int[]>() ;	// String= tableAname+tableBname, int[0]--> table1 column, int[1]-->table2column
		this.queryFourthLine =  new HashMap<Character, ArrayList<String>>();//  Character--> table name; String: columnNum+corresponding criteria
		this.neededColumn = new HashMap<String, ArrayList<String>>();// this map store the columned which will be used for each table
		this.joinPairs = new HashMap<String, String>();	
		this.resultTable = new ArrayList<int[]>();
//		this.tableColumnInfo  = new HashMap<String, Column[]>();
		this.databaseMetaData = new HashMap<String, Table>();
	}
	
	
	
	/**
	 * This method executes the query and load the database
	 * @param query
	 * @param path  path eg: data/xxxs/A.csv,data/xxxs/B.csv,data/xxxs/C.csv
	 * @throws IOException 
	 */
	public void executeQuery(String[] query) throws IOException {		
		getQuery(query);
		storeNeededColumn();
		//get join order
/*		
		Optimizer optimizer = new Optimizer();
		String firstT = optimizer.calculateJoinOrder(queryThirdLine,databaseMetaData,joinPairs);
		String secondT = this.joinPairs.get(firstT);
		//join table
		 joinTwoSingleTables(firstT, secondT, queryThirdLine, neededColumn);// maybe combine with join Tables with Table
		 String tables = firstT.toString()+secondT.toString();

		for(int i=2;i<querySecondLine.length;i++) {
			 joinTableswithTable(tables, querySecondLine[i],  queryThirdLine,  neededColumn, true) ;//
			tables= tables+querySecondLine[i].toString();		
		}
	*/	
		

		// print out the result of SUM
		long [] sum = join();
		if(sum.length==0) {
			for(int j=0;j<this.queryFirstLine.size();j++) {
				System.out.print(",");
			}
			System.out.println();
		}else {
			for(int k=0;k<sum.length;k++) {
				System.out.print(sum[k]+",");
			}
			System.out.println();
		}
	}

	/**
	 * This method parse the queries and store the query info in the global fields
	 * @param queryInput
	 */
	public void getQuery(String [] query) {
		Parser queryparser = new Parser();
		queryparser.parseQueries(query);
		this.joinPairs = queryparser.joinPairs;
		this.queryFirstLine=queryparser.selectCommandsList;
		this.querySecondLine=queryparser.tables;
		this.queryThirdLine=queryparser.equalJoinCommand;
		this.queryFourthLine=queryparser.filterCommands;	
		this.neededColumn=queryparser.getNeededColumn();
	
	}
	

	/**
	 * This method load csv files and store them into disk using column store.
	 * @param filename
	 * @throws IOException
	 */
	public void loadCsvFile(String tablename) throws IOException {
		
		
		BufferedReader br = new BufferedReader(new FileReader(this.path+tablename+".csv"));// check if this need to add ".csv"
		String line = br.readLine();
		int columnNum =line.split(",").length;
		br.close();
			
		FileReader fr = new FileReader(this.path+tablename+".csv");
		
		//each column of data is stored in separate file. eg the data in column A.c0 is stored in file 'A0.dat'
		//create an array of dos corresponding to each file of column store
		DataOutputStream[] columnsArray = new DataOutputStream[columnNum];
		for (int i =0;i<columnNum;i++) {
			columnsArray[i] = new DataOutputStream (new BufferedOutputStream(new FileOutputStream(tablename+i+".dat")));
			this.databaseMetaData.get(tablename).addColumnInfo(tablename+i);
			this.databaseMetaData.get(tablename).addnewColuIndexInfo(tablename+Integer.toString(i), i);
		}
		
		CharBuffer cb1 = CharBuffer.allocate(32*1024);
		CharBuffer cb2 = CharBuffer.allocate(32*1024);
	
		int rowNum=0;
		while (fr.read(cb1) != -1) {
			cb1.flip();
			int columnCursor = 0;
			int lastNumberStart = 0;
			for (int i = 0; i<cb1.length();i++) {
				if (cb1.charAt(i)==',' ) {
					int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);		
					columnsArray[columnCursor].writeInt(numRead);
					columnCursor++;
					lastNumberStart = i+1;
					// if numRead > columnMax
					
					if(numRead>this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[0]) {
						this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[0]=numRead;
					}
					// if numRead < columnMin
					if(numRead<this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[1]) {
						this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[1] = numRead;
					}
					
				}
				else if (cb1.charAt(i)=='\n') {
					int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);
					columnsArray[columnCursor].writeInt(numRead);
					lastNumberStart = i+1;
					columnCursor =0;
					rowNum ++;
					// if numRead > columnMax
					if(numRead>this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[0]) {
						this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[0]=numRead;
					}
					// if numRead < columnMin
					if(numRead<this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[1]) {
						this.databaseMetaData.get(tablename).columnInfo.get(tablename+Integer.toString(columnCursor))[1] = numRead;
					}
				}
			}
			cb2.clear();
			cb2.append(cb1,lastNumberStart, cb1.length());
			CharBuffer tmp = cb2;
			cb2 = cb1;
			cb1 = tmp;		
		}
		
		//close all the dos in the dos array
		for (DataOutputStream dostr : columnsArray) {
			dostr.close();
		}
		HashMap<String, int[]> tempColuInfo = this.databaseMetaData.get(tablename).columnInfo;
		HashMap<String, int[]>updatedColuInfo = storeUniqueValueInColumns(tempColuInfo, rowNum);
		
		//store the min, max, uniqueValue Num info of each column in the databaseMetaData hashmap
		this.databaseMetaData.get(tablename).columnInfo = updatedColuInfo;

		// reserve the row and column info and store them in the Table object in the databaseMetaData hashmap
		this.databaseMetaData.get(tablename).rowNumber = rowNum;
		this.databaseMetaData.get(tablename).rowNumber = columnNum;

		fr.close();
	}	
	
	public HashMap<String, int[]> storeUniqueValueInColumns(HashMap<String, int[]>coluInfo, int rowNum) {	
		for(String tableAndCol: coluInfo.keySet()) {
			int uniqueValueNum = coluInfo.get(tableAndCol)[0]-coluInfo.get(tableAndCol)[1];
			if(uniqueValueNum>rowNum) {
				coluInfo.get(tableAndCol)[2] = rowNum;
			}else {
				coluInfo.get(tableAndCol)[2] = uniqueValueNum;
			}
		}

		return coluInfo;
	}

	
	/**
	 * This method reads the dat files 
	 * @param filename
	 * @throws IOException
	 */
	public static void readDatFile(String filename) throws IOException {
		DataInputStream input = new DataInputStream(new FileInputStream(filename));

        while (input.available() > 0) {
            int x = input.readInt();
            System.out.println(x);
        }

        input.close();
	}	
	
	
	public Table findTable(String tableName) {
		for(String name: databaseMetaData.keySet()) {
			if(name.contains(tableName)) {
				return databaseMetaData.get(name);
			}
		} 
		return null;
	}
	
	public long[] join() throws IOException {
		String removeKey="";
		long[] sum = new long [1];
		Optimizer optimizer = new Optimizer();
		while(!this.joinPairs.isEmpty()) {
			String table1name =  optimizer.calculateJoinOrder(queryThirdLine,databaseMetaData,joinPairs);
			String table2name = this.joinPairs.get(table1name);
			removeKey = table1name;
			// make sure table 1 is a joined table, instead of a single table 
			if(findTable(table2name).tablename.length()>1) {
				String temp = table1name;
				table1name = table2name;
				table2name = temp;
				removeKey=table2name;
			}
			Table table1 = findTable(table1name);
			Table table2 = findTable(table2name);
			//ensure table1 is the smaller one between the two
			boolean sumHere=false;
			if(table1.rowNumber<table2.rowNumber) {
				if(this.joinPairs.size()==1) {
					sumHere = true;
					sum = joinTables(table1, table2, sumHere);
				}else {
					joinTables(table1, table2, sumHere);
				}	
			}else {
				if(this.joinPairs.isEmpty()) {
					sumHere = true;
					sum = 	joinTables(table2, table1, sumHere);
				}else {
					joinTables(table2, table1, sumHere);
				}
			}
			
			this.joinPairs.remove(removeKey);
		}
		return sum;
	}
	

	public void storeNeededColumn() {
		for(String t: this.databaseMetaData.keySet()) {
			this.databaseMetaData.get(t).storeNeededColumn(this.neededColumn.get(t));
		}
	}

	private long[] joinTables(Table table1, Table table2, boolean sumHere) throws IOException {
		long[] sum =new long[1];
		ArrayList xxx = table1.neededColumn;
		Loader t1Loader = new Loader(table1, table1.neededColumn);
		Loader t2Loader = new Loader(table2, table2.neededColumn);
		// update column index
		for(int a=0; a<table1.neededColumn.size();a++) {
			table1.addnewColuIndexInfo(table1.neededColumn.get(a), a);
		}
		for(int b = 0;b<table2.neededColumn.size();b++) {
			table2.addnewColuIndexInfo(table2.neededColumn.get(b), b);
		}
		
		
		
		if(table1.onDisk==false && table2.onDisk==false) {
			joinInMemory(table1, t1Loader, table2, t2Loader, sumHere);
		}else {
			if(table1.onDisk) {
				if(table2.onDisk) {
					if(sumHere) {
						System.out.println("now joining this two table:"+ table1.tablename +","+table2.tablename);
						sum=joinOnDisk(table1, t1Loader, table2, t2Loader, sumHere );
					}else {
						System.out.println("now joining this two table:"+ table1.tablename +","+table2.tablename);
						joinOnDisk(table1, t1Loader, table2, t2Loader, sumHere );
					}
				}else {
					if(sumHere) {
						System.out.println("now joining this two table:"+ table1.tablename +","+table2.tablename);
						sum= joinInMemory(table2, t2Loader, table1, t1Loader, sumHere);//flip
					}else {
						System.out.println("now joining this two table:"+ table1.tablename +","+table2.tablename);
						joinInMemory(table2, t2Loader, table1, t1Loader, sumHere);//flip
					}
				}
			}else {
				if(sumHere) {
					System.out.println("now joining this two table:"+ table1.tablename +","+table2.tablename);
					sum = 	joinInMemory(table1, t1Loader, table2, t2Loader, sumHere);
				}else {
					System.out.println("now joining this two table:"+ table1.tablename +","+table2.tablename);
					joinInMemory(table1, t1Loader, table2, t2Loader, sumHere);
				}
			}
		}
		// need to clear filter??
		this.databaseMetaData.remove(table1.tablename);
		this.databaseMetaData.remove(table2.tablename);
		return sum;
	}

	private long[] joinInMemory(Table table1, Loader t1Loader, Table table2, Loader t2Loader, boolean sumHere) throws IOException {
		// update column index
		for(int a=0; a<table1.neededColumn.size();a++) {
			table1.addnewColuIndexInfo(table1.neededColumn.get(a), a);
		}
		for(int b = 0;b<table2.neededColumn.size();b++) {
			table2.addnewColuIndexInfo(table2.neededColumn.get(b), b);
		}
		ArrayList<int[]> joinResult = new ArrayList<int[]>();
		int joinedTableRowNum=0;
		boolean satisfy=true;
		long [] sum = null;
		// find all the related equi-predicates
		HashMap<String, int[]> equiPredicates = new HashMap<String, int[]>();// String--> the two tables' name; int[] the two columns that should match
		String[] t1t2 = new String[table1.tablename.length()];
		for(int z=0;z<table1.tablename.length();z++ ) {
			String temp = Character.toString(table1.tablename.charAt(z))+table2.tablename;
			String temp2 = table2.tablename.toString()+Character.toString(table1.tablename.charAt(z));
			if(queryThirdLine.containsKey(temp)) {
				equiPredicates.put(temp, queryThirdLine.get(temp));
				queryThirdLine.remove(temp);
				t1t2[z]=temp;
			}else if(queryThirdLine.containsKey(temp2)) {
				//flip the position of targeted Column Num
				int [] tempo = new int[2];
				tempo[0]=queryThirdLine.get(temp2)[1];
				tempo[1]=queryThirdLine.get(temp2)[0];
				equiPredicates.put(temp, tempo);
				queryThirdLine.remove(temp2);
				t1t2[z]=temp;// eg AB,  only two character, two single table
			}
		}
		
		String firstTwoSingleTables="";
		
		
		int x=0;
		while(x<t1t2.length) {
			if(t1t2[x]!=null) {
				firstTwoSingleTables=t1t2[x];
				break;
			}
			x++;
		}
		System.out.println("equ"+equiPredicates.toString());
		
		
		Table combinedTable = combineTable(table1, table2);
		// get targeted Column of two tables
		int t1TargetedColumnOriginal = equiPredicates.get(firstTwoSingleTables)[0];
		int t2TargetedColumnOriginal = equiPredicates.get(firstTwoSingleTables)[1];
		int t1TargetedColumn = table1.findColumnIndex(Character.toString( firstTwoSingleTables.charAt(0))+Integer.toString(t1TargetedColumnOriginal));
		int t2TargetedColumn = table2.findColumnIndex(Character.toString(firstTwoSingleTables.charAt(1))+Integer.toString(t2TargetedColumnOriginal));
		
		if(sumHere) {
			sum = new long[this.queryFirstLine.size()];
		}
		
		DataOutputStream toDisk = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(combinedTable.tablename+".dat")));
		HashMap<Integer, ArrayList<int[]>> table1buffer = new HashMap<Integer, ArrayList<int[]>>();
		ArrayList<int[]> table2Buffer = new ArrayList<>();
		
		
			//read a block of row in table1
			ArrayList<int[]> t1block = t1Loader.getNext();
			t1Loader.close();
			for(int [] t1row: t1block) {
				if(table1buffer.containsKey(t1row[t1TargetedColumn])) {
					table1buffer.get(t1row[t1TargetedColumn]).add(t1row);
				}else {
					ArrayList<int[]> rowCollection = new ArrayList<int[]>();
					rowCollection.add(t1row);
					table1buffer.put(t1row[t1TargetedColumn], rowCollection);
				}
			}
			t1block = new ArrayList<int[]>();// empty t1block
		
			while(t2Loader.hasNext()) {
				ArrayList<int[]> table2buffer = t2Loader.getNext();
				for(int[] t2row: table2buffer) {
					// if two targeted columns match
					if(table1buffer.containsKey(t2row[t2TargetedColumn])) {
						for(int[] t1row: table1buffer.get(t2row[t2TargetedColumn])){
							int[] resultrow = new int[t1row.length+t2row.length];
							for(int p =0; p<t1row.length; p++) {
								resultrow[p]=t1row[p];
							}
							for(int q=0;q<t2row.length;q++) {
								resultrow[t1row.length+q]=t2row[q];	
							}
							if(satisfyOtherEquiPred(resultrow,equiPredicates, combinedTable ))	{
								satisfy=true;
								if(sumHere) {// sum up the column needed to aggregate
									for(int i=0;i<resultrow.length;i++) {
										int newPosition = combinedTable.findColumnIndex(queryFirstLine.get(i));
										sum[i]= sum[i]+resultrow[newPosition];
									}
								}else {
									// update min & max in the newly merged table
									for(int i=0;i<table1.neededColumn.size();i++) {
										String tableWithColOriginal = table1.neededColumn.get(i);
										int newPosition = combinedTable.findColumnIndex(tableWithColOriginal);
										if(resultrow[newPosition]>combinedTable.columnInfo.get( tableWithColOriginal)[0]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[0]=resultrow[newPosition];
										}
										if(resultrow[newPosition]<combinedTable.columnInfo.get( tableWithColOriginal)[1]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[1]=resultrow[newPosition];
										}
									}
									for(int j=0;j<table2.neededColumn.size();j++) {
										String tableWithColOriginal = table2.neededColumn.get(j);
										int newPosition = combinedTable.findColumnIndex(tableWithColOriginal);
										if(resultrow[newPosition]>combinedTable.columnInfo.get( tableWithColOriginal)[0]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[0]=resultrow[newPosition];
										}
										if(resultrow[newPosition]<combinedTable.columnInfo.get( tableWithColOriginal)[1]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[1]=resultrow[newPosition];
										}
									}
									if(combinedTable.onDisk) {
										for(int value: resultrow ) {
											toDisk.writeInt(value);
										}
									}else {
										joinResult.add(resultrow);
										joinedTableRowNum++;
									}
							}
						}
					}
				}
			}
			table2buffer=new ArrayList<int[]>();// clear table2buffer
			if (joinResult.size()>0 && joinResult.size()*joinResult.get(0).length>100000) {// if the combined table's row * column >100000
				if(sumHere==false) {
					// write it on disk
					for(int[] curr:joinResult) {
						for(int i=0;i<curr.length;i++) {
							toDisk.writeInt(curr[i]);
						}
					}
					combinedTable.onDisk=true;
					combinedTable.joined=true;
				}
				joinResult= new ArrayList<int[]>();//   clear  table
			}		
		}
		t2Loader.close();


		if(combinedTable.onDisk==false) {// if it will not be written on disk 
			File file = new File(combinedTable.tablename+".dat"); 
			file.delete();
			combinedTable.tableContent=joinResult;// store the content of combined table arraylist into the table object  
			combinedTable.onDisk=false;
		}
		toDisk.close();

		// update Unique value 
		HashMap<String, int[]>updatedColuInfo = storeUniqueValueInColumns(combinedTable.columnInfo, joinedTableRowNum);
		combinedTable.columnInfo=updatedColuInfo;
		this.databaseMetaData.put(combinedTable.tablename, combinedTable);// updat to databaseMetaData 
		joinResult = null;

		return sum;
		
	}

	private long[] joinOnDisk(Table table1, Loader t1Loader, Table table2, Loader t2Loader, boolean sumHere) throws IOException {
		// update column index
		for(int a=0; a<table1.neededColumn.size();a++) {
			table1.addnewColuIndexInfo(table1.neededColumn.get(a), a);
		}
		for(int b = 0;b<table2.neededColumn.size();b++) {
			table2.addnewColuIndexInfo(table2.neededColumn.get(b), b);
		}
		
		
		ArrayList<int[]> joinResult = new ArrayList<int[]>();
		int joinedTableRowNum=0;
		boolean satisfy=true;
		long [] sum = null;
		// find all the related equi-predicates
		HashMap<String, int[]> equiPredicates = new HashMap<String, int[]>();// String--> the two tables' name; int[] the two columns that should match
		String[] t1t2 = new String[table1.tablename.length()];
		for(int z=0;z<table1.tablename.length();z++ ) {
			String temp = Character.toString(table1.tablename.charAt(z))+table2.tablename;
			String temp2 = table2.tablename.toString()+Character.toString(table1.tablename.charAt(z));
			if(queryThirdLine.containsKey(temp)) {
				equiPredicates.put(temp, queryThirdLine.get(temp));
				queryThirdLine.remove(temp);
				t1t2[z]=temp;
			}else if(queryThirdLine.containsKey(temp2)) {
				//flip the position of targeted Column Num
				int [] tempo = new int[2];
				tempo[0]=queryThirdLine.get(temp2)[1];
				tempo[1]=queryThirdLine.get(temp2)[0];
				equiPredicates.put(temp, tempo);
				queryThirdLine.remove(temp2);
				t1t2[z]=temp;// eg AB,  only two character, two single table
			}
		}
		
		String firstTwoSingleTables="";
		int x = -1;
		do {
			x++;
			 firstTwoSingleTables = t1t2[x];// pick two tables to join first, ie, pick a predicate to join two table first				 
		}while(t1t2[x]==null&& x<t1t2.length);
		
		Table combinedTable = combineTable(table1, table2);
		// get targeted Column of two tables
		
		int t1TargetedColumnOriginal = equiPredicates.get(firstTwoSingleTables)[0];
		int t2TargetedColumnOriginal = equiPredicates.get(firstTwoSingleTables)[1];
		int t1TargetedColumn = table1.findColumnIndex(Character.toString( firstTwoSingleTables.charAt(0))+Integer.toString(t1TargetedColumnOriginal));
		int t2TargetedColumn = table2.findColumnIndex(Character.toString(firstTwoSingleTables.charAt(1))+Integer.toString(t2TargetedColumnOriginal));

		
		if(sumHere) {
			sum = new long[this.queryFirstLine.size()];
		}
		
		DataOutputStream toDisk = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(combinedTable.tablename+".dat")));
		HashMap<Integer, ArrayList<int[]>> table1buffer = new HashMap<Integer, ArrayList<int[]>>();
		while(t1Loader.hasNext()) {
			//read a block of row in table1
			ArrayList<int[]> t1block = t1Loader.getNext();
			for(int [] t1row: t1block) {
				if(table1buffer.containsKey(t1row[t1TargetedColumn])) {
					table1buffer.get(t1row[t1TargetedColumn]).add(t1row);
				}else {
					ArrayList<int[]> rowCollection = new ArrayList<int[]>();
					rowCollection.add(t1row);
					table1buffer.put(t1row[t1TargetedColumn], rowCollection);
				}
			}
			t1block = new ArrayList<int[]>();// empty t1block
			while(t2Loader.hasNext()) {
				ArrayList<int[]> table2buffer = t2Loader.getNext();
				for(int[] t2row: table2buffer) {
					// if two targeted columns match
					if(table1buffer.containsKey(t2row[t2TargetedColumn])) {
						for(int[] t1row: table1buffer.get(t2row[t2TargetedColumn])){
							int[] resultrow = new int[t1row.length+t2row.length];
							for(int p =0; p<t1row.length; p++) {
								resultrow[p]=t1row[p];
							}
							for(int q=0;q<t2row.length;q++) {
								resultrow[t1row.length+q]=t2row[q];	
							}
							if(satisfyOtherEquiPred(resultrow,equiPredicates, combinedTable ))	{
								satisfy=true;
								if(sumHere) {// sum up the column needed to aggregate
									for(int i=0;i<resultrow.length;i++) {
										int newPosition = combinedTable.findColumnIndex(queryFirstLine.get(i));
										sum[i]= sum[i]+resultrow[newPosition];
									}
								}else {
									// update min & max in the newly merged table
									for(int i=0;i<table1.neededColumn.size();i++) {
										String tableWithColOriginal = table1.neededColumn.get(i);
										int newPosition = combinedTable.findColumnIndex(tableWithColOriginal);
										if(resultrow[newPosition]>combinedTable.columnInfo.get( tableWithColOriginal)[0]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[0]=resultrow[newPosition];
										}
										if(resultrow[newPosition]<combinedTable.columnInfo.get( tableWithColOriginal)[1]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[1]=resultrow[newPosition];
										}
									}
									for(int j=0;j<table2.neededColumn.size();j++) {
										String tableWithColOriginal = table2.neededColumn.get(j);
										int newPosition = combinedTable.findColumnIndex(tableWithColOriginal);
										if(resultrow[newPosition]>combinedTable.columnInfo.get( tableWithColOriginal)[0]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[0]=resultrow[newPosition];
										}
										if(resultrow[newPosition]<combinedTable.columnInfo.get( tableWithColOriginal)[1]) {
											combinedTable.columnInfo.get( tableWithColOriginal)[1]=resultrow[newPosition];
										}
									}
									if(combinedTable.onDisk) {
										for(int value: resultrow ) {
											toDisk.writeInt(value);
										}
									}else {
										joinResult.add(resultrow);
										joinedTableRowNum++;
									}
							}
						}
					}
				}
			}
			table2buffer=new ArrayList<int[]>();// clear innerBuffer 
			if (joinResult.size()>0 && joinResult.size()*joinResult.get(0).length>100000) {// if the combined table's row * column >100000
				if(sumHere==false) {
					// write it on disk
					for(int[] curr:joinResult) {
						for(int i=0;i<curr.length;i++) {
							toDisk.writeInt(curr[i]);
						}
					}
					combinedTable.onDisk=true;
					combinedTable.joined=true;
				}
				joinResult= new ArrayList<int[]>();//   clear  table
			}		
		}
		t2Loader.close();
		t2Loader.readFromDisk();
		table1buffer=new HashMap<>();// clear buffer 
		}
		t1Loader.close();
		t2Loader.close();
		if(combinedTable.onDisk==false) {// if it will not be written on disk 
			File file = new File(combinedTable.tablename+".dat"); 
			file.delete();
			combinedTable.tableContent=joinResult;// store the content of combined table arraylist into the table object  
			combinedTable.onDisk=false;
		}
		toDisk.close();
		joinResult = null;
		HashMap<String, int[]>updatedColuInfo = storeUniqueValueInColumns(combinedTable.columnInfo, joinedTableRowNum);
		combinedTable.columnInfo=updatedColuInfo;// update Unique value 
		this.databaseMetaData.put(combinedTable.tablename, combinedTable);// updat to databaseMetaData 
	

		return sum;
	}
	
	/**
	 * This method check if this row of data satisfy other equi-predicates
	 * @param int[] row
	 * @param equiPredicates
	 * @param newColuIndex, key is the tablename+columNum, eg, A0, B1...; int [1] is the new col number
	 * @return
	 */
	public  boolean satisfyOtherEquiPred(int[] row, HashMap<String, int[]> equiPredicates, Table combinedTable) {
		
		for(String t1t2: equiPredicates.keySet()) {
			//parse predicates
			Character t1 = t1t2.charAt(0);
			int t1TargetedCol = equiPredicates.get(t1t2)[0];
			
			String tableWithColOriginal = t1.toString()+ t1TargetedCol;
			int t1TargtedColNew = combinedTable.findColumnIndex(tableWithColOriginal);
			
			
			
			Character t2 = t1t2.charAt(1);
			int t2TargetedCol = equiPredicates.get(t1t2)[1];
			
			String tableWithColOriginal2 = t2.toString()+ t2TargetedCol;
			int t2TargtedColNew = combinedTable.findColumnIndex(tableWithColOriginal2);
			
			
			// check if this row satisfy other equi predicates 
			if(row[ t1TargtedColNew] != row[ t2TargtedColNew]) {
				return false;
			}	
		}
		return true;
	}
	

	private Table combineTable(Table table1, Table table2) {
		Table combinedTable = new Table(table1.tablename+table2.tablename);
		combinedTable.columnNumber = table1.neededColumn.size()+table2.neededColumn.size();
		combinedTable.onDisk=false;
		combinedTable.joined = true;
		
		ArrayList<String> t1NeededColumn = table1.neededColumn;// table1's needed column
		ArrayList<String> t2NeededColumn = table2.neededColumn;
		ArrayList<String> t1t2NeededColumn = new ArrayList<String>();// two tables' needed column
		t1t2NeededColumn.addAll(t1NeededColumn);
		t1t2NeededColumn.addAll(t2NeededColumn);
		combinedTable.storeNeededColumn(t1t2NeededColumn); 
		this.neededColumn.put(combinedTable.tablename, t1t2NeededColumn);// store it in global field neededColumn hashmap
		
		HashMap<String, int[]> coluInfo = new HashMap<String, int[]>(); //key: columnIndex;     int[]: max, min, num of uiqueValue;
		//add the needed column info into the table object(storing the column index)
		for(String tableAndCol:  table1.columnInfo.keySet()) {
			int[] maxMinUniq = new int[3];
			maxMinUniq[0] = table1.columnInfo.get(tableAndCol)[0];
			maxMinUniq[1] =  table1.columnInfo.get(tableAndCol)[1];
			maxMinUniq[2] = table1.columnInfo.get(tableAndCol)[2];
			coluInfo.put(tableAndCol, maxMinUniq);
		}
		for(String tableAndCol: table2.columnInfo.keySet()) {
			int[] maxMinUniq = new int[3];
			maxMinUniq[0] = table2.columnInfo.get(tableAndCol)[0];
			maxMinUniq[1] =  table2.columnInfo.get(tableAndCol)[1];
			maxMinUniq[2] = table2.columnInfo.get(tableAndCol)[2];
			coluInfo.put(tableAndCol, maxMinUniq);
		}
		combinedTable.columnInfo=coluInfo;
	// update new column Index
		HashMap<String, Integer> newColumnIndex = new HashMap<String, Integer>();
		for(int a=0;a<t1NeededColumn.size();a++) {
			String TableWithCol = t1NeededColumn.get(a);
			newColumnIndex.put(TableWithCol, a);
		}
		for(int b=0;b<t2NeededColumn.size();b++) {
			String TableWithCol = t2NeededColumn.get(b);
			newColumnIndex.put(TableWithCol, b);
		}
		combinedTable.newColuIndex=newColumnIndex;
		
		
		return combinedTable;
	}
	/**
	 * This method calculates the sum of the targeted columns
	 * @param resultTable the final joined table
	 * @param columnNumOfTables
	 * @param queryFirstLine
	 * @return
	 */
	public  long[] sum(int[] resultrow,  Table combinedTable,  ArrayList<String> queryFirstLine) {
		int numOfSumCommand = queryFirstLine.size();
		long[]sum = new long[numOfSumCommand ]; 
		int[] colToSumIndex = new int[numOfSumCommand];				
		for (int i =0; i<numOfSumCommand;i++) {
			Character tableName = queryFirstLine.get(i).charAt(0);
			String coluIndexOld = queryFirstLine.get(i).substring(1);
			
			String x = tableName.toString();
		//	int[] a = newColuIndex.get(x+coluIndexOld);
			
		//	int coluIndexNew = newColuIndex.get(tableName.toString()+coluIndexOld)[1];
	//		colToSumIndex[i] = coluIndexNew;
		}
		for(int[] row: resultTable) {
			for (int j = 0;j<numOfSumCommand;j++) {
				sum[j] =sum[j]+ row[colToSumIndex[j]];
			}	
		}		
		return sum;
	}
	private void storeCombinedTableInfo(Table table1, Table table2) {
		ArrayList<int[]> newColuIndex = new ArrayList<int[]>(); 
		
;
		
		
	
		
	}
	/**
	 * This method check if the current row of data satisfy the filter command(eg, A.c1 =2000)
	 * @param filterCommand, starting with column number, eg, A.c1=2000 --> 1=2000
	 * @param row
	 * @return true if the current row of data match the requirements; otherwise, return false;
	 */
	public boolean satisfyFilterCommand(int[] row, Table table1, Table table2, HashMap<String, Integer> tColumnIndex ) {
		for(Character t: this.queryFourthLine.keySet()) {
			for(int i=0;i<this.queryFourthLine.get(t).size();i++) {
				String [] temp = this.queryFourthLine.get(t).get(i).split("\\s+");
				//update old Colnum with new columnNum in the filter Command list
				String tableNameWithOldCol = t.toString()+temp[0];
				int newCol = tColumnIndex.get(tableNameWithOldCol);
				int targetNum=Integer.parseInt(temp[2]);
				
			
				if (temp[1].equals("=")) {
					if(row[newCol]!=targetNum) {
						return false ;
					}
				}else if(temp[1].equals("<")) {
					if(!(row[newCol]<targetNum)) {
						return false;
					}
				}else if (temp[1].equals(">")) {
					if(!(row[newCol]>targetNum)) {
						return false;
					}
				}
			}
		}
		return true;
	}

	/**
	 * This method joins tables with another single table pulled out from the dat files from different column store 
	 * @param tablesName a big tables joined by multiple tables previously, t1
	 * @param tableName a single table store in the disk in the style of column store, t2
	 * @throws IOException 
	 */
/*	
	public void joinTableswithTable(String tablesName, Character tableName, HashMap<String, int[] > thirdQueryLine, HashMap<Character, HashSet<Integer>> neededColumn, boolean toDisk) throws IOException {
		int  joinedTableRowNum=0;
		DataOutputStream output = new DataOutputStream(new FileOutputStream(tablesName+tableName.toString()+".dat"));
	
		// find all the related equi-predicates
		HashMap<String, int[]> equiPredicates = new HashMap<String, int[]>();// String--> the two tables' name; int[] the two columns that should match
		String[] t1t2 = new String[tablesName.length()];
		for(int z=0;z<tablesName.length();z++ ) {
			String temp = tablesName.charAt(z)+tableName.toString();
			String temp2 = tableName.toString()+tablesName.charAt(z);
			if(thirdQueryLine.containsKey(temp)) {
				equiPredicates.put(temp, thirdQueryLine.get(temp));
				t1t2[z]=temp;
			}else if(thirdQueryLine.containsKey(temp2)) {
				equiPredicates.put(temp2, thirdQueryLine.get(temp2));
				t1t2[z]=temp2;
			}
		}
		
		if(equiPredicates.equals(null)) {
			joinTableswithTableNoPred(tablesName, tableName, thirdQueryLine, neededColumn);	
		}else {
			int tables1colNum = metaData.get(tablesName)[1];
			DataInputStream table1input = new DataInputStream(new FileInputStream(tablesName+".dat"));
			ArrayList<DataInputStream> table2InputList = new ArrayList< DataInputStream>();
			
			HashMap<String, Integer> t2columnIndex = new HashMap<String, Integer>();// how to retain this?
			// this maps store the position of column x of the single table in the row created by pulling data from column store, which is the same as the order number in the table1InputList
		

			int isec=0;
			for(int col2: neededColumn.get(tableName)) {// if this table has x columns that will be used, then create x number of DataInputStream
				String inputTableAndColNum = tableName.toString()+Integer.toString(col2);
		//		table2InputList.add( new DataInputStream(new FileInputStream(inputFileName+".dat")));
				t2columnIndex.put(inputTableAndColNum, isec);// inoutTableAndColNum = eg, A1
				int [] t2coluIndexTrans = new int[2];
				t2coluIndexTrans[0] = Integer.parseInt( inputTableAndColNum.substring(1));// original colunm Index in the single table
				t2coluIndexTrans[1] =  tables1colNum+isec;
				newColuIndex.put(inputTableAndColNum, t2coluIndexTrans );
				isec++;
			}
							
			String twoTableToJoin="";
			int x = -1;
			do {
				x++;
				System.out.println("cut");	
				 twoTableToJoin = t1t2[x];// pick two tables to join first, ie, pick a predicate to join two table first				 
			}while(t1t2[x]==null);
			
			//find the predicate bw these two table, ie find which two columns should match
			int t1targetedColumn=	-1;
			int t2targetedColumn=	-1;
			Character table1singleName;
			if (twoTableToJoin.charAt(0)==tableName) {
				 t2targetedColumn=	equiPredicates.get(twoTableToJoin)[0];
				 t1targetedColumn = equiPredicates.get(twoTableToJoin)[1];
				 table1singleName = twoTableToJoin.charAt(1);
			}else {
				t1targetedColumn = equiPredicates.get(twoTableToJoin)[0];
				t2targetedColumn=	equiPredicates.get(twoTableToJoin)[1];
				table1singleName = twoTableToJoin.charAt(0);
			}
			equiPredicates.remove(twoTableToJoin);
			// and the remove from thirdQueryLine
			int t1targetedColNew = newColuIndex.get(table1singleName.toString()+ t1targetedColumn)[1];
			int t2targetedColNew = t2columnIndex.get(tableName.toString()+t2targetedColumn);
			
			int table2NeededColumnNum = neededColumn.get(tableName).size();
		
			
			//while we have not reach to the end of table 1
			while (table1input.available()>0) {
				ArrayList<int[]> joinResult = new ArrayList<int[]>();
				
				 organize 2000 rows of table1 into a block and store it in hashmap called table1
				 
				HashMap<Integer, ArrayList<int[]>> table1 = new HashMap<Integer, ArrayList<int [ ]>>();
				// the key is the data in the targeted column which will be used to do equi-join
				int j =0;
				while(j<blocksize && table1input.available()>0) {
					j++;
					int key=-1;
					int[ ] row = new int[tables1colNum];
					for(int k=0;k< tables1colNum ; k++) {
						int w = table1input.readInt();
						if(k==t1targetedColNew) {
							key = w;
						}
						row[k]=w;
					}
					if(table1.containsKey(key)) { // if there is already a same integer in the targeted column stored in the hashmap table1, add the new row to row collection
						table1.get(key).add(row);
					}else {// if not, create a new row collection and store the key and the row collection in the hashmap table1 
						ArrayList<int[]> rowCollection = new ArrayList<int[]>();
						rowCollection.add(row);
						table1.put(key, rowCollection);
					}	
				}
				// this hashmap stores a bunch of dataInputStream to import data from different columns; String--> tablename+columnNum
				
				table2InputList = new ArrayList<DataInputStream>();
				for(int col2:  neededColumn.get(tableName)) {// if this table has x columns that will be used, then create x number of DataInputStream
					String inputFileName = tableName.toString()+Integer.toString(col2);
					table2InputList.add( new DataInputStream(new FileInputStream(inputFileName+".dat")));
				}
				
				// next try to compare each row in table2 with each row in the block of table1 
				while(table2InputList.get(t2targetedColNew).available()>0) {
					int key2 = -1;
					int[] t2row = new int[table2NeededColumnNum];
					for (int l = 0;l<table2InputList.size();l++) {
						int y = table2InputList.get(l).readInt();
						if(l==t2targetedColNew) {
							key2 = y;
						}
						t2row[l]=y;
					}		
				
				
		//			System.out.println("Next print result of the join:");
					if (table1.containsKey(key2)) {// eg. if A.c1 = B.c2, write the joined row into 
						System.out.println("table1.getKey2. size:"+table1.get(key2).size());
						for(int[] t1row : table1.get(key2)) {
							int[] resultrow = new int[tables1colNum+table2NeededColumnNum];
							for(int p =0; p<t1row.length; p++) {
								resultrow[p]=t1row[p];
							}
							
							for(int q=0;q<t2row.length;q++) {
								resultrow[tables1colNum+q]=t2row[q];	
							}
							if(satisfyOtherEquiPred(resultrow,equiPredicates, newColuIndex ))	{
								joinResult.add(resultrow);
								joinedTableRowNum++;
							}
						}
					}
				}		
				
		
			//	transfer content in 'joinResult' to disk 
				for (int[] r: joinResult) {
					for(int data: r) {
						output.writeInt(data);
					}
					
				}
				
			}
			//close datainput stream
			table1input.close();
			for(DataInputStream t2input: table2InputList) {
				t2input.close();
			}
			
			output.close();
				
			
				
			//update joined table size to meta Data 	
			int [] rowCol = new int[2];
			rowCol[0] = joinedTableRowNum;
			rowCol[1] = tables1colNum+table2NeededColumnNum;
			metaData.put(tablesName+tableName.toString(), rowCol);
		}	
	}
	*/

	
	
	
	/**
	 *This method joins two tables together
	 * Assume only joining two single table, assume there is a predicate to make equi-join between two tables
	 * @param table1name, a character
	 * @param table2name, a character
	 * @param thirdQueryLine
	 * @param neededColumn
	 * @throws IOException
	 */
	/*
	public void joinTwoSingleTables(Character table1name, Character table2name, HashMap<String, int[] > thirdQueryLine, HashMap<Character, HashSet<Integer>> neededColumn ) throws IOException {
	//	ArrayList<ArrayList<Integer>> joinResult = new ArrayList<ArrayList<Integer>>();
		System.out.println("execute method joinTwoSingleTables");
		int joinedTableRowNum=0;
		//String: joined table name, int[0]: the old index num in the single table, int [1] the new column index after the two tables are joined
		DataOutputStream output = new DataOutputStream(new FileOutputStream(table1name.toString()+table2name.toString()+".dat"));
 		int t1targetedColumn=-1;
		int t2targetedColumn=-1;
		
		String temp =table1name.toString()+table2name;
		if(thirdQueryLine.containsKey(temp)) {
			 t1targetedColumn = thirdQueryLine.get(temp)[0];
			 t2targetedColumn = thirdQueryLine.get(temp)[1];
			 
		}else {
			temp = table2name.toString()+table1name;{
				if(thirdQueryLine.containsKey(temp)) {
					t2targetedColumn = thirdQueryLine.get(temp)[0];
					t1targetedColumn = thirdQueryLine.get(temp)[1];
				}
			}
		}
		
	
		HashMap<String, Integer> t1columnIndex = new HashMap<String, Integer>();// how to retain this?
		// this maps store the position of column x of table A in the row created by pulling data from column store, which is the same as the order number in the table1InputList
		//key: A1; value: new col number
		
		ArrayList<DataInputStream> table1InputList = new ArrayList< DataInputStream>();
		ArrayList<DataInputStream> table2InputList = new ArrayList< DataInputStream>();
		// this hashmap stores a bunch of dataInputStream to import data from different columns; String--> tablename+columnNum
		
		
		int ifirst=0;
		for(int col1: neededColumn.get(table1name)) {// if this table has x columns that will be used, then create x number of DataInputStream
			String inputFileName = table1name.toString()+Integer.toString(col1);
			table1InputList.add( new DataInputStream(new FileInputStream(inputFileName+".dat")));
			t1columnIndex.put(inputFileName, ifirst);// key: A0;  value: new col number
			// 	update the changed info of column number in the new table as a result of two tables joining together
			int [] t1coluIndexTrans = new int[2];
			t1coluIndexTrans[0] = Integer.parseInt( inputFileName.substring(1));// original colunm Index in the single table
			t1coluIndexTrans[1] = ifirst;
			newColuIndex.put(inputFileName, t1coluIndexTrans );
			ifirst++;
		}
		int table1NeededColumnNum = neededColumn.get(table1name).size();
		
		HashMap<String, Integer> t2columnIndex = new HashMap<String, Integer>();// how to retain this?
		// this maps store the position of column x of table B in the row created by pulling data from column store, which is the same as the order number in the table1InputList
		//key: tableName+oldColNum, value: new col
		
		// this hashmap stores a bunch of dataInputStream to import data from different columns; String--> tablename+columnNum
		int isec =0;
		for(int col2: neededColumn.get(table2name)) {// if this table has x columns that will be used, then create x number of DataInputStream
			String inputTableAndColNum = table2name.toString()+Integer.toString(col2);
			t2columnIndex.put(inputTableAndColNum, isec);
			int [] t2coluIndexTrans = new int[2];
			t2coluIndexTrans[0] = Integer.parseInt( inputTableAndColNum.substring(1));// original colunm Index in the single table
			t2coluIndexTrans[1] = table1NeededColumnNum+isec;
			newColuIndex.put(inputTableAndColNum, t2coluIndexTrans );
			isec++;
		}
		
		
		
		// update the changed info of column number in the new table as a result of two tables joining together
	
		int t1targetedColNew = t1columnIndex.get(table1name.toString()+t1targetedColumn);
		int t2targetedColNew = t2columnIndex.get(table2name.toString()+t2targetedColumn);
		
		int table2NeededColumnNum = neededColumn.get(table2name).size();
		
		
		//while we have not reach to the end of table 1
		while (table1InputList.get(t1targetedColNew).available()>0) {
			
			// organize 2000 rows of table1 into a block and store it in hashmap called table1
			 
			HashMap<Integer, ArrayList<int []>> table1 = new HashMap<Integer, ArrayList<int[]>>();
			// the key is the data in the  targeted column which will be used to do equi-join
			int j =0;
			
			while(j<blocksize && table1InputList.get(t1targetedColNew).available()>0) {
				j++;
				int key=-1;
				int[] row = new int[table1NeededColumnNum];
				for(int k=0;k<table1InputList.size() ; k++) {
					int x = table1InputList.get(k).readInt();
					if(k==t1targetedColNew) {
						key = x;
					}
					row[k]=x;
				}
				//check if this row satisfy the filter command
				if(this.queryFourthLine.containsKey(table1name)) {
					ArrayList<String> filters = this.queryFourthLine.get(table1name);	
					if(satisfyFilterCommand(filters, row, t1columnIndex, table1name.toString())) {
						if(table1.containsKey(key)) { // if there is already a same integer in the targeted column stored in the hashmap table1, add the new row to row collection
							table1.get(key).add(row);
						}else {// if not, create a new row collection and store the key and the row collection in the hashmap table1 
							ArrayList<int[]> rowCollection = new ArrayList<int[]>();
							rowCollection.add(row);
							table1.put(key, rowCollection);
						}
					}
				}else {
					if(table1.containsKey(key)) { // if there is already a same integer in the targeted column stored in the hashmap table1, add the new row to row collection
						table1.get(key).add(row);
					}else {// if not, create a new row collection and store the key and the row collection in the hashmap table1 
						ArrayList<int[]> rowCollection = new ArrayList<int[]>();
						rowCollection.add(row);
						table1.put(key, rowCollection);
					}
				}
			}
			
		
			
			// this hashmap stores a bunch of dataInputStream to import data from different columns; String--> tablename+columnNum
			table2InputList=new ArrayList<DataInputStream>();
			for(int col: neededColumn.get(table2name)) {// if this table has x columns that will be used, then create x number of DataInputStream
				String inputFileName = table2name.toString()+Integer.toString(col);
				table2InputList.add( new DataInputStream(new FileInputStream(inputFileName+".dat")));
			}
			
			// next try to compare each row in table2 with each row in the block of table1 
			boolean needNextLine = false;
			while(table2InputList.get(t2targetedColNew).available()>0) {
				int key2 = -1;
				int[] t2row = new int[table2NeededColumnNum];
				do {
					for (int l = 0;l<table2InputList.size();l++) {
						int y = table2InputList.get(l).readInt();
						if(l==t2targetedColNew) {
							key2 = y;
						}
						t2row[l]=(y);
					}
					// check if this row satisfy the filter command
					if(queryFourthLine.containsKey(table2name)) {
						ArrayList<String> filters2 = queryFourthLine.get(table2name);
						if(!satisfyFilterCommand(filters2, t2row, t2columnIndex, table2name.toString())) {
							needNextLine=true;
						}
					}
				}while(needNextLine == true && table2InputList.get(0).available()>0);
			
				if (table1.containsKey(key2)) {// eg. if A.c1 = B.c2, write the joined row into 
					System.out.println("table1.getKey2. size:"+table1.get(key2).size());
					for(int[] t1row : table1.get(key2)) {
			//			int[] resultrow = new int[table1NeededColumnNum+table2NeededColumnNum];
			//			int p=0;
						for(int a: t1row) {
			//				resultrow[p]=a;
			//				p++;
							output.writeInt(a);
							
						}
			//			p=0;
						for(int b: t2row) {
			//				resultrow[p]=b;
			//				p++;
							output.writeInt(b);	
							
						}
						joinedTableRowNum++;
					}
				}
			}
		}
		//close input Stream
		for(DataInputStream t1input: table1InputList) {
			t1input.close();
		}
		for(DataInputStream t2input: table2InputList) {
			t2input.close();
		}
		output.close();

		//update joined table size to meta Data 	
				int [] rowCol = new int[2];
				rowCol[0] = joinedTableRowNum;
				rowCol[1] = table1NeededColumnNum+table2NeededColumnNum;
				metaData.put(table1name.toString()+table2name.toString(), rowCol);
	}
*/	
	

	
	/**
	 * This method joins tables with another single table pulled out from the dat files from different column store
	 * block size 2000 rows
	 * there is no predicates between left table and the right table
	 * @param tablesName a big tables joined by multiple tables previously, t1
	 * @param tableName a single table store in the disk in the style of column store, t2
	 * @throws IOException 
	 */
	/*
	public void joinTableswithTableNoPred(String tablesName, Character tableName, HashMap<String, int[] > thirdQueryLine, HashMap<Character, HashSet<Integer>> neededColumn) throws IOException {
		System.out.println("execute method joinTableswithTableNoPredi");
		int joinedTableRowNum=0;
		DataOutputStream output = new DataOutputStream(new FileOutputStream(tablesName+tableName.toString()+".dat"));
	
		int tables1colNum = metaData.get(tablesName)[1];
		DataInputStream table1input = new DataInputStream(new FileInputStream(tablesName+".dat"));
		ArrayList<DataInputStream> table2InputList = new ArrayList< DataInputStream>();
		
		HashMap<String, Integer> t2columnIndex = new HashMap<String, Integer>();// how to retain this?
		// this maps store the position of column x of the single table in the row created by pulling data from column store, which is the same as the order number in the table1InputList
		

		int ifirst=0;
		for(int col1: neededColumn.get(tableName)) {// if this table has x columns that will be used, then create x number of DataInputStream
			String inputTableAndColNum = tableName.toString()+Integer.toString(col1);
	//		table2InputList.add( new DataInputStream(new FileInputStream(inputFileName+".dat")));
			t2columnIndex.put(inputTableAndColNum, ifirst);// inoutTableAndColNum = eg, A1
			// update the new col number after the two tables are joined
			int [] t2coluIndexTrans = new int[2];
			t2coluIndexTrans[0] = Integer.parseInt( inputTableAndColNum.substring(1));// original colunm Index in the single table
			t2coluIndexTrans[1] =  tables1colNum+ifirst;
			newColuIndex.put(inputTableAndColNum, t2coluIndexTrans );
			ifirst++;
		}
		
		int table2NeededColumnNum = neededColumn.get(tableName).size();	
		//while we have not reach to the end of table 1
		while (table1input.available()>0) {
			ArrayList<int[]> joinResult = new ArrayList<int[]>();
			
			// organize 2000 rows of table1 into a block and store it in hashmap called table1
			 
			ArrayList<int[]> table1 = new ArrayList<int[]>();
			// the key is the data in the targeted column which will be used to do equi-join
			int j =0;
			while(j<blocksize && table1input.available()>0) {
				j++;
				int[] row = new int[tables1colNum];
				for(int k=0;k< tables1colNum ; k++) {
					row[k]= table1input.readInt();
				}
				table1.add(row);	
			}
			
			//????    this hashmap stores a bunch of dataInputStream to import data from different columns; String--> tablename+columnNum
			table2InputList = new ArrayList< DataInputStream>();
			for(int col2: neededColumn.get(tableName)) {// if this table has x columns that will be used, then create x number of DataInputStream
				String inputFileName = tableName.toString()+Integer.toString(col2);
				table2InputList.add( new DataInputStream(new FileInputStream(inputFileName+".dat")));
			}
			
			// next try to compare each row in table2 with each row in the block of table1 
			while(table2InputList.get(0).available()>0) {
				int[] t2row = new int[table2NeededColumnNum];
				for (int l = 0;l<table2InputList.size();l++) {
					int y = table2InputList.get(l).readInt();
					t2row[l]=(y);
				}				

				// write the result of joining t1row and t2row together to the disk			
				for(int a=0;a<table1.size();a++) {
					int [] t1row = table1.get(a);
					for(int z=0; z<t1row.length;z++) {
						output.writeInt(t1row[z]);
					}
					
					for(int b=0; b<t2row.length;b++) {
						output.writeInt(t2row[b]);
					}
					joinedTableRowNum++;
				}	
			}		
		}		
		//close datainput stream
		table1input.close();
		for(DataInputStream t2input: table2InputList) {
			t2input.close();
		}
		output.close();
		
		//update joined table size to meta Data 	
		int [] rowCol = new int[2];
		rowCol[0] = joinedTableRowNum;
		rowCol[1] = tables1colNum+table2NeededColumnNum;
		metaData.put(tablesName+tableName.toString(), rowCol);
		
	}
		
	public Table findTable(String tableName) {
		for(String tablesName:this.databaseMetaData.keySet()) {
			if(tablesName.contains(tableName)) {
				return databaseMetaData.get(tablesName);
			}
		} 
		return null;
	}
*/
	
	
	 
	 /**
	 * This method load csv files and store them into disk using column store.
	 * This method only stores the rows that satisfy the criteria of the forth line of the query
	 * @param filename
	 * @param filter: a formula in the forth line of a query in the fornat of "1 = 1000, i.e, columNum = a number". If there is no corresponding filter to this table, this string will be null, 
	 * @throws IOException
	 */
	/*
	public static void loadCsvFilewithFilter(String filename, ArrayList<String> filters) throws IOException {
		FileReader fr = new FileReader(filename);
		String tableName = filename.substring(0, filename.length()-4);
		
		CharBuffer cb1 = CharBuffer.allocate(32*1024);
		CharBuffer cb2 = CharBuffer.allocate(32*1024);
			
		//each column of data is stored in separate file. eg the data in column A.c0 is stored in file 'A0.dat'
		//this block of code create a list of dos corresponding to each file of column store
		ArrayList<DataOutputStream> dosList = new ArrayList<DataOutputStream>();
		
		// load the file to the disk starting from the first row
		boolean reachEndofRow = false;
		int columnNum =0;
		int rowNum=0;
	
		ArrayList <Integer> row = new ArrayList <Integer>();
		while (fr.read(cb1) != -1) {
			cb1.flip();
			int columnCursor = 0;
			int lastNumberStart = 0;
			for (int i = 0; i<cb1.length();i++) {
				// creat a list of DataOutpoutStream object corresponding to each dat file in the fashion of column store
				if(dosList.size()<=columnCursor) {
					FileOutputStream outputFile = new FileOutputStream(tableName+columnCursor+".dat");
					DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(outputFile));
					dosList.add(columnCursor,dos);
				}
				if (cb1.charAt(i)==',' ) {
					int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);		
					row.add(numRead);		//add to the arraylist called row, which contains a single row of data in the table
					columnCursor++;
					lastNumberStart = i+1;
				}
				else if (cb1.charAt(i)=='\n') {
					int numRead = Integer.parseInt(cb1, lastNumberStart, i, 10);
					row.add(numRead);    //add to the arraylist called row, which contains a single row of data in the table
					columnNum = columnCursor+1;
					columnCursor =0;
					reachEndofRow=true;
					rowNum ++;
					lastNumberStart = i+1;
				}
				//write the data in row into saprate dat file in the disk if this row satisfy the filter 
				if(reachEndofRow==true) {
					
				//	if (satisfyFilterCommand( filters,  row )==true) {
				//		for(int b=0; b<row.size();b++) {
				//			dosList.get(b).writeInt(row.get(b));
				//		}
				//	}
					
					row.clear();
					reachEndofRow =false;
				}	
			}
			cb2.clear();
			cb2.append(cb1,lastNumberStart, cb1.length());
			CharBuffer tmp = cb2;
			cb2 = cb1;
			cb1 = tmp;		
		}
		
		//close all the dos in the dos list
		for (DataOutputStream dostr : dosList) {
			dostr.close();
		}

		// reserve the row and column info and store them in the hashMap called metaData
		int[] rowCol = new int[2];
		rowCol[0] = rowNum;
		rowCol[1] = columnNum;
		System.out.println("columnNumber: "+ columnNum);
		System.out.println("rowNum: "+ rowNum);
		metaData.put(tableName, rowCol);
		fr.close();
	}		
	*/
	
}

	
