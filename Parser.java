import java.util.*;


public class Parser{
//	public static Map<Character, HashSet<int[]>> relationDataList = new HashMap<Character, HashSet<int[]>>();
//	public static HashSet<int[]> relationData = new HashSet<int []>();
	public ArrayList<String> selectCommandsList;// eg A12
	//public HashMap<Character, ArrayList<Integer>> selectCommandsList;
	public Character [] tables ;
	public HashMap<String, String> joinPairs;
	public String[] equations ;
	public HashMap<String, int[]> equalJoinCommand;
	public String[] tempFilterCommands ;
	public HashMap<Character, ArrayList<String>> filterCommands;// Character--> table name; String: columnNum+corresponding criteria
	public HashMap<String, ArrayList<String>> neededColumn = new HashMap<String, ArrayList<String>>();// this map store the columned which will be used for each table
//	public String[] sumOrder;                           																								 //Character-->table name, ArrayList<String>--> tableName+columnNum 
																																																			
	
	public Parser() {
	//	this.selectCommandsList = new HashMap<Character, ArrayList<Integer>>();
		this.selectCommandsList =  new ArrayList<String>();
		this.equalJoinCommand = new HashMap<String, int[]>();
		this.filterCommands = new HashMap<Character, ArrayList<String>>();
	}
	/*
	public void main(String arg[]) {
		System.out.println("Enter predicate ");
		Scanner scanner = new Scanner(System.in);
		parseQueries(scanner);
		scanner = new Scanner(System.in);
		parseQueries(scanner);
		scanner = new Scanner(System.in);
		parseQueries(scanner);
		scanner = new Scanner(System.in);
		parseQueries(scanner);
	}
	*/

	
	

	/**
	 * This method scans the table and returns the targeted result
	 * @param condition: an array of predicates
	 */
	public void parseQueries (String query[]) {
	//	int numOfQuery = scanner.nextInt();
	//	int queryIndex = 0;
	
		
		String line;
		
		//parse four lines of query
		for(int x=0;x<4;x++){
			line = query[x];
			if (line.startsWith("SELECT")) {
				parseSelect(line);
			}else if(line.startsWith("FROM")) {
				parseFrom(line);		
			}else if (line.startsWith("WHERE")) {
				parseWhere(line);		
			}else if (line.startsWith("AND")) {
				parseAnd(line);
			}
			     
		}
	
	}
	
	public void parseSelect(String line) {
		String tableName;
		String columnNum;
		
		String [] sumCommands = line.substring(7).split(", ");
		
		for (int j=0; j<sumCommands.length;j++) {
			tableName = Character.toString(sumCommands[j].charAt(4));
			//count the num of digit of column number
			int start = 7;
			int end = 7;
			while(end<sumCommands[j].length() && sumCommands[j].charAt(end) !='c') {
				end++;
			}
			
			if (start == end) {
					
				columnNum = Character.toString(sumCommands[j].charAt(7));
			}else {
				
				columnNum = (sumCommands[j].substring(7,end-1));
			}
			
			
			this.selectCommandsList.add(tableName+columnNum.toString());
	/*		
			if(this.selectCommandsList.containsKey(tableName)) {
				this.selectCommandsList.get(tableName).add(columnNum);
			}else {
				ArrayList<Integer> arrayOfColNum = new ArrayList<Integer>();
				arrayOfColNum.add(columnNum);
				this.selectCommandsList.put(tableName, arrayOfColNum);// a list of A.c1, B.c2.... to sum up
			}
		*/	
			
			
			// add the info into the hashmap called neededColumns to store info on what table needs to use which columns in the future
			if (this.neededColumn.containsKey(tableName)) {
				this.neededColumn.get(tableName).add(tableName+columnNum);
			}else {
				ArrayList<String> arrayofColNum = new ArrayList<String>();
				arrayofColNum.add(tableName+columnNum);
				this.neededColumn.put(tableName, arrayofColNum);
			}
			//store the order of sum command in the sumOrder array
//			this.sumOrder[j] = tableName+columnNum.toString();
		}
	//	this.selectCommandsArray.add(selectCommandsList);
	}
	
	public void parseFrom(String line) {
		String [] temp= line.substring(5).split(", ");	
		 // an array of the name of relation tables that will need to be joint
		this.tables = new Character[temp.length];
		for (int k=0;k<tables.length;k++) {
			this.tables[k] = temp[k].charAt(0);			
			
		}	
	}
	

	public void parseWhere(String line) {
		this.joinPairs = new HashMap<String, String>();
		this.equations = line.substring(6).split(" AND ");
		char table1name;
		char table2name;
		for (int k=0;k<equations.length;k++) {
			String [] temp = equations[k].split(" = ");
			table1name = temp[0].charAt(0);
			table2name = temp[1].charAt(0);
			String joinTableName = ""+table1name+table2name;
			int tableColumn [] = new int [2];
			tableColumn[0] = Integer.parseInt( temp[0].substring(3));
			tableColumn[1] = Integer.parseInt(temp[1].substring(3));
			this.equalJoinCommand.put(joinTableName, tableColumn);
			
			if(this.joinPairs.containsKey(Character.toString(table1name))) {
				joinPairs.put(Character.toString(table2name), Character.toString(table1name));
			}else {
				joinPairs.put(Character.toString(table1name), Character.toString(table2name));
			}
			
			
			// add the info into the hashmap called neededColumns to store info on what table needs to use which columns in the future
			if (this.neededColumn.containsKey(Character.toString(table1name))) {
				this.neededColumn.get(Character.toString(table1name)).add(Character.toString(table1name)+Integer.toString(tableColumn[0]));
			}else {
				ArrayList<String> arrayofColNum = new ArrayList<String>();
				arrayofColNum.add(Character.toString(table1name)+Integer.toString(tableColumn[0]));
				this.neededColumn.put(Character.toString(table1name), arrayofColNum);
			}
			
			if (this.neededColumn.containsKey(Character.toString(table2name))) {
				this.neededColumn.get(Character.toString(table2name)).add(Character.toString(table2name)+Integer.toString(tableColumn[1]));
			}else {
				ArrayList<String> arrayofColNum = new ArrayList<String>();
				arrayofColNum.add(Character.toString(table2name)+Integer.toString(tableColumn[1]));
				this.neededColumn.put(Character.toString(table2name), arrayofColNum);
			}
			
			
			
			
		}
//		this.equationsList.add(equations);				
	}
	
	public void parseAnd(String line) {
		String temp = line.substring(0, line.length()-1);
		this. tempFilterCommands = temp.substring(4).split(" AND ");
	
		for(int i=0; i<tempFilterCommands.length;i++) {
			Character tableName = tempFilterCommands[i].charAt(0);
			if(this.filterCommands.containsKey(tableName)) {
				this.filterCommands.get(tableName).add( tempFilterCommands[i].substring(3));
			}else {
				ArrayList<String> arrayOfFilterPred = new ArrayList<String>();
				arrayOfFilterPred.add( tempFilterCommands[i].substring(3));
				this.filterCommands.put(tableName,arrayOfFilterPred);
			}
			String filterCommand =  tempFilterCommands[i].substring(3);
			int start = 0;
			int end = 1;
			while(!(filterCommand.charAt(end) == '=' ||  filterCommand.charAt(end) =='<' ||  filterCommand.charAt(end) =='>') ) {
				end++;
			}
			String coluNum = ( filterCommand.substring(start, end-1));
			if (this.neededColumn.containsKey(Character.toString(tableName))) {
				this.neededColumn.get(Character.toString(tableName)).add(Character.toString(tableName)+coluNum);
			}else {
				ArrayList<String> arrayofColNum = new ArrayList<String>();
				arrayofColNum.add(Character.toString(tableName)+coluNum);
				this.neededColumn.put(Character.toString(tableName), arrayofColNum);
			}
			
			
		}
//			this.filterCommandList.add(filterCommand);
	}

	/**
	 * This method returns a hashmap containg info about each table will need which columns
	 * @return
	 */
	public HashMap<String, ArrayList<String>> getNeededColumn() {
		//get rid of the duplicate column in the neededColumn, eg change  <A, [2,2,3]> to <A, [2,3]>
	
		Set<String> tableSet = neededColumn.keySet();
		for(String t: tableSet ) {
			HashSet<String> columnCollection = new HashSet<String>();
			for(String coluIndex: neededColumn.get(t)) {
				columnCollection.add(coluIndex); 
			}
			// add the content in the set to another arrayList
			ArrayList<String> newColumnCollection  = new ArrayList<String>();
			for(String columnIndex: columnCollection ) {
				newColumnCollection.add(columnIndex);
			}
			this.neededColumn.put(t, newColumnCollection); 
		}
		
		return this.neededColumn;
	}
	
	/**
	 * This method parse the query and return an array of string which are the condition command
	 */
	public static  String[] queryParser (String query) {
		 String[ ] a = {};
		 return a;
	}
	
	
	
}
