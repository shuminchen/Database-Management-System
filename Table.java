import java.util.*;

public class Table{
	public String tablename;
	public ArrayList<int[]> tableContent;
	public boolean onDisk=true;
	public boolean joined=false;
	public int columnNumber;
	public int rowNumber;
	public HashMap<String,  int[]> columnInfo; //key:originalTtableName+columnIndex;     int[]: max, min, num of uiqueValue;
	public HashMap<String, Integer> newColuIndex = new HashMap<String, Integer>();	//key: originalTableAndColNum as String, eg A0;  int: new index in  this table	
	public ArrayList<String> neededColumn;// A0;
	
	public Table(String tableFileName) {
		this.tablename = tableFileName;
		this.tableContent = new ArrayList<int[]>();
		this.columnInfo = new HashMap<String, int[]>();
	}
	
	
	public void storeNeededColumn( ArrayList<String> NeededColumn) {
		this.neededColumn = NeededColumn;
	}
	
	public void addColumnInfo(String tableWithCol) {
		int[] maxMinUniq = new int[3];
		maxMinUniq[0] = Integer.MAX_VALUE;
		maxMinUniq[1] = Integer.MIN_VALUE;
		maxMinUniq[2] = 0;
		this.columnInfo.put(tableWithCol, maxMinUniq);
	}
	public void addnewColuIndexInfo(String tableWithCol, Integer i) {
		 this.newColuIndex.put(tableWithCol, i);
	}
	
	public int findColumnIndex(String orginalTableWithColNum) {
		return this.newColuIndex.get(orginalTableWithColNum);
	}
	
}