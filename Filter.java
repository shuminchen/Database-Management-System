import java.util.ArrayList;

public class Filter{
	
	/**
	 * This method check if the current row of data satisfy the filter command(eg, A.c1 =2000)
	 * @param filterCommand, starting with column number, eg, A.c1=2000 --> 1=2000, ie column number + "=" +  value
	 * @param row
	 * @return true if the current row of data match the requirements; otherwise, return false;
	 */
	public  boolean satisfyFilterCommand(ArrayList<String> filterCommand, ArrayList<Integer> row ) {
		for(int i=0;i<filterCommand.size();i++) {
			String [] temp = filterCommand.get(i).split("\\s+");
			int targetNum=Integer.parseInt(temp[2]);
			int columnNum = Integer.parseInt(temp[0]);
		
			if (temp[1].equals("=")) {
				if(row.get(columnNum)!=targetNum) {
					return false ;
				}
			}else if(temp[1].equals("<")) {
				if(!(row.get(columnNum)<targetNum)) {
					return false;
				}
			}else if (temp[1].equals(">")) {
				if(!(row.get(columnNum)>targetNum)) {
					return false;
				}
			}
		}

		
		return true;
	}
}