import java.io.IOException;
import java.util.Scanner;

public class Main{
	public static void main(String arg[]) throws IOException {
		Scanner console = new Scanner(System.in);
		String path = "data/xxs/A.csv,data/xxs/B.csv,data/xxs/C.csv,data/xxs/D.csv,data/xxs/E.csv,data/xxs/F.csv,";
	//	String path = console.nextLine();
		DataBase db = new DataBase(path);
		String [] querys = new String[4];
		querys[0]="SELECT SUM(D.c4), SUM(F.c2), SUM(D.c3)";
		querys[1]="FROM A, C, D, F";
		querys[2]="WHERE A.c2 = C.c0 AND A.c3 = D.c0 AND D.c1 = F.c0";
		querys[3]="AND D.c2 < 28957;";
		
		//pass query to database 	
		db.executeQuery(querys);
		console.nextLine();
	/*	
		int queryNum = console.nextInt();
		console.nextLine();
		for(int i=0;i<queryNum;i++) {
			String [] querys = new String[4];
			for(int j=0;j<4;j++) {
				querys[j] = console.nextLine();
			}
			//pass query to database 	
			db.executeQuery(querys);
			console.nextLine();
		}
		*/
		//remove database;
		db.clear();
	}
	
}