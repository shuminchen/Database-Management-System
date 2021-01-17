import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.io.*;

public class Loader{
	public Table table;
	public FileChannel[] scan;
	private int rowCursor;
	private int bufferFactor=2000;
	public  ArrayList<String> neededColumn;
	public ArrayList<String> filterCommand;
	
	public Loader(Table table, ArrayList<String> neededCol) throws FileNotFoundException {
		this.table = table;
		this.rowCursor=0;
		this.neededColumn = neededCol;
		readFromDisk();
	}

	public void readFromDisk() throws FileNotFoundException {
		System.out.println(this.table.tablename+"neededColumn in Loader:" + this.neededColumn.toString());
		
		
		if(table.onDisk) {
			FileChannel[] inputArray;
			if(table.joined) {
				inputArray = new FileChannel[1];
				inputArray[0] = new RandomAccessFile(table.tablename+".dat", "r").getChannel();
			}else {
				inputArray = new FileChannel[neededColumn.size()];
				for (int i =0; i<inputArray.length;i++) {
					System.out.println("Loader neededCol  "+ neededColumn.get(i)+".dat");
					inputArray[i] = new RandomAccessFile(neededColumn.get(i)+".dat", "r").getChannel();
				}
			}
			this.scan = inputArray;
			this.rowCursor=0;
		}
		
	}
	
	
	public ArrayList<int[]> getNext() throws IOException{
		if(table.onDisk) {
			ArrayList<int[]> tableContent = new ArrayList<int[]>();
			int [] row = new int[this.neededColumn.size()];
			if(table.joined) {
				ByteBuffer bf = ByteBuffer.allocate(this.neededColumn.size()*4*bufferFactor);
				this.scan[0].read(bf);
				bf.flip();
				while(bf.hasRemaining()) {
					this.rowCursor++;
					row = new int[neededColumn.size()];
					for(int i=0; i<row.length;i++) {
						row[i] = bf.getInt();
					}
					tableContent.add(row);
				}
				bf.clear();
			}else {
				System.out.println(table.tablename+" "+neededColumn.size());
				ByteBuffer[] bfs = new ByteBuffer[neededColumn.size()];
				for(int j=0;j<bfs.length;j++) {
					bfs[j] = ByteBuffer.allocate(4*this.bufferFactor);
					scan[j].read(bfs[j]);
					bfs[j].flip();
				}
				while(bfs[0].hasRemaining()) {
					this.rowCursor++;
					row = new int[neededColumn.size()];
					for(int k=0; k<row.length;k++) {
						row[k] = bfs[k].getInt();
					}
					System.out.println(Arrays.toString(row));
					tableContent.add(row);
				}
				for(ByteBuffer bf: bfs) {
					bf.clear();
				}
			}
			return tableContent;
		}else {
			return table.tableContent;
		}
	}
	
	
	public boolean hasNext() {
		if(rowCursor<table.rowNumber) {
			return true;
		}
		return false;
	}
	public void close() throws IOException {
		if(table.onDisk) {
			for(int i=0;i<scan.length;i++) {
				scan[i].close();
			}
		}
		this.rowCursor=0;
	}
	
}