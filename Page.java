package database;
import java.io.RandomAccessFile;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Page
{
	public static int pageSize = 512;
	public static final String dateFormat = "yyyy-MM-dd_HH:mm:ss";

	// Give the Payload size
	public static short calculatePayloadSize(String[] valueStr, String[] dataTypeStr)
	{
		int value = 1 + dataTypeStr.length - 1; // # col + stc - rowid
		for(int i = 1; i < dataTypeStr.length; i++)
		{
			String dType = dataTypeStr[i];
			switch(dType)
			{
				case "TINYINT":
					value = value + 1;
					break;
				case "SMALLINT":
					value = value + 2;
					break;
				case "INT":
					value = value + 4;
					break;
				case "BIGINT":
					value = value + 8;
					break;
				case "REAL":
					value = value + 4;
					break;		
				case "DOUBLE":
					value = value + 8;
					break;
				case "DATETIME":
					value = value + 8;
					break;
				case "DATE":
					value = value + 8;
					break;
				case "TEXT":
					String text1 = valueStr[i];
					int leng = text1.length();
					value = value + leng;
					break;
				default:
					break;
			}
		}
		return (short)value;
	}


	// Create an interior page and return the page number
	public static int createInteriorPage(RandomAccessFile rAFile)
	{
		int numPage = 0;
		try
		{
			numPage = (int)(rAFile.length()/(new Long(pageSize)));
			numPage = numPage + 1;
			rAFile.setLength(pageSize * numPage);
			rAFile.seek((numPage-1)*pageSize);
			rAFile.writeByte(0x05);  // this page is a table interior page
		}
		catch(Exception e)
		{
			System.out.println("Error in createInteriorPage");
		}
		return numPage;
	}

	// Create a leaf page and return the page number.
	public static int createLeafPage(RandomAccessFile rAFile)
	{
		int numPage = 0;
		try
		{
			numPage = (int)(rAFile.length()/(new Long(pageSize)));
			numPage = numPage + 1;
			rAFile.setLength(pageSize * numPage);
			rAFile.seek((numPage-1)*pageSize);
			rAFile.writeByte(0x0D);  // This page is a table leaf page
		}
		catch(Exception e)
		{
			System.out.println("Error at createLeafPage");
		}

		return numPage;
	}

	// To get the middle value of page
	public static int getMidKey(RandomAccessFile rAFile, int pageNo)
	{
		int val = 0;
		try
		{
			rAFile.seek((pageNo-1)*pageSize);
			byte pageType = rAFile.readByte();
			int numCells = getCellNo(rAFile, pageNo);
			int middle = (int) Math.ceil((double) numCells / 2);
			long location = getCellLocation(rAFile, pageNo, middle-1);
			rAFile.seek(location);

			switch(pageType)
			{
				case 0x05:
					val = rAFile.readInt(); 
					val = rAFile.readInt();
					break;
				case 0x0D:
					val = rAFile.readShort();
					val = rAFile.readInt();
					break;
			}

		}
		catch(Exception e)
		{
			System.out.println("Error at getMidKey");
		}

		return val;
	}

	// Make currentPage's parent as newPage's parent and give half of its cells too
	public static void splitLeafPage(RandomAccessFile raFile, int currentPage, int newPage)
	{
		try
		{
			int numCells = getCellNo(raFile, currentPage);
			int middle = (int) Math.ceil((double) numCells / 2);
			int numberCell1 = middle - 1;
			int contentSize = 512;

			for(int i = numberCell1; i < numCells; i++)
			{
				long location = getCellLocation(raFile, currentPage, i);
				raFile.seek(location);
				int cellSize = raFile.readShort()+6;
				contentSize = contentSize - cellSize;
				raFile.seek(location);
				byte[] cell = new byte[cellSize];
				raFile.read(cell);
				raFile.seek((newPage-1)*pageSize+contentSize);
				raFile.write(cell);
				setCellOffset(raFile, newPage, i - numberCell1, contentSize);
			}

			raFile.seek((newPage-1)*pageSize+2);
			raFile.writeShort(contentSize);

			short offset = getCellOffset(raFile, currentPage, numberCell1-1);
			raFile.seek((currentPage-1)*pageSize+2);
			raFile.writeShort(offset);

			int right = getRight(raFile, currentPage);
			setRight(raFile, newPage, right);
			setRight(raFile, currentPage, newPage);

			int parent = findParent(raFile, currentPage);
			setParent(raFile, newPage, parent);

			byte numByte = (byte) numberCell1;
			setCellNo(raFile, currentPage, numByte);
			numByte = (byte) numberCell1;
			setCellNo(raFile, newPage, numByte);
		}
		catch(Exception e)
		{
			System.out.println("Error at splitLeafPage");
			e.printStackTrace();
		}
	}
	
	// Give half cells of currentPage to newPage.
	public static void splitInteriorPage(RandomAccessFile rAFile, int currentPage, int newPage)
	{
		try
		{
			int numCells = getCellNo(rAFile, currentPage);
			int middle = (int) Math.ceil((double) numCells / 2);

			int numberCell1 = middle - 1;
			int numberCell2 = numCells - numberCell1 - 1;
			short contentSize = 512;

			for(int i = numberCell1+1; i < numCells; i++)
			{
				long location = getCellLocation(rAFile, currentPage, i);
				short cellSize = 8;
				contentSize = (short)(contentSize - cellSize);
				rAFile.seek(location);
				byte[] cell = new byte[cellSize];
				rAFile.read(cell);
				rAFile.seek((newPage-1)*pageSize+contentSize);
				rAFile.write(cell);
				rAFile.seek(location);
				int page = rAFile.readInt();
				setParent(rAFile, page, newPage);
				setCellOffset(rAFile, newPage, i - (numberCell1 + 1), contentSize);
			}
			int temp = getRight(rAFile, currentPage);
			setRight(rAFile, newPage, temp);
			long middleLocation = getCellLocation(rAFile, currentPage, middle - 1);
			rAFile.seek(middleLocation);
			temp = rAFile.readInt();
			setRight(rAFile, currentPage, temp);
			rAFile.seek((newPage-1)*pageSize+2);
			rAFile.writeShort(contentSize);
			short offset = getCellOffset(rAFile, currentPage, numberCell1-1);
			rAFile.seek((currentPage-1)*pageSize+2);
			rAFile.writeShort(offset);

			int parent = findParent(rAFile, currentPage);
			setParent(rAFile, newPage, parent);
			byte num = (byte) numberCell1;
			setCellNo(rAFile, currentPage, num);
			num = (byte) numberCell2;
			setCellNo(rAFile, newPage, num);
		}
		catch(Exception e)
		{
			System.out.println("Error at splitInteriorPage");
		}
	}

	// Split leaf
	public static void splitLeaf(RandomAccessFile rAFile, int page)
	{
		int newPage = createLeafPage(rAFile);
		int middleKey = getMidKey(rAFile, page);
		splitLeafPage(rAFile, page, newPage);
		int parent = findParent(rAFile, page);
		if(parent == 0)
		{
			int rootPage = createInteriorPage(rAFile);
			setParent(rAFile, page, rootPage);
			setParent(rAFile, newPage, rootPage);
			setRight(rAFile, rootPage, newPage);
			insertInteriorCell(rAFile, rootPage, page, middleKey);
		}
		else
		{
			long plocation = findPointerLocation(rAFile, page, parent);
			setPointerLocation(rAFile, plocation, parent, newPage);
			insertInteriorCell(rAFile, parent, page, middleKey);
			sortCells(rAFile, parent);
			while(checkInteriorSpace(rAFile, parent))
			{
				parent = splitInterior(rAFile, parent);
			}
		}
	}

	// Split non leaf
	public static int splitInterior(RandomAccessFile rAFile, int page)
	{
		int newPage = createInteriorPage(rAFile);
		int middleKey = getMidKey(rAFile, page);
		splitInteriorPage(rAFile, page, newPage);
		int parent = findParent(rAFile, page);
		if(parent == 0)
		{
			int rootPage = createInteriorPage(rAFile);
			setParent(rAFile, page, rootPage);
			setParent(rAFile, newPage, rootPage);
			setRight(rAFile, rootPage, newPage);
			insertInteriorCell(rAFile, rootPage, page, middleKey);
			return rootPage;
		}
		else
		{
			long plocation = findPointerLocation(rAFile, page, parent);
			setPointerLocation(rAFile, plocation, parent, newPage);
			insertInteriorCell(rAFile, parent, page, middleKey);
			sortCells(rAFile, parent);
			return parent;
		}
	}

	public static void sortCells(RandomAccessFile rAFile, int page)
	{
		 byte numByte = getCellNo(rAFile, page);
		 int[] keyArray = findKeyArray(rAFile, page);
		 short[] cellArray = findCellArray(rAFile, page);
		 int ltemp;
		 short rtemp;

		 for (int i = 1; i < numByte; i++) 
		 {
            for(int j = i ; j > 0 ; j--)
            {
                if(keyArray[j] < keyArray[j-1])
                {

                    ltemp = keyArray[j];
                    keyArray[j] = keyArray[j-1];
                    keyArray[j-1] = ltemp;

                    rtemp = cellArray[j];
                    cellArray[j] = cellArray[j-1];
                    cellArray[j-1] = rtemp;
                }
            }
         }

         try
         {
        	 rAFile.seek((page-1)*pageSize+12);
         	for(int i = 0; i < numByte; i++)
         	{
         		rAFile.writeShort(cellArray[i]);
			}
         }
         catch(Exception e)
         {
         	System.out.println("Error at sortCells");
         }
	}

	public static int[] findKeyArray(RandomAccessFile rAFile, int page)
	{
		int num1 = new Integer(getCellNo(rAFile, page));
		int[] array = new int[num1];

		try
		{
			rAFile.seek((page-1)*pageSize);
			byte pageType = rAFile.readByte();
			byte offset = 0;
			switch(pageType)
			{
				case 0x05:
					offset = 4;
					break;
				case 0x0d:
					offset = 2;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < num1; i++)
			{
				long location = getCellLocation(rAFile, page, i);
				rAFile.seek(location+offset);
				array[i] = rAFile.readInt();
			}

		}
		catch(Exception e)
		{
			System.out.println("Error at findKeyArray");
		}

		return array;
	}

	public static short[] findCellArray(RandomAccessFile rAFile, int page)
	{
		int num1 = new Integer(getCellNo(rAFile, page));
		short[] array = new short[num1];

		try
		{
			rAFile.seek((page-1)*pageSize+12);
			for(int i = 0; i < num1; i++){
				array[i] = rAFile.readShort();
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at findCellArray");
		}

		return array;
	}

	// Return the parent page number
	public static int findParent(RandomAccessFile rAFile, int page)
	{
		int valT = 0;
		try
		{
			rAFile.seek((page-1)*pageSize+8);
			valT = rAFile.readInt();
		}
		catch(Exception e)
		{
			System.out.println("Error at findParent");
		}

		return valT;
	}

	public static void setParent(RandomAccessFile rAFile, int page, int parent)
	{
		try
		{
			rAFile.seek((page-1)*pageSize+8);
			rAFile.writeInt(parent);
		}
		catch(Exception e)
		{
			System.out.println("Error at setParent");
		}
	}

	// Get pointer location
	public static long findPointerLocation(RandomAccessFile rAFile, int page, int parent)
	{
		long valT = 0;
		try
		{
			int numberCell = new Integer(getCellNo(rAFile, parent));
			for(int i=0; i < numberCell; i++)
			{
				long location = getCellLocation(rAFile, parent, i);
				rAFile.seek(location);
				int childPage = rAFile.readInt();
				if(childPage == page)
				{
					valT = location;
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at findPointerLocation");
		}

		return valT;
	}

	// Set pointer
	public static void setPointerLocation(RandomAccessFile rAFile, long location, int parent, int page)
	{
		try
		{
			if(location == 0)
			{
				rAFile.seek((parent-1)*pageSize+4);
			}
			else
			{
				rAFile.seek(location);
			}
			rAFile.writeInt(page);
		}
		catch(Exception e)
		{
			System.out.println("Error at setPointerLocation");
		}
	} 

	// Insert a cell into interior page
	public static void insertInteriorCell(RandomAccessFile rAFile, int page, int child, int key)
	{
		try
		{
			// find location
			rAFile.seek((page-1)*pageSize+2);
			short contentSize = rAFile.readShort();
			if(contentSize == 0)
			{	
				contentSize = 512;
			}
			contentSize = (short)(contentSize - 8);
			// write data
			rAFile.seek((page-1)*pageSize+contentSize);
			rAFile.writeInt(child);
			rAFile.writeInt(key);
			// fix content
			rAFile.seek((page-1)*pageSize+2);
			rAFile.writeShort(contentSize);
			// fix cell arrray
			byte num1 = getCellNo(rAFile, page);
			setCellOffset(rAFile, page ,num1, contentSize);
			// fix number of cell
			num1 = (byte) (num1 + 1);
			setCellNo(rAFile, page, num1);

		}
		catch(Exception e)
		{
			System.out.println("Error at insertInteriorCell");
		}
	}

	// Insert a cell into a leaf page.
	public static void insertLeaf(RandomAccessFile rAFile, int page, int offset, short pLSize, int key, byte[] stc, String[] valStr, String table)
	{
		try
		{
			String str;
			rAFile.seek((page-1)*pageSize+offset);
			String[] columnName = Table.findColumnName(table);
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
			{	
				RandomAccessFile IndexFile = new RandomAccessFile("data\\"+DavisBase.currentDataBase+"\\"+table+"\\"+columnName[0]+".ndx", "rw");
				IndexFile.seek(IndexFile.length());
				IndexFile.writeInt(key);
				IndexFile.writeLong(rAFile.getFilePointer());
				IndexFile.close();
				
				for(int i = 1; i < valStr.length; i++)					
				{
					IndexFile = new RandomAccessFile("data\\"+DavisBase.currentDataBase+"\\"+table+"\\"+columnName[i]+".ndx", "rw");
					IndexFile.seek(IndexFile.length());
					switch(stc[i-1])
					{
						case 0x00:
							IndexFile.writeByte(0);
							break;
						case 0x01:
							IndexFile.writeShort(0);
							break;
						case 0x02:
							IndexFile.writeInt(0);
							break;
						case 0x03:
							IndexFile.writeLong(0);
							break;
						case 0x04:
							IndexFile.writeByte(new Byte(valStr[i]));
							break;
						case 0x05:
							IndexFile.writeShort(new Short(valStr[i]));
							break;
						case 0x06:
							IndexFile.writeInt(new Integer(valStr[i]));
							break;
						case 0x07:
							IndexFile.writeLong(new Long(valStr[i]));
							break;
						case 0x08:
							IndexFile.writeFloat(new Float(valStr[i]));
							break;
						case 0x09:
							IndexFile.writeDouble(new Double(valStr[i]));
							break;
						case 0x0A:
							str = valStr[i];
							Date date = new SimpleDateFormat(dateFormat).parse(str);
							long time = date.getTime();
							IndexFile.writeLong(time);
							break;
						case 0x0B:
							str = valStr[i];
							str = str+"_00:00:00";
							Date date1 = new SimpleDateFormat(dateFormat).parse(str);
							long time2 = date1.getTime();
							IndexFile.writeLong(time2);
							break;
						default:
							rAFile.writeBytes(valStr[i]);
							break;
						
					}
					
					IndexFile.writeLong(rAFile.getFilePointer());
					IndexFile.close();
				}
				
			}
			
			
			rAFile.seek((page-1)*pageSize+offset);
			rAFile.writeShort(pLSize);
			rAFile.writeInt(key);
			int colNo = valStr.length - 1;
			
			
			rAFile.writeByte(colNo);
			rAFile.write(stc);
			
			for(int i = 1; i < valStr.length; i++)
			{	
				switch(stc[i-1])
				{
					case 0x00:
						rAFile.writeByte(0);
						break;
					case 0x01:
						rAFile.writeShort(0);
						break;
					case 0x02:
						rAFile.writeInt(0);
						break;
					case 0x03:
						rAFile.writeLong(0);
						break;
					case 0x04:
						rAFile.writeByte(new Byte(valStr[i]));
						break;
					case 0x05:
						rAFile.writeShort(new Short(valStr[i]));
						break;
					case 0x06:
						rAFile.writeInt(new Integer(valStr[i]));
						break;
					case 0x07:
						rAFile.writeLong(new Long(valStr[i]));
						break;
					case 0x08:
						rAFile.writeFloat(new Float(valStr[i]));
						break;
					case 0x09:
						rAFile.writeDouble(new Double(valStr[i]));
						break;
					case 0x0A:
						str = valStr[i];
						Date date = new SimpleDateFormat(dateFormat).parse(str);
						long time = date.getTime();
						rAFile.writeLong(time);
						break;
					case 0x0B:
						str = valStr[i];
						str = str+"_00:00:00";
						Date date1 = new SimpleDateFormat(dateFormat).parse(str);
						long time2 = date1.getTime();
						rAFile.writeLong(time2);
						break;
					default:
						rAFile.writeBytes(valStr[i]);
						break;
				}
			}
			int no = getCellNo(rAFile, page);
			byte temp = (byte) (no+1);
			setCellNo(rAFile, page, temp);
			rAFile.seek((page-1)*pageSize+12+no*2);
			rAFile.writeShort(offset);
			rAFile.seek((page-1)*pageSize+2);
			int contentSize = rAFile.readShort();
			if(contentSize >= offset || contentSize == 0)
			{
				rAFile.seek((page-1)*pageSize+2);
				rAFile.writeShort(offset);
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at insertLeaf");
			e.printStackTrace();
		}
	}

	public static void updateLeaf(RandomAccessFile rAFile, int page, int offset, int pLSize, int key, byte[] stc, String[] valStr, String table)
	{
		try
		{
			String str;
			rAFile.seek((page-1)*pageSize+offset);
			rAFile.writeShort(pLSize);
			rAFile.writeInt(key);
			int column = valStr.length - 1;
			rAFile.writeByte(column);
			rAFile.write(stc);
			for(int i = 1; i < valStr.length; i++)
			{
				switch(stc[i-1])
				{
					case 0x00:
						rAFile.writeByte(0);
						break;
					case 0x01:
						rAFile.writeShort(0);
						break;
					case 0x02:
						rAFile.writeInt(0);
						break;
					case 0x03:
						rAFile.writeLong(0);
						break;
					case 0x04:
						rAFile.writeByte(new Byte(valStr[i]));
						break;
					case 0x05:
						rAFile.writeShort(new Short(valStr[i]));
						break;
					case 0x06:
						rAFile.writeInt(new Integer(valStr[i]));
						break;
					case 0x07:
						rAFile.writeLong(new Long(valStr[i]));
						break;
					case 0x08:
						rAFile.writeFloat(new Float(valStr[i]));
						break;
					case 0x09:
						rAFile.writeDouble(new Double(valStr[i]));
						break;
					case 0x0A:
						str = valStr[i];
						Date date = new SimpleDateFormat(dateFormat).parse(str.substring(1, str.length()-1));
						long time = date.getTime();
						rAFile.writeLong(time);
						break;
					case 0x0B:
						str = valStr[i];
						str = str.substring(1, str.length()-1);
						str = str+"_00:00:00";
						Date date2 = new SimpleDateFormat(dateFormat).parse(str);
						long time2 = date2.getTime();
						rAFile.writeLong(time2);
						break;
					default:
						rAFile.writeBytes(valStr[i]);
						break;
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at updateLeaf");
			System.out.println(e);
		}
	}

	public static int getRight(RandomAccessFile rAFile, int page)
	{
		int value = 0;
		try
		{
			rAFile.seek((page-1)*pageSize+4);
			value = rAFile.readInt();
		}
		catch(Exception e)
		{
			System.out.println("Error in getRight");
		}
		return value;
	}

	public static void setRight(RandomAccessFile rAFile, int page, int right)
	{
		try
		{
			rAFile.seek((page-1)*pageSize+4);
			rAFile.writeInt(right);
		}
		catch(Exception e)
		{
			System.out.println("Error in setRight");
		}
	}

	// Get number of cells
	public static byte getCellNo(RandomAccessFile rAFile, int page)
	{
		byte valueByte = 0;
		try
		{
			rAFile.seek((page-1)*pageSize+1);
			valueByte = rAFile.readByte();
		}
		catch(Exception e)
		{
			System.out.println(e);
			System.out.println("Error at getCellNo");
		}
		return valueByte;
	}

	public static void setCellNo(RandomAccessFile rAFile, int page, byte numByte)
	{
		try
		{
			rAFile.seek((page-1)*pageSize+1);
			rAFile.writeByte(numByte);
		}
		catch(Exception e)
		{
			System.out.println("Error at setCellNo");
		}
	}

	public static boolean checkInteriorSpace(RandomAccessFile rAFile, int page)
	{
		byte numberOfCells = getCellNo(rAFile, page);
		if(numberOfCells > 30)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	// 
	public static int checkLeafSpace(RandomAccessFile rAFile, int page, int size)
	{
		int value = -1;
		try
		{
			rAFile.seek((page-1)*pageSize+2);
			int contentSize = rAFile.readShort();
			if(contentSize == 0)
			{	
				return pageSize - size;
			}
			int numberOfCells = getCellNo(rAFile, page);
			int spaces = contentSize - 20 - 2*numberOfCells;
			if(size < spaces)
			{	
				return contentSize - size;
			}
			
		}
		catch(Exception e)
		{
			System.out.println("Error at checkLeafSpace");
		}

		return value;
	}

	// Check presence of key
	public static boolean hasKey(RandomAccessFile rAFile, int page, int key)
	{
		int[] arrayTemp = findKeyArray(rAFile, page);
		for(int i : arrayTemp)
		{	if(key == i)
			{
				return true;
			}
		}
		return false;
	}

	// Get Cell's location
	public static long getCellLocation(RandomAccessFile rAFile, int page, int id)
	{
		long location = 0;
		try
		{
			rAFile.seek((page-1)*pageSize+12+id*2);
			short offset = rAFile.readShort();
			long orig = (page-1)*pageSize;
			location = orig + offset;
		}
		catch(Exception e)
		{
			System.out.println("Error at getCellLocation");
		}
		return location;
	}

	public static short getCellOffset(RandomAccessFile rAFile, int page, int id)
	{
		short offset = 0;
		try
		{
			rAFile.seek((page-1)*pageSize+12+id*2);
			offset = rAFile.readShort();
		}
		catch(Exception e)
		{
			System.out.println("Error at getCellOffset");
		}
		return offset;
	}

	public static void setCellOffset(RandomAccessFile rAFile, int page, int id, int offset)
	{
		try
		{
			rAFile.seek((page-1)*pageSize+12+id*2);
			rAFile.writeShort(offset);
		}
		catch(Exception e)
		{
			System.out.println("Error at setCellOffset");
		}
	}
}