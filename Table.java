package database;

import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table
{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;

	// To show all tables
	public static void show()
	{
		String[] columns = {"table_name"};
		String[] comp = new String[0];
		String table = "davisbase_tables";
		select("data\\catalog\\"+table+".tbl",table, columns, comp);
	}
	
	// To show databases
	public static void showDataBase()
	{
		
		File f1= new File("data");
		String[] listDirectory = f1.list();
		
		for(String i:listDirectory)
		{
			if(i.equals("catalog") || i.equals("user_data"))
			{	
				continue;
			}
			System.out.println(i);
		}
		
	}

	// To drop a table
	public static void drop(String table,String dbStr)
	{
		try
		{
			RandomAccessFile rAFile = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			int numberOfPages = pages(rAFile);
			for(int page = 1; page <= numberOfPages; page ++)
			{
				rAFile.seek((page-1)*pageSize);
				byte typeB = rAFile.readByte();
				if(typeB == 0x05)
				{
					continue;
				}
				else
				{
					short[] cells = Page.findCellArray(rAFile, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++)
					{
						long location = Page.getCellLocation(rAFile, page, j);
						String[] pL = getPayload(rAFile, location);
						String tB = pL[1];
						if(!tB.equals(DavisBase.currentDataBase+"."+table))
						{
							Page.setCellOffset(rAFile, page, i, cells[j]);
							i++;
						}
					}
					Page.setCellNo(rAFile, page, (byte)i);
				}
			}

			rAFile = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			numberOfPages = pages(rAFile);
			for(int page = 1; page <= numberOfPages; page ++)
			{
				rAFile.seek((page-1)*pageSize);
				byte typeB = rAFile.readByte();
				if(typeB == 0x05)
				{
					continue;
				}
				else
				{
					short[] cells = Page.findCellArray(rAFile, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++)
					{
						long location = Page.getCellLocation(rAFile, page, j);
						String[] pL = getPayload(rAFile, location);
						String tB = pL[1];
						if(!tB.equals(DavisBase.currentDataBase+"."+table))
						{
							Page.setCellOffset(rAFile, page, i, cells[j]);
							i++;
						}
					}
					Page.setCellNo(rAFile, page, (byte)i);
				}
			}
			rAFile.close();
			File dropTable = new File("data\\"+dbStr+"\\"+table);
			String[] listFiles = dropTable.list();
			for(String f:listFiles)
			{
				File dropFile = new File("data\\"+dbStr+"\\"+table,f);
				dropFile.delete();
			}
			dropTable = new File("data\\"+dbStr, table); 
			dropTable.delete();
		}
		catch(Exception e)
		{
			System.out.println("Error at drop");
			System.out.println(e);
		}
	}

	public static void dropDataBase(String databaseStr)
	{
		File f1= new File("data\\"+databaseStr);
		String[] listDir = f1.list();
		
		for(String i:listDir)
		{
			if(i.equals("catalog") || i.equals("user_data"))
			{
				continue;
			}
			drop(i,databaseStr);
		}
		File dropFile = new File("data", databaseStr); 
		dropFile.delete();
	}

	public static void createDataBase(String databaseStr)
	{
		try 
		{	
			File dbFile = new File("data\\"+databaseStr);
			
			if(dbFile.exists())
			{
				System.out.println("Database already exists");
				return;
			}
			dbFile.mkdir();
			DavisBase.currentDataBase=databaseStr;
			
			System.out.println("Database "+databaseStr+" created successfully.");
		}
		catch (SecurityException se) 
		{
			System.out.println("Cannot create catalog:"+se);
		}
	}
	
	public static boolean checkDataBase(String databaseStr)
	{
		File catalog = new File("data\\"+databaseStr);
		
		if(catalog.exists())
		{
			DavisBase.currentDataBase=databaseStr;	
			return true;
		}
		return false;
	}

	public static String[] getPayload(RandomAccessFile file, long location)
	{
		String[] payload = new String[0];
		try
		{
			Long tmpL;
			SimpleDateFormat formater = new SimpleDateFormat (datePattern);

			file.seek(location);
			int plSize = file.readShort();
			int key = file.readInt();
			int numberColumns = file.readByte();
			byte[] stc = new byte[numberColumns];
			int temp = file.read(stc);
			payload = new String[numberColumns+1];
			payload[0] = Integer.toString(key);
			for(int i=1; i <= numberColumns; i++)
			{
				switch(stc[i-1])
				{
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmpL = file.readLong();
								Date dateTime = new Date(tmpL);
								payload[i] = formater.format(dateTime);
								break;

					case 0x0B:  tmpL = file.readLong();
								Date date = new Date(tmpL);
								payload[i] = formater.format(date).substring(0,10);
								break;

					default:    int leng = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[leng];
								for(int j = 0; j < leng; j++)
								{	
									bytes[j] = file.readByte();
								}
								payload[i] = new String(bytes);
								break;
				}
			}

		}
		catch(Exception e)
		{
			System.out.println("Error at getPayLoad");
		}
		return payload;
	}


	public static void createTable(String table, String[] colStr)
	{
		try
		{	
			File catalogFile = new File("data\\"+DavisBase.currentDataBase+"\\"+table);
			
			catalogFile.mkdir();
			RandomAccessFile file = new RandomAccessFile("data\\"+DavisBase.currentDataBase+"\\"+table+"\\"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			
			file = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			int numberPages = pages(file);
			int page = 1;
			for(int p = 1; p <= numberPages; p++)
			{
				int rm = Page.getRight(file, p);
				if(rm == 0)
				{
					page = p;
				}
			}
			int[] keyArray = Page.findKeyArray(file, page);
			int l = keyArray[0];
			for(int i = 0; i < keyArray.length; i++)
			{	
				if(l < keyArray[i])
				{
					l = keyArray[i];
				}
			}
			file.close();
			String[] values = {Integer.toString(l+1), DavisBase.currentDataBase+"."+table};
			insert("davisbase_tables", values);

			RandomAccessFile cfile = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {};
			filter(cfile, cmp, columnName, buffer);
			l = buffer.content.size();

			for(int i = 0; i < colStr.length; i++)
			{
				l = l + 1;
				String[] token = colStr[i].split(" ");
				String n = "YES";
				if(token.length > 2)
				{
					n = "NO";
				}
				String colName = token[0];
				String dt = token[1].toUpperCase();
				String position = Integer.toString(i+1);
				String[] v = {Integer.toString(l), DavisBase.currentDataBase+"."+table, colName, dt, position, n};
				insert("davisbase_columns", v);
			}
			cfile.close();
			file.close();
		}
		catch(Exception e)
		{
			System.out.println("Error at createTable");
			e.printStackTrace();
		}
	}

	public static void update(String table, String[] set, String[] comp)
	{
		try
		{
			List<Integer> key = new ArrayList<Integer>();
			RandomAccessFile file = new RandomAccessFile("data\\"+DavisBase.currentDataBase+"\\"+table+"\\"+table+".tbl", "rw");	
			Buffer buff = new Buffer();
			String[] columnName = findColumnName(table);
			String[] type = findDataType(table);
			filter(file, comp, columnName, type, buff);
			
			for(String[] i : buff.content.values())
			{
				for(int j = 0; j < i.length; j++)
				{
					if(buff.columnName[j].equals(comp[0]) && i[j].equals(comp[2]))
					{
						key.add(Integer.parseInt(i[0]));							
						break;
					}
				}
			}
				
			for(int indKey:key)
			{
				int numPages = pages(file);
				int page = 1;
	
				for(int p = 1; p <= numPages; p++)
				{	
					if(Page.hasKey(file, p, indKey))
					{
						page = p;
					}
				}
				int[] array = Page.findKeyArray(file, page);
				int id = 0;
				for(int i = 0; i < array.length; i++)
				{
					if(array[i] == indKey)
					{
						id = i;
					}
				}
				int offset = Page.getCellOffset(file, page, id);
				long location = Page.getCellLocation(file, page, id);
				String[] array_s = findColumnName(table);
				String[] values = getPayload(file, location);
	
	
				
				for(int i=0; i < type.length; i++)
				{
					if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					{
						values[i] = "'"+values[i]+"'";
					}
				}
	
				for(int i = 0; i < array_s.length; i++)
				{	
					if(array_s[i].equals(set[0]))
					{
						id = i;
					}
				}
				values[id] = set[2];
	
				String[] nullable = getNullable(table);
	
				for(int i = 0; i < nullable.length; i++)
				{
					if(values[i].equals("null") && nullable[i].equals("NO"))
					{
						System.out.println("NULL value constraint violation!");
						System.out.println();
						return;
					}
				}
				
				byte[] stc = new byte[array_s.length-1];
				int plSize = calculatePayloadSize(table, values, stc);
				Page.updateLeaf(file, page, offset, plSize, indKey, stc, values,table);
			}
			file.close();

		}
		catch(Exception e)
		{
			System.out.println("Error at update");
			System.out.println(e);
		}
	}

	public static void insert(RandomAccessFile file, String table, String[] values)
	{
		String[] dType = findDataType(table);
		String[] nullable = getNullable(table);

		for(int i = 0; i < nullable.length; i++)
		{
			if(values[i].equals("null") && nullable[i].equals("NO"))
			{
				System.out.println("NULL value constraint violation!");
				System.out.println();
				return;
			}
		}

		int key = new Integer(values[0]);
		int page = searchKey(file, key);
		if(page != 0)
		{
			if(Page.hasKey(file, page, key))
			{
				System.out.println("Uniqueness constraint violation!");
				System.out.println();
				return;
			}
		}
		if(page == 0)
		{
			page = 1;
		}

		byte[] stc = new byte[dType.length-1];
		short plSize = (short) calculatePayloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Page.checkLeafSpace(file, page, cellSize);

		if(offset != -1)
		{
			Page.insertLeaf(file, page, offset, plSize, key, stc, values,table);
		}
		else
		{
			Page.splitLeaf(file, page);
			insert(file, table, values);
		}
	}

	public static void insert(String table, String[] values)
	{
		try
		{
			RandomAccessFile file = new RandomAccessFile("data\\catalog\\"+table+".tbl", "rw");
			insert(file, table, values);
			file.close();

		}
		catch(Exception e)
		{
			System.out.println("Cannot insert data.");
			e.printStackTrace();
		}
	}

	// get Payload size
	public static int calculatePayloadSize(String table, String[] vals, byte[] stc)
	{
		String[] dataType = findDataType(table);
		int size = 1;
		size = size + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++)
		{
			byte tmp = getstcCode(vals[i], dataType[i]);
			stc[i - 1] = tmp;
			size = size + fieldLength(tmp);
		}
		return size;
	}

	// get STC code value length
	public static short fieldLength(byte stc)
	{
		switch(stc)
		{
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}

	// get STC
	public static byte getstcCode(String val, String dataType)
	{
		if(val.equals("null"))
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}
		else
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int searchKey(RandomAccessFile file, int key)
	{
		int val = 1;
		try
		{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++)
			{
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D)
				{
					int[] keys = Page.findKeyArray(file, page);
					if(keys.length == 0)
					{
						return 0;
					}
					int rM = Page.getRight(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1])
					{
						return page;
					}
					else if(rM == 0 && keys[keys.length - 1] < key)
					{
						return page;
					}
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at searchKey");
			System.out.println(e);
		}

		return val;
	}


	public static String[] findDataType(String table)
	{
		String[] dataType = new String[0];
		try
		{
			RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Buffer buff = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
			{
				table = DavisBase.currentDataBase+"."+table;
			}
			String[] comp = {"table_name","=",table};
			filter(file, comp, columnName, buff);
			HashMap<Integer, String[]> content = buff.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values())
			{
				array.add(i[3]);
			}
			dataType = array.toArray(new String[array.size()]);
			file.close();
			return dataType;
		}
		catch(Exception e)
		{
			System.out.println("Error in findDataType");
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] findColumnName(String table)
	{
		String[] c = new String[0];
		try
		{
			RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Buffer buff = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
			{
				table = DavisBase.currentDataBase+"."+table;
			}
			String[] comp = {"table_name","=",table};
			filter(file, comp, columnName, buff);
			HashMap<Integer, String[]> content = buff.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values())
			{
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			file.close();
			return c;
		}
		catch(Exception e)
		{
			System.out.println("Error in findColumnName");
			System.out.println(e);
		}
		return c;
	}

	public static String[] getNullable(String table)
	{
		String[] n = new String[0];
		try
		{
			RandomAccessFile file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Buffer buff = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
			{
				table = DavisBase.currentDataBase+"."+table;
			}
			String[] comp = {"table_name","=",table};
			filter(file, comp, columnName, buff);
			HashMap<Integer, String[]> content = buff.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values())
			{
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}
		catch(Exception e)
		{
			System.out.println("Error at getNullable");
			System.out.println(e);
		}
		return n;
	}

	public static void select(String file, String table, String[] cols, String[] comp)
	{
		try
		{
			Buffer buff = new Buffer();
			String[] columnName = findColumnName(table);
			String[] type = findDataType(table);

			RandomAccessFile rFile = new RandomAccessFile(file, "rw");
			filter(rFile, comp, columnName, type, buff);
			buff.display(cols);
			rFile.close();
		}
		catch(Exception e)
		{
			System.out.println("Error at select");
			System.out.println(e);
		}
	}

	public static void delete(String table, String[] comp)
	{
			
		try 
		{
			int key = -1;
			RandomAccessFile file = new RandomAccessFile("data\\"+DavisBase.currentDataBase+"\\"+table+"\\"+table+".tbl", "rw");
			Buffer buff = new Buffer();
			String[] columnName = findColumnName(table);
			String[] type = findDataType(table);
			filter(file, comp, columnName, type, buff);
			boolean flag=false;
			for(String[] i : buff.content.values())
			{
				if(flag)
				{
					break;
				}
				for(int j = 0; j < i.length; j++)
				{
					if(buff.columnName[j].equals(comp[0]) && i[j].equals(comp[2]))
					{
						key =(Integer.parseInt(i[0]));							
						flag = true;
						break;
					}
				}
			}
				
			int numPages = pages(file);
			int page = 1;

			for(int p = 1; p <= numPages; p++) 
			{
				if(Page.hasKey(file, p, key))
				{
					page = p;
				}
			}
			int[] array = Page.findKeyArray(file, page);
			int id = 0;
			for(int i = 0; i < array.length; i++)
			{
				if(array[i] == key)
				{
					id = i;
				}
			}
			int offset = Page.getCellOffset(file, page, id);
			long location = Page.getCellLocation(file, page, id);
			String[] array_s = findColumnName(table);
		
			String[] values = getPayload(file, location);

			byte[] stc = new byte[array_s.length-1];
			int plSize = calculatePayloadSize(table, values, stc);
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plSize);
			file.writeInt(-10000);
			
			file.close();

		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		
	}
	// Filter
	public static void filter(RandomAccessFile file, String[] comp, String[] columnName, String[] type, Buffer buff)
	{
		try
		{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++)
			{
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
				{
					continue;
				}
				else
				{
					byte numCells = Page.getCellNo(file, page);

					for(int i=0; i < numCells; i++)
					{
						long loc = Page.getCellLocation(file, page, i);
						file.seek(loc+2);
						int rowid = file.readInt();
						
						String[] payload = getPayload(file, loc);

						for(int j=0; j < type.length; j++)
						{
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
							{
								payload[j] = "'"+payload[j]+"'";
							}
						}

						boolean check = compCheck(payload, rowid, comp, columnName);

						for(int j=0; j < type.length; j++)
						{
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
							{
								payload[j] = payload[j].substring(1, payload[j].length()-1);
							}
						}
						if(check)
						{
							buff.add(rowid, payload);
						}
					}
				}
			}

			buff.columnName = columnName;
			buff.format = new int[columnName.length];

		}
		catch(Exception e)
		{
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	public static void filter(RandomAccessFile file, String[] comp, String[] columnName, Buffer buff)
	{
		try
		{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++)
			{
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
				{
					continue;
				}
				else
				{
					byte numCells = Page.getCellNo(file, page);

					for(int i=0; i < numCells; i++)
					{
						long location = Page.getCellLocation(file, page, i);
						file.seek(location+2);
						int rowid = file.readInt();
						String[] payload = getPayload(file, location);

						boolean check = compCheck(payload, rowid, comp, columnName);
						if(check)
						{
							buff.add(rowid, payload);
						}
					}
				}
			}

			buff.columnName = columnName;
			buff.format = new int[columnName.length];

		}
		catch(Exception e)
		{
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	// Get number of pages
	public static int pages(RandomAccessFile file)
	{
		int num_pages = 0;
		try
		{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}
		catch(Exception e)
		{
			System.out.println("Error at pages");
		}

		return num_pages;
	}

	public static boolean compCheck(String[] payload, int rowid, String[] comp, String[] columnName)
	{

		boolean check = false;
		if(comp.length == 0)
		{
			check = true;
		}
		else
		{
			int colPosition = 1;
			for(int i = 0; i < columnName.length; i++)
			{
				if(columnName[i].equals(comp[0]))
				{
					colPosition = i + 1;
					break;
				}
			}
			String opt = comp[1];
			String val = comp[2];
			if(colPosition == 1)
			{
				switch(opt)
				{
					case "=": if(rowid == Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}
			else
			{
				if(val.equals(payload[colPosition-1]))
				{
					check = true;
				}
				else
				{
					check = false;
				}
			}
		}
		return check;
	}

	public static void initialize() 
	{

		// Create data directory
		try 
		{
			File catalog = new File("data\\catalog");
			String[] oldTableFiles;
			oldTableFiles = catalog.list();
			for (int i=0; i<oldTableFiles.length; i++) 
			{
				File anOldFile = new File(catalog, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException se) 
		{
			System.out.println("Cannot create catalog directory :"+se);	
		}

		try 
		{
			davisbaseTablesCatalog = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x02);
			int[] offset=new int[2];
			int sizeA=24;
			int sizeB=25;
			offset[0]=pageSize-sizeA;
			offset[1]=offset[0]-sizeB;
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeInt(0);
			davisbaseTablesCatalog.writeInt(10);
			davisbaseTablesCatalog.writeShort(offset[1]);
			davisbaseTablesCatalog.writeShort(offset[0]);
			davisbaseTablesCatalog.seek(offset[0]);
			davisbaseTablesCatalog.writeShort(20);
			davisbaseTablesCatalog.writeInt(1); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(28);
			davisbaseTablesCatalog.writeBytes("davisbase_tables");
			davisbaseTablesCatalog.seek(offset[1]);
			davisbaseTablesCatalog.writeShort(21);
			davisbaseTablesCatalog.writeInt(2); 
			davisbaseTablesCatalog.writeByte(1);
			davisbaseTablesCatalog.writeByte(29);
			davisbaseTablesCatalog.writeBytes("davisbase_columns");
		}
		catch (Exception e) 
		{
			System.out.println("Cannot create database_tables.");
			System.out.println(e);
		}
		
		try 
		{
			davisbaseColumnsCatalog = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       
			davisbaseColumnsCatalog.writeByte(0x0D);
			davisbaseColumnsCatalog.writeByte(0x08);
			int[] offset=new int[10];
			offset[0]=pageSize-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			offset[8]=offset[7]-49;
			davisbaseColumnsCatalog.writeShort(offset[8]);
			davisbaseColumnsCatalog.writeInt(0);
			davisbaseColumnsCatalog.writeInt(0);
			for(int i=0;i<9;i++)
			{
				davisbaseColumnsCatalog.writeShort(offset[i]);
			}

			davisbaseColumnsCatalog.seek(offset[0]);
			davisbaseColumnsCatalog.writeShort(33);
			davisbaseColumnsCatalog.writeInt(1); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[1]);
			davisbaseColumnsCatalog.writeShort(39);
			davisbaseColumnsCatalog.writeInt(2); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_tables");
			davisbaseColumnsCatalog.writeBytes("table_name");  
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[2]);
			davisbaseColumnsCatalog.writeShort(34);
			davisbaseColumnsCatalog.writeInt(3); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(17);
			davisbaseColumnsCatalog.writeByte(15);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("rowid");
			davisbaseColumnsCatalog.writeBytes("INT");
			davisbaseColumnsCatalog.writeByte(1);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[3]);
			davisbaseColumnsCatalog.writeShort(40);
			davisbaseColumnsCatalog.writeInt(4); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(22);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("table_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(2);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[4]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(5); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("column_name");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(3);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[5]);
			davisbaseColumnsCatalog.writeShort(39);
			davisbaseColumnsCatalog.writeInt(6); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(21);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("data_type");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[6]);
			davisbaseColumnsCatalog.writeShort(49);
			davisbaseColumnsCatalog.writeInt(7); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(28);
			davisbaseColumnsCatalog.writeByte(19);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("ordinal_position");
			davisbaseColumnsCatalog.writeBytes("TINYINT");
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeBytes("NO");
			
			davisbaseColumnsCatalog.seek(offset[7]);
			davisbaseColumnsCatalog.writeShort(41);
			davisbaseColumnsCatalog.writeInt(8); 
			davisbaseColumnsCatalog.writeByte(5);
			davisbaseColumnsCatalog.writeByte(29);
			davisbaseColumnsCatalog.writeByte(23);
			davisbaseColumnsCatalog.writeByte(16);
			davisbaseColumnsCatalog.writeByte(4);
			davisbaseColumnsCatalog.writeByte(14);
			davisbaseColumnsCatalog.writeBytes("davisbase_columns");
			davisbaseColumnsCatalog.writeBytes("is_nullable");
			davisbaseColumnsCatalog.writeBytes("TEXT");
			davisbaseColumnsCatalog.writeByte(6);
			davisbaseColumnsCatalog.writeBytes("NO");
		}
		catch (Exception e) 
		{
			System.out.println("Cannot create database_columns file.");
			System.out.println(e);
		}
	}
}


