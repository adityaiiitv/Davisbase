package database;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Scanner;

public class DavisBase 
{
	static String currentDataBase = "user_data";
	static Scanner inputSc = new Scanner(System.in).useDelimiter(";");
	static String prompt = "davisql> ";
	
	public static void main(String[] args) 
	{
		initialize();
		String query = "";	// Input
		System.out.println("DavisBase Started");
		System.out.println("Type help; to see all supported commands.");
		while (!query.equals("exit")) 
		{
			System.out.print(prompt);
			query = inputSc.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseQuery(query);
		}
		System.out.println("Davisbase closed.");
	}

	public static void initialize() 
	{
		try 
		{
			File dataFile = new File("data");
			if (!dataFile.exists()) 
			{
				dataFile.mkdir();
			}
			File dataBaseCatalog = new File("data\\catalog");
			if (dataBaseCatalog.mkdir()) 
			{
				System.out.println("Catalog created.");
				Table.initialize();
			} 
			else 
			{
				String metaColumns = "davisbase_columns.tbl";
				String metaTables = "davisbase_tables.tbl";
				String[] allTables = dataBaseCatalog.list();
				boolean catalogBool = false;
				
				for (int i = 0; i < allTables.length; i++) 
				{
					if (allTables[i].equals(metaColumns))
					{	
						catalogBool = true;
					}
				}
				if (!catalogBool) 
				{
					System.out.println("Creating davisbase_columns.");
					System.out.println();
					Table.initialize();
				}
				catalogBool = false;
				for (int i = 0; i < allTables.length; i++) 
				{
					if (allTables[i].equals(metaTables))
					{
						catalogBool = true;
					}
				}
				if (!catalogBool) 
				{
					System.out.println("Creating davisbase_tables.");
					System.out.println();
					Table.initialize();
				}
			}
		} 
		catch (SecurityException se) 
		{
			System.out.println("Failed to create catalog." + se);
		}
	}

	
	public static void parseQuery(String query) 
	{

		String[] queryTokens = query.split(" ");

		switch (queryTokens[0]) 
		{
		
			case "use":
			if(queryTokens[1].equals(""))
			{
				
				System.out.println("Encountered a problem in input. Try again. Press help; for supported commands.");
			}
			else{
				if(!Table.checkDataBase(queryTokens[1]))
				{
					System.out.println("Database not present.");
					System.out.println();
					break;
				}
				currentDataBase=queryTokens[1];
				System.out.println("using "+currentDataBase);
			}
			break;
			case "create":
			if(queryTokens[1].equals("table"))
			{
				String createTable = queryTokens[2];
				String[] createTemp = query.split(createTable);
				String columnTemp = createTemp[1].trim();
				String[] createColumns = columnTemp.substring(1, columnTemp.length() - 1).split(",");
				for (int i = 0; i < createColumns.length; i++)
				{	
					createColumns[i] = createColumns[i].trim();
				}
				if (tablePresent(createTable)) 
				{
					System.out.println("Table " + createTable + " already present.");
					System.out.println();
					break;
				}
				Table.createTable(createTable, createColumns);
				System.out.println("Table "+createTable+" created.");
				
			}
			else if(queryTokens[1].equals("database"))
			{
				String createDataBase = queryTokens[2];
			
				Table.createDataBase(createDataBase);
				
			}
			else
			{
				System.out.println("Encountered a problem in input. Try again. Press help; for supported commands.");				
			}
			
			break;

			case "insert":
			String insertValues = query.split("values")[1].trim();
			insertValues = insertValues.substring(1, insertValues.length() - 1);
			String[] insert_Values = insertValues.split(",");
			String insertTable = queryTokens[2];
			for (int i = 0; i < insert_Values.length; i++)
			{	
				insert_Values[i] = insert_Values[i].trim();
			}
			if (!tablePresent(insertTable)) 
			{
				System.out.println("Table " + insertTable + " not present.");
				System.out.println();
				break;
			}
			RandomAccessFile file;
			try 
			{
				file = new RandomAccessFile("data\\"+currentDataBase+"\\"+insertTable+"\\"+insertTable+".tbl", "rw");
				Table.insert(file,insertTable, insert_Values);
			} 
			catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
			
			break;
			
			case "select":
			String[] selectTemp1 = query.split("where");
			String[] selectQuery = selectTemp1[0].split("from");
			String selectTable = selectQuery[1].trim();
			String selectColumns = selectQuery[0].replace("select", "").trim();
			String[] selectCompare;
			String[] selectColumn;
			
			if(selectTable.equals("davisbase_tables"))
			{
				if (selectColumns.contains("*")) 
				{
					selectColumn = new String[1];
					selectColumn[0] = "*";
				} 
				else 
				{
					selectColumn = selectColumns.split(",");
					for (int i = 0; i < selectColumn.length; i++)
					{	
						selectColumn[i] = selectColumn[i].trim();
					}
				}
				if (selectTemp1.length > 1) 
				{
					String trimmer = selectTemp1[1].trim();
					selectCompare = parserEqn(trimmer);
				} 
				else 
				{
					selectCompare = new String[0];
				}
				Table.select("data\\catalog\\davisbase_tables.tbl", selectTable, selectColumn, selectCompare);
				System.out.println();
				break;
			}
			
			else if(selectTable.equals("davisbase_columns"))
			{
				if (selectColumns.contains("*")) 
				{
					selectColumn = new String[1];
					selectColumn[0] = "*";
				} 
				else 
				{
					selectColumn = selectColumns.split(",");
					for (int i = 0; i < selectColumn.length; i++)
					{	
						selectColumn[i] = selectColumn[i].trim();
					}
				}
				if (selectTemp1.length > 1) 
				{
					String trimmer = selectTemp1[1].trim();
					selectCompare = parserEqn(trimmer);
				} 
				else 
				{
					selectCompare = new String[0];
				}
				Table.select("data\\catalog\\davisbase_columns.tbl", selectTable, selectColumn, selectCompare);
				System.out.println();
				break;
			}

			else
			{
				if(!tablePresent(selectTable)) 
				{
					System.out.println("Incorrect table name.");
					System.out.println("Table " + selectTable + " not present.");
					System.out.println();
					break;
				}
			}

			if (selectTemp1.length > 1) 
			{
				String trimmer = selectTemp1[1].trim();
				selectCompare = parserEqn(trimmer);
			} 
			else 
			{
				selectCompare = new String[0];
			}

			if (selectColumns.contains("*")) 
			{
				selectColumn = new String[1];
				selectColumn[0] = "*";
			} 
			else 
			{
				selectColumn = selectColumns.split(",");
				for (int i = 0; i < selectColumn.length; i++)
				{	
					selectColumn[i] = selectColumn[i].trim();
				}
			}
			
			Table.select("data\\"+currentDataBase+"\\"+selectTable+"\\"+selectTable+".tbl", selectTable, selectColumn, selectCompare);
			System.out.println();
			break;	
			
			case "drop":
			if(queryTokens[1].equals("table"))
			{
				String dropTable = queryTokens[2];
				if (!tablePresent(dropTable)) 
				{
					System.out.println("Table " + dropTable + " not present.");
					System.out.println();
					break;
				}
				Table.drop(dropTable,currentDataBase);
				System.out.println("Table "+dropTable+" dropped.");
			}
			else if(queryTokens[1].equals("database"))
			{
				String dropDataBase = queryTokens[2];
				if (!Table.checkDataBase(dropDataBase)) 
				{
					System.out.println("Database " + dropDataBase + " not present.");
					System.out.println();
					break;
				}
				Table.dropDataBase(dropDataBase);
				System.out.println("Database "+dropDataBase+" dropped.");
			}
			else
			{
				System.out.println("Encountered a problem in input. Try again. Press help; for supported commands.");
			}
			System.out.println();
			break;

			case "show":
			String command = queryTokens[1];
			System.out.println();
			if(command.equals("tables"))
			{
				Table.show();
			}
			else if(command.equals("databases"))
			{
				Table.showDataBase();
			}
			System.out.println();
			break;

			case "delete":
			String[] deleteTemp = query.split("where");
			String[] deleteQuery = deleteTemp[0].split("from");
			String deleteTable = deleteQuery[1].trim();
			String[] deleteCompare = null;
			if(!tablePresent(deleteTable)) 
			{	
				System.out.println("Incorrect table name.");
				System.out.println("Table " + deleteTable + " doesn't exist.");
				System.out.println();
				break;
			}
			
			if (deleteTemp.length > 1) 
			{
				String trimmer = deleteTemp[1].trim();  
				deleteCompare = parserEqn(trimmer);
			} 
			else 
			{
				deleteCompare = new String[0];
			}
			Table.delete(deleteTable, deleteCompare);
			System.out.println();
			break;
		
			case "update":
			String[] updateTempA = query.split("set");
			String[] updateTempB = updateTempA[1].split("where");
			String updateCompareS = updateTempB[1];
			String updateSetS = updateTempB[0];
			String updateTable = queryTokens[1];
			String[] set = parserEqn(updateSetS);
			String[] updateCompare = parserEqn(updateCompareS);
			if (!tablePresent(updateTable)) 
			{
				System.out.println("Table " + updateTable + " not present.");
				System.out.println();
				break;
			}
			Table.update(updateTable, set, updateCompare);
			System.out.println("Table "+updateTable+" updated.");
			System.out.println();
			break;

			case "help":
			System.out.println();
			System.out.println("Supported commands:");
			System.out.println("\t(a) SHOW TABLES;                                               	Shows all tables");
			System.out.println("\t(b) SHOW DATABASES;                                            	Shows all databases");
			System.out.println("\t(c) CREATE TABLE table_name;                                     	To create a new table");
			System.out.println("\t(d) DROP TABLE table_name;                                       	To delete a table and its data");
			System.out.println("\t(e) CREATE DATABASE database_name;                             	Creates a database");
			System.out.println("\t(f) DROP DATABASE database_name;                             		Deletes a database");
			System.out.println("\t(g) HELP;                                                    	 	Show the list of supported commands");
			System.out.println("\t(h) INSERT INTO table_name (columns) VALUES (values);        		Insert a record into the table");
			System.out.println("\t(i) DELETE FROM TABLE table_name WHERE condition;             	Delete a particular record");
			System.out.println("\t(j) UPDATE table_name SET column_name = value WHERE condition;   	Update a record");
			System.out.println("\t(k) SELECT * FROM table_name;                                    	Show all records");
			System.out.println("\t(l) SELECT * FROM table_name WHERE rowid = value;              	Show records for a condition");
			System.out.println("\t(m) EXIT;                                                    		Quit");
			System.out.println();
			break;

			case "exit":
			System.out.println();
			break;

			default:
			System.out.println();
			System.out.println("Encountered a problem in input. Try again. Press help; for supported commands.");
			System.out.println();
			break;
		}
	}
	
	public static String[] parserEqn(String equation) 
	{
		String tempArray[] = new String[2];
		String compare[] = new String[3];
		if (equation.contains("=")) 
		{
			tempArray = equation.split("=");
			compare[0] = tempArray[0].trim();
			compare[1] = "=";
			compare[2] = tempArray[1].trim();
		}

		if (equation.contains(">")) 
		{
			tempArray = equation.split(">");
			compare[0] = tempArray[0].trim();
			compare[1] = ">";
			compare[2] = tempArray[1].trim();
		}

		if (equation.contains("<")) 
		{
			tempArray = equation.split("<");
			compare[0] = tempArray[0].trim();
			compare[1] = "<";
			compare[2] = tempArray[1].trim();
		}

		if (equation.contains(">=")) 
		{
			tempArray = equation.split(">=");
			compare[0] = tempArray[0].trim();
			compare[1] = ">=";
			compare[2] = tempArray[1].trim();
		}

		if (equation.contains("<=")) 
		{
			tempArray = equation.split("<=");
			compare[0] = tempArray[0].trim();
			compare[1] = "<=";
			compare[2] = tempArray[1].trim();
		}

		if (equation.contains("<>")) 
		{
			tempArray = equation.split("<>");
			compare[0] = tempArray[0].trim();
			compare[1] = "<>";
			compare[2] = tempArray[1].trim();
		}

		return compare;
	}
	
	// To check the presence of table.
	public static boolean tablePresent(String tableStr) 
	{
		boolean tableFlag = false;
		try 
		{
			File userTables = new File("data\\"+currentDataBase);
			if (userTables.mkdir()) 
			{
				System.out.println("Creating user_data.");
			}
			String[] allTables;
			allTables = userTables.list();
			for (int i = 0; i < allTables.length; i++) 
			{
				if (allTables[i].equals(tableStr))
				{
					return true;
				}
			}
		} 
		catch (SecurityException se) 
		{
			System.out.println("Cannot create data directory." + se);
		}
		return tableFlag;
	}

}