package database;
import java.util.HashMap;

class Buffer
{
	public int[] format;
	public String[] columnName;
	public HashMap<Integer, String[]> content;
	public int noOfRows;
	
	// Display
	public void display(String[] col)
	{
		if(noOfRows == 0)
		{
			System.out.println("");
		}
		else
		{
			updatePattern();
			if(col[0].equals("*"))
			{
				for(int l: format)
				{
					System.out.print(line("_", l+3));
				}
				System.out.println();
				for(int j = 0; j < columnName.length; j++)
				{
					System.out.print(fix(format[j], columnName[j]));
				}
				System.out.println();
				for(int l: format)
				{
					System.out.print(line("_", l+3));
				}
				System.out.println();
				for(String[] i : content.values())
				{
					if(i[0].equals("-10000"))
					{
						continue;
					}
					for(int j = 0; j < i.length; j++)
					{
						System.out.print(fix(format[j], i[j]));
					}
					System.out.println();
				}
				System.out.println();
			}
			else
			{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
				{
					for(int i = 0; i < columnName.length; i++)
					{
						if(col[j].equals(columnName[i]))
						{
							control[j] = i;
						}
					}
				}
				for(int j = 0; j < control.length; j++)
				{
					System.out.print(line("_", format[control[j]]+3));
				}
				System.out.println();
				for(int j = 0; j < control.length; j++)
				{
					System.out.print(fix(format[control[j]], columnName[control[j]]));
				}
				System.out.println();
				for(int j = 0; j < control.length; j++)
				{
					System.out.print(line("_", format[control[j]]+3));
				}
				System.out.println();
				for(String[] i : content.values())
				{
					for(int j = 0; j < control.length; j++)
					{
						System.out.print(fix(format[control[j]], i[control[j]]));
					}
					System.out.println();
				}
				System.out.println();
			}
		}
	}

	public String line(String str,int len) 
	{
		String l = "";
		for(int i=0;i<len;i++) 
		{
			l = l + str;
		}
		return l;
	}
	
	
	public String fix(int len, String s) 
	{
		return String.format("%-"+(len+3)+"s", s);
	}
	
	
	
	// Update
	public void updatePattern()
	{
		for(int i = 0; i < format.length; i++)
		{
			format[i] = columnName[i].length();
		}
		for(String[] i : content.values())
		{
			for(int j = 0; j < i.length; j++)
			{
				if(format[j] < i[j].length())
				{
					format[j] = i[j].length();
				}
			}
		}
	}
	
	
	public Buffer()
	{
		content = new HashMap<Integer, String[]>();
		noOfRows = 0;
		
	}
	
	
	// Add data into content container.
	public void add(int rowid, String[] val)
	{
		noOfRows++;
		content.put(rowid, val);
		
	}
	
}