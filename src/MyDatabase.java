import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author divyanshu
 *
 */
public class MyDatabase {
	static String prompt = "davisql>";
	static String schema;

	public MyDatabase() {
		schema = "University";
		// createSchema(schema);
	}

	/**
	 * creating a new schema
	 * 
	 * @param name
	 */
	public void createSchema(String name) {
		if (schemaExists(name)) {
			System.out.println("Schema Already Exists");
			return;
		}

		try {
			// storing information of all the schemas
			RandomAccessFile schemataTableFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			try {
				schemataTableFile.seek(schemataTableFile.length());
				schemataTableFile.writeInt(name.length());
				for (int i = 0; i < name.length(); i++) {
					char c = name.charAt(i);
					schemataTableFile.writeByte(c);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void createTable(String name, String tuple) throws FileNotFoundException {

		String[] tupleInfo;
		tupleInfo = tuple.split(",");// 1 column information at each index
		int numOfCol = tupleInfo.length;
		// storing information of all the tables
		RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema.table.tbl", "rw");

		// writing to the tables file
		try {
			// set pointer at last position of the file
			tablesTableFile.seek(tablesTableFile.length());
			// write schema length and schema name to the tables file
			tablesTableFile.writeInt(schema.length());
			for (int i = 0; i < schema.length(); i++) {
				char c = schema.charAt(i);
				tablesTableFile.writeByte(c);
			}
			// write table length and table name to the tables file
			tablesTableFile.writeInt(name.length());
			for (int i = 0; i < name.length(); i++) {
				char c = name.charAt(i);
				tablesTableFile.writeByte(c);
			}

			tablesTableFile.writeInt(numOfCol);// write num of columns for this
												// table to the tables file

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// storing information of all the columns
		RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");

		for (int i = 0; i < tupleInfo.length; i++) {
			// split the tuple information into 3 parts
			String[] column;
			String colName;
			String datatype;
			String primarykey = "";
			boolean primaryKey = false;
			boolean notNull = false;
			column = tupleInfo[i].split(" ");// correction here
			if (column[2] != null && !column[2].equals("primary")) {
				colName = column[1];// Column name
				datatype = column[2];// Data type of the column
				if (datatype.contains("varchar")) {
					datatype += ")";
				}
			} else {
				colName = column[0];// Column name
				datatype = column[1];// Data type of the column
			}
			if (column.length > 2) {
				notNull = true;
				primaryKey = true;
			}

			// writing to the columns table
			try {
				// set pointer at last position of the file
				columnsTableFile.seek(columnsTableFile.length());
				// write schema length and schema name to the column file
				columnsTableFile.writeInt(schema.length());
				for (int j = 0; j < schema.length(); j++) {
					char c = schema.charAt(j);
					columnsTableFile.writeByte(c);
				}
				// write table length and table name to the column file
				columnsTableFile.writeInt(name.length());
				for (int j = 0; j < name.length(); j++) {
					char c = name.charAt(j);
					columnsTableFile.writeByte(c);
				}
				// write column length and name to the column file
				columnsTableFile.writeInt(colName.length());
				for (int j = 0; j < colName.length(); j++) {
					char c = colName.charAt(j);
					columnsTableFile.writeByte(c);
				}
				columnsTableFile.writeInt(i + 1);// ordinal position
				columnsTableFile.writeInt(datatype.length());// length of data
																// type
				columnsTableFile.writeBytes(datatype);

				if (notNull) {// isNullable
					columnsTableFile.writeInt("No".length());
					columnsTableFile.writeBytes("No");
				} else {
					columnsTableFile.writeInt("Yes".length());
					columnsTableFile.writeBytes("Yes");
				}
				if (primaryKey) {
					columnsTableFile.writeInt("Yes".length());
					columnsTableFile.writeBytes("Yes");
				} else {
					columnsTableFile.writeInt("No".length());
					columnsTableFile.writeBytes("Yes");
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		RandomAccessFile widgetTableFile = new RandomAccessFile(schema + "." + name + "." + "tbl.dat", "rw");

	}

	/**
	 * Insert value into the table
	 * 
	 * @param tableName
	 *            - Name of the Table
	 * @param values
	 *            - values to be inserted
	 */
	public static void insertTable(String tableName, String values) {

		try {
			RandomAccessFile checkTable = new RandomAccessFile("information_schema.table.tbl", "rw");
			boolean flag = false;
			String schemaName = "";
			String nameOfTable = "";
			int schemaLength, tableLength, noOfCol = 0;
			while (checkTable.getFilePointer() < checkTable.length()) {

				schemaLength = checkTable.readInt();
				for (int i = 0; i < schemaLength; i++) {
					schemaName += (char) checkTable.readByte();
				}
				// System.out.println(schemaName);
				tableLength = checkTable.readInt();
				for (int i = 0; i < tableLength; i++) {
					nameOfTable += (char) checkTable.readByte();
				}
				// System.out.println(nameOfTable);
				// System.out.println(schema);
				// System.out.println(tableName);
				noOfCol = checkTable.readInt();
				if (schema.equals(schemaName) && tableName.equals(nameOfTable)) {
					flag = true;
					break;
				}
			}
			if (flag != true) {
				System.out.println("Table does not exist");
				return;
			}
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			int col_schemaLength;
			String col_schemaName = "";
			int col_tableLength;
			String col_nameOftable = "";
			int col_length;
			String col_name = "";
			int ord_position;
			int col_typelength;
			String col_type = "";
			int nullLength;
			String isNullable = "";
			String primaryKey = "";
			int keyLength;
			ArrayList<Item> col_details = new ArrayList<>();
			Item[] col_detail = new Item[noOfCol];
			int pos = 0;
			while (columnsTableFile.getFilePointer() < columnsTableFile.length()) {
				col_schemaName = "";
				col_nameOftable = "";
				col_name = "";
				col_type = "";
				isNullable = "";
				primaryKey = "";
				col_schemaLength = columnsTableFile.readInt();
				for (int i = 0; i < col_schemaLength; i++) {
					col_schemaName += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_schemaName);
				col_tableLength = columnsTableFile.readInt();
				for (int i = 0; i < col_tableLength; i++) {
					col_nameOftable += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_nameOftable);
				col_length = columnsTableFile.readInt();
				for (int i = 0; i < col_length; i++) {
					col_name += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_name);
				ord_position = columnsTableFile.readInt();
				col_typelength = columnsTableFile.readInt();
				for (int i = 0; i < col_typelength; i++) {
					col_type += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_type);
				nullLength = columnsTableFile.readInt();
				for (int i = 0; i < nullLength; i++) {
					isNullable += (char) columnsTableFile.readByte();
				}
				// System.out.println(isNullable);
				keyLength = columnsTableFile.readInt();
				for (int i = 0; i < keyLength; i++) {
					primaryKey += (char) columnsTableFile.readByte();
				}
				// System.out.println(primaryKey);
				if (col_schemaName.equals(schemaName) && col_nameOftable.equals(nameOfTable)) {
					// col_details.add(new Item(col_name, ord_position,
					// col_type, isNullable, primaryKey));
					col_detail[pos] = new Item(col_name, ord_position, col_type, isNullable, primaryKey);
					pos++;
				}
			}
			String[] value;
			value = values.split(",");
			RandomAccessFile table = new RandomAccessFile(schema + "." + tableName + "." + "tbl.dat", "rw");
			table.seek(table.length());
			if (noOfCol != value.length) {
				System.out.println("please enter all column values");
				return;
			}

			/*
			 * for (int i = 0; i < noOfCol; i++) {
			 * 
			 * System.out.println(col_detail[i]); }
			 */

			for (int i = 0; i < noOfCol; i++) {
				if (col_detail[i].col_type.equalsIgnoreCase("BYTE")) {
					table.writeByte(Byte.parseByte(value[i]));
				} else if (col_detail[i].col_type.equalsIgnoreCase("SHORT")
						|| col_detail[i].col_type.equalsIgnoreCase("SHORT INT")) {
					table.writeShort(Short.parseShort(value[i]));
				} else if (col_detail[i].col_type.equalsIgnoreCase("INT")) {
					table.writeInt(Integer.parseInt(value[i]));
				} else if (col_detail[i].col_type.equalsIgnoreCase("LONG")
						|| col_detail[i].col_type.equalsIgnoreCase("LONG INT")) {
					table.writeLong(Long.parseLong(value[i]));
				} else if (col_detail[i].col_type.matches("char(.+)")) {
					table.writeInt(value[i].length());
					for (int j = 0; j < value[i].length(); j++) {
						table.writeByte(value[i].charAt(j));
					}
				} else if (col_detail[i].col_type.matches("varchar(.+)")) {
					// System.out.println("Writing bytes in varchar:" +
					// value[i].length());
					table.writeInt(value[i].length());
					for (int j = 0; j < value[i].length(); j++) {
						table.writeByte(value[i].charAt(j));
					}
				} else if (col_detail[i].col_type.equalsIgnoreCase("FLOAT")) {
					table.writeFloat(Float.parseFloat(value[i]));
				} else if (col_detail[i].col_type.equalsIgnoreCase("DOUBLE")) {
					table.writeDouble(Double.parseDouble(value[i]));
				} else if (col_detail[i].col_type.equalsIgnoreCase("DATE")) {
					table.writeLong(dateParser(value[i]));
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static long dateParser(String dateInput) {
		dateInput = dateInput.replace("'", "");
		dateInput = dateInput.trim();
		String[] s = dateInput.split("-");
		String s1 = s[0] + s[1] + s[2];
		return Long.valueOf(s1);

	}

	public static String dateDisplayParser(long dateInput) {
		String input = String.valueOf(dateInput);

		String result = "";
		String year = input.substring(0, 4);
		String month = input.substring(4, 6);
		String date = input.substring(6, 8);
		result = year + "-" + month + "-" + date;
		return result;
	}

	public static void selectQuery(String tableName, String colName, String operator, String value) {

		try {
			RandomAccessFile checkTable = new RandomAccessFile("information_schema.table.tbl", "rw");
			boolean flag = false;
			String schemaName = "";
			String nameOfTable = "";
			int schemaLength, tableLength, noOfCol = 0;
			while (checkTable.getFilePointer() < checkTable.length()) {

				schemaLength = checkTable.readInt();
				for (int i = 0; i < schemaLength; i++) {
					schemaName += (char) checkTable.readByte();
				}
				// System.out.println(schemaName);
				tableLength = checkTable.readInt();
				for (int i = 0; i < tableLength; i++) {
					nameOfTable += (char) checkTable.readByte();
				}
				// System.out.println(nameOfTable);
				// System.out.println(schema);
				// System.out.println(tableName);
				noOfCol = checkTable.readInt();
				if (schema.equals(schemaName) && tableName.equals(nameOfTable)) {
					flag = true;
					break;
				}
			}
			if (flag != true) {
				System.out.println("Table does not exist");
				return;
			}
			RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			int col_schemaLength;
			String col_schemaName = "";
			int col_tableLength;
			String col_nameOftable = "";
			int col_length;
			String col_name = "";
			int ord_position;
			int col_typelength;
			String col_type = "";
			int nullLength;
			String isNullable = "";
			String primaryKey = "";
			int keyLength;
			ArrayList<Item> col_details = new ArrayList<>();
			Item[] col_detail = new Item[noOfCol];
			int pos = 0;
			while (columnsTableFile.getFilePointer() < columnsTableFile.length()) {
				col_schemaName = "";
				col_nameOftable = "";
				col_name = "";
				col_type = "";
				isNullable = "";
				primaryKey = "";
				col_schemaLength = columnsTableFile.readInt();
				for (int i = 0; i < col_schemaLength; i++) {
					col_schemaName += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_schemaName);
				col_tableLength = columnsTableFile.readInt();
				for (int i = 0; i < col_tableLength; i++) {
					col_nameOftable += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_nameOftable);
				col_length = columnsTableFile.readInt();
				for (int i = 0; i < col_length; i++) {
					col_name += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_name);
				ord_position = columnsTableFile.readInt();
				col_typelength = columnsTableFile.readInt();
				for (int i = 0; i < col_typelength; i++) {
					col_type += (char) columnsTableFile.readByte();
				}
				// System.out.println(col_type);
				nullLength = columnsTableFile.readInt();
				for (int i = 0; i < nullLength; i++) {
					isNullable += (char) columnsTableFile.readByte();
				}
				// System.out.println(isNullable);
				keyLength = columnsTableFile.readInt();
				for (int i = 0; i < keyLength; i++) {
					primaryKey += (char) columnsTableFile.readByte();
				}
				// System.out.println(primaryKey);
				if (col_schemaName.equals(schemaName) && col_nameOftable.equals(nameOfTable)) {
					// col_details.add(new Item(col_name, ord_position,
					// col_type, isNullable, primaryKey));
					col_detail[pos] = new Item(col_name, ord_position, col_type, isNullable, primaryKey);
					pos++;
				}

			}

			RandomAccessFile table = new RandomAccessFile(schema + "." + tableName + "." + "tbl.dat", "rw");

			while (table.getFilePointer() < table.length()) {
				for (int i = 0; i < noOfCol; i++) {
					if (col_detail[i].col_type.equalsIgnoreCase("BYTE")) {
						System.out.print(table.readByte() + "\t");
					} else if (col_detail[i].col_type.equalsIgnoreCase("SHORT")
							|| col_detail[i].col_type.equalsIgnoreCase("SHORT INT")) {
						System.out.print(table.readShort() + "\t");
					} else if (col_detail[i].col_type.equalsIgnoreCase("INT")) {
						System.out.print(table.readInt() + "\t");
					} else if (col_detail[i].col_type.equalsIgnoreCase("LONG")
							|| col_detail[i].col_type.equalsIgnoreCase("LONG INT")) {
						System.out.print(table.readLong() + "\t");
					} else if (col_detail[i].col_type.matches("CHAR(.+)")) {
						int n = table.readInt();
						for (int j = 0; j < n; j++) {
							System.out.print((char) table.readByte());
						}
						System.out.print("\t");
					} else if (col_detail[i].col_type.matches("varchar(.+)")) {
						int n = table.readInt();
						for (int j = 0; j < n; j++) {
							System.out.print((char) table.readByte());
						}
						System.out.print("\t");
					} else if (col_detail[i].col_type.equalsIgnoreCase("FLOAT")) {
						System.out.print(table.readFloat() + "\t");
					} else if (col_detail[i].col_type.equalsIgnoreCase("DOUBLE")) {
						System.out.print(table.readDouble() + "\t");
					} else if (col_detail[i].col_type.equalsIgnoreCase("DATE")) {

						System.out.print(dateDisplayParser(table.readLong()) + "\t");
					}
				}
				System.out.println();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Using a schema
	 * 
	 * @param name
	 */
	public void useSchema(String name) {
		if (!schemaExists(name)) {
			System.out.println("Schema does not exist");
			return;
		}
		schema = name;
	}

	public Boolean schemaExists(String name) {
		String s = "";
		try {
			RandomAccessFile schemataTableFile = new RandomAccessFile("Schema.txt", "rw");
			try {
				while (schemataTableFile.getFilePointer() < schemataTableFile.length()) {
					int length = schemataTableFile.readInt();
					// System.out.print(length + "\t");
					for (int i = 0; i < length; i++) {
						s += (char) schemataTableFile.readByte();
					}
					if (s.equals(name)) {
						return true;
					} else {
						s = "";
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Displaying all the schema records
	 */
	public void showSchema() {
		try {
			RandomAccessFile schemataTableFile = new RandomAccessFile("Schema.txt", "rw");
			try {
				while (schemataTableFile.getFilePointer() < schemataTableFile.length()) {
					int length = schemataTableFile.readInt();
					// System.out.print(length + "\t");
					for (int i = 0; i < length; i++) {
						System.out.print((char) schemataTableFile.readByte());
					}
					System.out.println();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself
	 *         num times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void version() {
		System.out.println("MyDatabase v1.0\n");
	}

	public static void splashScreen() {
		System.out.println(line("*", 80));
		System.out.println("Welcome to MyDatabase"); // Display the string.
		version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("*", 80));
	}

	public static void main(String[] args) {
		splashScreen();
		MyDatabase db = new MyDatabase();
		// reading from the prompt
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand = ""; // Variable to collect user input from the
									// prompt
		String[] command;// array to store commands as an array

		String[] createCommand;// string taking input for create table
		String[] insertCommand;// string taking input for insert command
		do {// do while !exit
			System.out.print(prompt);
			userCommand = scanner.next().trim();
			// handle parser for creating tables
			if (userCommand.contains("CREATE TABLE")) {
				createCommand = userCommand.split("\\(", 2);

				// System.out.println(createCommand[0]);
				command = createCommand[0].split(" ");
				String tableName;
				tableName = command[2];
				// createCommand[0] contains create table table_name string
				// System.out.println(createCommand[1]);
				// createCommand[1] contains info for the table
				// remove extra ")" from this string
				createCommand[1] = createCommand[1].replace(")", "");
				// System.out.println(createCommand[1]);
				try {
					createTable(tableName, createCommand[1]);
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}

			}
			// parser for using schema : command USE SCHEMA
			if (userCommand.contains("USE")) {
				command = userCommand.split(" ");
				db.schema = command[1];
			}

			if (userCommand.contains("DROP")) {
				String[] dropCommand;
				String tableName;
				dropCommand = userCommand.split(" ");
				tableName = dropCommand[2];
				// call method for dropping the table
			}

			if (userCommand.contains("SELECT")) {
				String[] selectCommand;
				String tableName;
				String colName = "";
				String whereClause = "";
				String[] clause;
				String value = "";
				String operator = "";
				selectCommand = userCommand.split(" ");
				tableName = selectCommand[3];
				if (selectCommand.length > 5)
					whereClause = selectCommand[5];
				clause = whereClause.split("\\>=|\\<=|\\=|\\>|\\<");
				colName = clause[0];
				value = clause[1];
				if (whereClause.contains(">="))
					operator = ">=";
				else if (whereClause.contains("<="))
					operator = "<=";
				else if (whereClause.contains(">"))
					operator = ">";
				else if (whereClause.contains("<"))
					operator = "<";
				else if (whereClause.contains("="))
					operator = "=";
				System.out.println(colName + " " + operator + " " + value);
				// call method for select query
				selectQuery(tableName, colName, operator, value);
			}

			// parser for inserting values to the table
			if (userCommand.contains("INSERT")) {
				String[] insertName;// has table name field
				String tableName;// table name
				insertCommand = userCommand.split("\\(", 2);
				insertName = insertCommand[0].split(" ");
				tableName = insertName[2];
				insertCommand[1] = insertCommand[1].replace(")", "");
				insertTable(tableName, insertCommand[1]);
			}
			command = userCommand.split(" ");
			if (command.length < 2) {
				if (command[0].equals("EXIT"))
					continue;
				else {
					System.out.println("Please enter the valid input");
					System.exit(0);
				}
			}
			// command for checking the particular command to be processed
			String switchCommand = command[0] + " " + command[1];
			String name = "";
			if (command.length > 2) {
				name = command[2];// name of the schema/ table to be created
			}
			// for (int i = 0; i < command.length; i++)
			// System.out.println(command[i]);
			switch (switchCommand) {
			case "CREATE SCHEMA":
				db.createSchema(name);
				break;
			case "SHOW SCHEMAS":
				db.showSchema();
				break;
			case "CREATE table":
				break;
			case "SHOW TABLES":
				break;
			}

		} while (!userCommand.equals("exit"));
	}

}
