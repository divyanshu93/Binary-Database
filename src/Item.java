
public class Item {

	String col_name;
	int index;
	String col_type;
	String isNullable;
	String primaryKey;

	public Item(String col_name, int index, String col_type, String isNullable, String primaryKey) {
		this.col_name = col_name;
		this.index = index;
		this.col_type = col_type;
		this.isNullable = isNullable;
		this.primaryKey = primaryKey;
	}

	@Override
	public String toString() {
		return "Item [col_name=" + col_name + ", index=" + index + ", col_type=" + col_type + ", isNullable="
				+ isNullable + ", primaryKey=" + primaryKey + "]";
	}

}
