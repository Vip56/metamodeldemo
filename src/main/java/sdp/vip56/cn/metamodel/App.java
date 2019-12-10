package sdp.vip56.cn.metamodel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * Hello world!
 *
 */
public class App {
	private static final String CONNECTION_STRING = "jdbc:mysql://192.168.1.213/ambari";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "";

	public static void main(String[] args) {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection connection = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD);
			UpdateableDataContext dc = DataContextFactory.createJdbcDataContext(connection);
			Schema schema = dc.getSchemaByName("ambari");
			dc.executeUpdate(new UpdateScript() {
				@Override
				public void run(UpdateCallback cb) {
					String sql = cb.createTable(schema, "my_table")
					  .withColumn("id").ofType(ColumnType.INTEGER)
					  .withColumn("personName").ofType(ColumnType.STRING).ofSize(255)
					  .withColumn("age").ofType(ColumnType.INTEGER)
					  .toSql();
					System.out.println(sql);
				}
			});
			List<String> tables = schema.getTableNames();
			for(String name : tables) {
				System.out.println("Table Name is : " + name);
			}

			Table table = schema.getTableByName(tables.get(0));
			List<Column> columns = table.getColumns();
			for(Column column : columns) {
				System.out.println("Column Name is: " + column.getName() + ", Type is: " + column.getType().getName());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println("Hello World!");
	}
}
