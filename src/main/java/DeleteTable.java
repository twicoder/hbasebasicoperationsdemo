import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DeleteTable {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        try {
            Admin admin = connection.getAdmin();

            TableName tableName = TableName.valueOf("census2");

            if(admin.tableExists(tableName)){
                System.out.println("Table exists, Deleting...");
                admin.disableTable(tableName);
                admin.deleteTable(tableName);

                System.out.println("Done.");
            } else {
                System.out.println("Table does not exit.");
            }

        } finally {
            connection.close();
        }
    }
}
