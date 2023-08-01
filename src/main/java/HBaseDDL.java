import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @author Akang
 * @create 2023-06-27 12:09
 */
public class HBaseDDL {
    private static Connection connection = HBaseConnect.getConnection();

    /**
     * 创建命名空间
     *
     * @param name
     * @throws IOException
     */
    public static void createNamespace(String name) throws IOException {
        Admin admin = connection.getAdmin();

        NamespaceDescriptor descriptor = NamespaceDescriptor
                .create(name)
                .addConfiguration("user", "fish")
                .build();
        try {
            admin.createNamespace(descriptor);
        } catch (IOException e) {
            System.out.println("命名空间已经存在");
            e.printStackTrace();
        }

        admin.close();
    }


    /**
     * 创建表格
     *
     * @param nameSpace
     * @param tableName
     * @param columnFamilies
     */
    public static void createTable(String nameSpace, String tableName, String... columnFamilies) throws IOException {
        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族");
            return;
        }

        if (isTableExists(nameSpace, tableName)) {
            System.out.println("表格已经存在");
            return;
        }

        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(nameSpace, tableName));

        for (String columnFamily : columnFamilies) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            hColumnDescriptor.setMaxVersions(5);

            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        try {
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    /**
     * 判断表格是否存在
     *
     * @param nameSpace 命名空间
     * @param tableName 表格名称
     * @return
     * @throws IOException
     */
    public static boolean isTableExists(String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(nameSpace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

        return b;
    }

    /**
     * 删除表格
     *
     * @param nameSpace
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean deleteTable(String nameSpace, String tableName) throws IOException {
        if (!isTableExists(nameSpace, tableName)) {
            return false;
        }

        Admin admin = connection.getAdmin();

        try {
            TableName name = TableName.valueOf(nameSpace, tableName);
            admin.disableTable(name);
            admin.deleteTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    /**
     * 修改表格
     *
     * @param nameSpace
     * @param tableName
     * @param columnFamily
     * @param version
     */
    public static void modifyTable(String nameSpace, String tableName, String columnFamily, int version)
            throws IOException {
        if (!isTableExists(nameSpace, tableName)) {
            System.out.println("表格不存在");
            return;
        }

        Admin admin = connection.getAdmin();
        try {
            TableName name = TableName.valueOf(nameSpace, tableName);
            // 获取原表描述，再对其进行修改
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);

            // 获取原列族描述，再对其进行修改
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

            for (HColumnDescriptor family : columnFamilies) {
                family.setMaxVersions(version) ;
                tableDescriptor.modifyFamily(family) ;
            }
            // 重新初始化列族
//            tableDescriptor.modifyFamily(new HColumnDescriptor(columnFamily).setMaxVersions(version)) ;

            admin.modifyTable(
                    name
                    , tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();

    }

    public static void main(String[] args) throws IOException {

        // 创建命名空间
        createNamespace("bigdata");

        System.out.println(isTableExists("bigdata", "student"));

        createTable("bigdata", "student", "info", "msg");

        modifyTable("bigdata", "student", "info", 6);

        System.out.println("其它代码");

        HBaseConnect.closeConnection();


    }
}
