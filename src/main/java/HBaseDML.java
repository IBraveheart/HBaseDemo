import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Akang
 * @create 2023-06-28 15:19
 */
public class HBaseDML {
    private static Connection connection = HBaseConnect.getConnection();

    /**
     * 插入数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void putCell(String nameSpace, String tableName, String rowKey, String columnFamily, String columnName,
                               String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 读取数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void getCells(String nameSpace, String tableName, String rowKey, String columnFamily
            , String columnName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));


        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
            // 获得所有的版本数据
            get.setMaxVersions();

            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String row = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String column = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(row + "," + family + ":" + column + "," + value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();

    }

    /**
     * 扫描数据
     *
     * @param nameSpace
     * @param tableName
     * @param startRow
     * @param endRow
     * @throws IOException
     */
    public static void scanRows(String nameSpace, String tableName, String startRow, String endRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        try {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRow));
            scan.withStopRow(Bytes.toBytes(endRow));

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    System.out.println(rowKey + "," + family + ":" + column + "," + value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /**
     * 带过滤条件的扫描
     *
     * @param nameSpace
     * @param tableName
     * @param startRow
     * @param endRow
     * @param columnFamily
     * @param columnName
     * @param data
     * @throws IOException
     */
    public static void filterScan(String nameSpace, String tableName, String startRow, String endRow
            , String columnFamily, String columnName, String data) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        try {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRow));
            scan.withStopRow(Bytes.toBytes(endRow));

            FilterList filterList = new FilterList();

            // 保留整行的数据
            // 结果同时会保留没有当前行的数据
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(columnFamily)
                    , Bytes.toBytes(columnName)
                    , CompareFilter.CompareOp.EQUAL
                    , Bytes.toBytes(data)
            );

            filterList.addFilter(singleColumnValueFilter);
            scan.setFilter(filterList);

            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String rowKey = new String(CellUtil.cloneRow(cell));
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    System.out.println(rowKey + "," + family + ":" + column + "," + value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();

    }

    /**
     * 删除数据
     *
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @throws IOException
     */
    public static void deleteColum(String nameSpace, String tableName, String rowKey, String columnFamily
            , String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            // 删除所有版本的数据
            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
            // 删除列族
            delete.addFamily(Bytes.toBytes(columnFamily));


            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    public static List<String> dealCell(Cell[] cells) {
        List<String> result = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneValue(cell);
            result.add(new String(bytes));
        }

        return result;
    }

    public static void main(String[] args) throws IOException {

//        putCell("bigdata", "student", "1001", "info"
//                , "name", "张三");
//        putCell("bigdata", "student", "1001", "info"
//                , "name", "李四");
//        putCell("bigdata", "student", "1001", "info"
//                , "name", "王五");

//        getCells("bigdata", "student", "1001", "info", "name");

//        scanRows("bigdata", "student", "1001","1005");

//        filterScan("bigdata", "student", "1001","1005","info","age","20");

        deleteColum("bigdata", "student", "1001", "info", "age");


        System.out.println("其他代码");

        HBaseConnect.closeConnection();
    }
}
