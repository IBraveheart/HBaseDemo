import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author Akang
 * @create 2023-06-27 11:49
 */
public class HBaseConnect {
    private static Connection connection = null ;

    static {
        try {
            connection = ConnectionFactory.createConnection() ;
        } catch (IOException e) {
            System.out.println("获取连接失败");
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        return connection ;
    }
    public static void closeConnection() throws IOException {
        if (connection != null){
            connection.close();
        }
    }

    public static void main(String[] args) {
        System.out.println(getConnection());

    }
}
