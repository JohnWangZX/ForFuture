package 并发编程.ThreadLocal学习;

/**
 * @author: wzx
 * @date: 2023/5/22
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

    private static final ThreadLocal<Connection> dbConnectionLocal = new ThreadLocal<Connection>();
//    {
//        @Override
//        protected Connection initialValue() {
//            try {
//                return DriverManager.getConnection("", "", "");
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            return null;
//        }
//    };

    public Connection getConnection() {
        return dbConnectionLocal.get();
    }
}