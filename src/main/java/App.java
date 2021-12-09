import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class App {

    private static final String TABLE_NAME = "password_table";

    public static void main(String[] args) throws IOException {

        boolean isFirstStart = false;

        String connectionUrl = "jdbc:sqlserver://localhost;databaseName=password-db;user=admin;password=admin";

        BufferedReader br = new BufferedReader(new FileReader("c://All/VSB/2rocnik/AVD/Ukol-filtr/6-sorted.uniq"));
        br.skip(500);

        long totalPassword = 1_000_000;
        Random random = new Random();

        // create
        CuckooFilter<String> filter = new CuckooFilter.Builder<String>(Funnels.stringFunnel(StandardCharsets.UTF_8), 1000000).build();


        try (Connection connection = DriverManager.getConnection(connectionUrl)) {

            List<String> passwords = new ArrayList<>();
            if (isFirstStart) {
                System.out.println("Start inserting password to DB");
                long start = System.currentTimeMillis();
                connection.prepareStatement("TRUNCATE TABLE " + TABLE_NAME).execute();
                PreparedStatement preparedInsertStatement = connection.prepareStatement("INSERT INTO " + TABLE_NAME + " (id, password) VALUES (?, ?)");
                int inserted = 0;
                for (long id = 1; id <= totalPassword; ) {
                    br.skip(random.nextInt(5));
                    String password = br.readLine().replaceAll("\\s+", "");
                    if (!password.isEmpty() && !passwords.contains(password)) {
                        preparedInsertStatement.setLong(1, id);
                        preparedInsertStatement.setString(2, password);
                        if (id < totalPassword / 2) {
                            filter.put(password);
                        }
                        passwords.add(password);
                        preparedInsertStatement.addBatch();
                        id++;
                    }
                    if (id % 1000 == 0) {
                        inserted += preparedInsertStatement.executeBatch().length;
                        connection.commit();
                        System.out.println("Total committed " + id + "/" + totalPassword);
                    }
                }
                inserted += preparedInsertStatement.executeBatch().length;
                connection.commit();
                System.out.println("Total inserted: " + inserted);

                preparedInsertStatement.close();

                long end = System.currentTimeMillis();
                System.out.println("End inserting Total time taken = " + (end - start) + " ms.");
            } else {
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT password FROM " + TABLE_NAME);
                int inserted = 0;
                while (resultSet.next()) {
                    final String password = resultSet.getString(1);
                    filter.put(password);
                    passwords.add(password);
                    inserted ++;
                    if (inserted % 10000 == 0) {
                        System.out.println("Total true password in list so far: " + inserted);
                    }
                }
            }


            /*for (int i = 0; i < totalPassword - 490_000; ) {
                String password = RandomStringUtils.randomAlphabetic(3, 10);
                if (!passwords.contains(password)) {
                    passwords.add(password);
                    i++;
                    if (i % 10000 == 0) {
                        System.out.println("Total false password in list so far: " + i);
                    }
                }
            }*/

            System.out.println("Total password in list " + passwords.size());

            Collections.shuffle(passwords);

            matchChecking(filter, passwords, connection, true);
            matchChecking(filter, passwords, connection, false);

            //matchCheckingParallel(filter, passwords, connection, true);
            //matchCheckingParallel(filter, passwords, connection, false);


        } catch (Exception e) {
            e.printStackTrace();
        }




    }

    private static void matchCheckingParallel(CuckooFilter<String> filter, List<String> passwords, Connection connection, boolean withFilter) throws SQLException {
        AtomicInteger totalMightContain = new AtomicInteger();
        AtomicInteger totalActual = new AtomicInteger();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 FROM password_table WHERE password = ?");
        passwords.parallelStream().forEach(password -> {
            if (withFilter) {
                if (filter.mightContain(password)) {
                    totalMightContain.incrementAndGet();
                    try {
                        getActualResult(preparedStatement, totalActual, password);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                try {
                    getActualResult(preparedStatement, totalActual, password);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        });
    }

    private static void matchChecking(CuckooFilter<String> filter, List<String> passwords, Connection connection, boolean withFilter) throws SQLException {
        long start = System.currentTimeMillis();
        System.out.println("Start match checking. With filter? " + withFilter);
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 FROM password_table WHERE password = ?");
        int totalMightContain = 0;
        int totalActual = 0;
        for (String password : passwords) {
            if (withFilter) {
                if (filter.mightContain(password)) {
                    totalMightContain++;
                    totalActual = getActualResult(preparedStatement, totalActual, password);
                }
            } else {
                totalActual = getActualResult(preparedStatement, totalActual, password);
            }
            if (totalActual % 10000 == 0) {
                long end = System.currentTimeMillis();
                System.out.println("Total actual check: " + totalActual + " Total time taken = " + (end - start) + " ms. With filter? " + withFilter );
            }
        }
        preparedStatement.close();

        long end = System.currentTimeMillis();
        System.out.println("End match checking. With filter? " + withFilter + " Total time taken = " + (end - start) + " ms. Total might contain " + totalMightContain + " total actual " + totalActual);
    }

    private static int getActualResult(PreparedStatement preparedStatement, int totalActual, String password) throws SQLException {
        preparedStatement.setString(1, password);
        preparedStatement.execute();
        final ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            totalActual++;
        }
        resultSet.close();
        return totalActual;
    }

    private static void getActualResult(PreparedStatement preparedStatement, AtomicInteger totalActual, String password) throws SQLException {
        preparedStatement.setString(1, password);
        preparedStatement.execute();
        final ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            totalActual.incrementAndGet();
        }
        resultSet.close();
    }
}
