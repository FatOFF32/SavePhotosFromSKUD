package root;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class BlobHandler {

    // logger
    private static final Logger LOGGER = LoggerFactory.getLogger(BlobHandler.class);

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        String addressBase;
        String userMySQL;
        String passMySQL;
        String catalogPath;

        Properties config = new Properties();
        Properties lof4jConf = new Properties();
        try {
            // Получим данные нашего конфига
            config.load(BlobHandler.class.getResourceAsStream("/config.properties"));
            addressBase = config.getProperty("addressBase");
            userMySQL = config.getProperty("userMySQL");
            passMySQL = config.getProperty("passMySQL");
            // т.к. наименования папок могут быть русскими, преобразуем кодировку в windows-1251
            catalogPath = new String(config.getProperty("catalogPath").getBytes("ISO-8859-1"), "Cp1251");

            // Установим месторасположение лог файла для log4j
            lof4jConf.load(BlobHandler.class.getResourceAsStream("/log4j.properties"));
            lof4jConf.setProperty("log4j.appender.file.File", catalogPath + "log4j.log");
            PropertyConfigurator.configure(lof4jConf);
        } catch (IOException e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Ошибка разбора конфигурационного файла config.properties", e);
            return;
        }

        // Загружаем нужный нам JDBC драйвер. Используем  Class.forName для того, чтобы не импортировать com.mysql.jdbc.Driver
        Class.forName("com.mysql.jdbc.Driver");//.newInstance(); //.cj
        // Выполняем подключение, используя загруженный драйвер: 1c_exp - название БД в mysql СУБД
        try (Connection con =
                     DriverManager.getConnection("jdbc:mysql://" + addressBase, userMySQL, passMySQL)) {
                     // Можно ещё так:
                     //DriverManager.getConnection("jdbc:mysql://" + addressBase + "?useSSL=false&user=" + userMySQL + "&password=" + passMySQL)) {
            // Создаем statement
            try (Statement stmnt = con.createStatement()) {
                // Выполняем запрос к БД
                try (ResultSet rs = stmnt.executeQuery("SELECT * FROM photos")) {
                    String tabID;
                    String exp = ".jpg";
                    String fileName;
                    Blob photo;
                    ArrayList<Integer> idFromDel = new ArrayList<>();
                    // Перемщаем курсор по результам
                    while (rs.next()) {
                        // Извлекаем конкретные значения из ResultSet
                        tabID = rs.getString("TABID");
                        photo = rs.getBlob("Photo");

                        // Проверим, есть ли табельный номер и данные файла, если есть - выгружаем.
                        if(tabID == null || tabID.isEmpty() || photo == null)
                            continue;

                        // сохраним файлы
                        fileName = catalogPath + tabID.trim() + exp;
                        if (!exportBlob(fileName, photo))
                            continue;
/*
                        try {
                            exportBlob(fileName, photo);
                        } catch (Exception e) {
                            if (LOGGER.isErrorEnabled())
                                LOGGER.error("Ошибка сохранения файла " + fileName, e);
                            continue;
                        }
*/

                        idFromDel.add(rs.getInt("ID"));
                        if (LOGGER.isInfoEnabled())
                            LOGGER.info("Сохранен файл: " + fileName);
                    }
                    // Удалим строки из таблицы
                    if (idFromDel.size() > 0) {
                        String queryCond = idFromDel.toString();
                        queryCond = queryCond.substring(1, queryCond.length() - 1);
                        try (PreparedStatement prepared = con.prepareStatement("DELETE FROM photos WHERE ID in (" + queryCond + ")")) {

                            prepared.executeUpdate();
                        }
                    }
                }
            }
        } catch (SQLException e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Ошибка SQL: ", e);
        }

    }
    private static boolean exportBlob(String myFile, Blob myBlob) {

        File binaryFile = new File(myFile);
        try (OutputStream outStream = new BufferedOutputStream(new FileOutputStream(binaryFile));
            InputStream inStream = new BufferedInputStream(myBlob.getBinaryStream())) {
            int b;
            while ((b = inStream.read()) != -1)
                outStream.write(b);
            outStream.flush();

        } catch (SQLException | IOException e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error("Ошибка сохранения файла: " + myFile, e);
            return false;
        }
        return true;

/*
        File binaryFile = new File(myFile);
        FileOutputStream outStream = new FileOutputStream(binaryFile);
        InputStream inStream = myBlob.getBinaryStream();
        int size = 4096;//myBlob.getBufferSize();
        byte[] buffer = new byte[size];
        int length;
        while ((length = inStream.read(buffer)) != -1) {
            outStream.write(buffer, 0, length);
            outStream.flush();
        }
        inStream.close();
        outStream.close();
*/
    }
}

