package Analysis.Team2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class AnalysisService {
    private static final String API_URL = "https://api.openai.com/v1/chat/completions";
    private static final String API_KEY = System.getenv("API_KEY");
    private static final String LOG_DIR = "log";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Async
    public CompletableFuture<Long> getMonthlySalesAmountAsync(String city, String adminiDistrict, String primaryBusiness, String secondaryBusiness) {
        DataSource dataSource = jdbcTemplate.getDataSource();
        Long amount = null;
        try (Connection conn = dataSource.getConnection();
             CallableStatement callableStatement = conn.prepareCall("{ call proc_month_amt(?, ?, ?, ?, ?) }")) {
            callableStatement.setString(1, city);
            callableStatement.setString(2, adminiDistrict);
            callableStatement.setString(3, primaryBusiness);
            callableStatement.setString(4, secondaryBusiness);
            callableStatement.registerOutParameter(5, Types.NUMERIC);
            callableStatement.execute();
            amount = callableStatement.getLong(5);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(amount);
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getDaySalesAsync(String city, String adminiDistrict, String primaryBusiness, String secondaryBusiness) {
        DataSource dataSource = jdbcTemplate.getDataSource();
        List<Map<String, Object>> daySales = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             CallableStatement cstmt = conn.prepareCall("{call Get_Day_Sales_Proc(?, ?, ?, ?, ?)}")) {
            cstmt.setString(1, city);
            cstmt.setString(2, adminiDistrict);
            cstmt.setString(3, primaryBusiness);
            cstmt.setString(4, secondaryBusiness);
            cstmt.registerOutParameter(5, Types.ARRAY, "DAY_SALES_TAB");
            cstmt.execute();
            try (ResultSet rs = cstmt.getArray(5).getResultSet()) {
                while (rs.next()) {
                    Struct row = (Struct) rs.getObject(2);
                    Object[] attrs = row.getAttributes();
                    Map<String, Object> saleData = new HashMap<>();
                    saleData.put("day", attrs[0]);
                    saleData.put("totalAmt", attrs[1]);
                    daySales.add(saleData);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(daySales);
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getGenderAgeDistributionAsync(String city, String dong, String primaryCategory, String secondaryCategory) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> distribution = new ArrayList<>();
            try (Connection conn = jdbcTemplate.getDataSource().getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call Get_Distribution_By_Separate_Codes(?, ?, ?, ?, ?)}")) {
                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.setString(3, primaryCategory);
                cstmt.setString(4, secondaryCategory);
                cstmt.registerOutParameter(5, Types.ARRAY, "GENDER_AGE_TAB");

                cstmt.execute();

                try (ResultSet rs = cstmt.getArray(5).getResultSet()) {
                    while (rs.next()) {
                        Struct row = (Struct) rs.getObject(2); // each row is a STRUCT
                        Object[] attrs = row.getAttributes();
                        Map<String, Object> data = new HashMap<>();
                        data.put("sex", attrs[0]);
                        data.put("ageLabel", attrs[1]);
                        data.put("percentage", attrs[2]);
                        distribution.add(data);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return distribution;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getHourlySalesAsync(String city, String adminDistrict, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            DataSource dataSource = jdbcTemplate.getDataSource();
            List<Map<String, Object>> hourlySales = new ArrayList<>();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call Get_Hourly_Sales_Proc(?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, adminDistrict);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.ARRAY, "SALES_TAB");

                cstmt.execute();

                try (ResultSet rs = cstmt.getArray(5).getResultSet()) {
                    while (rs.next()) {
                        Struct row = (Struct) rs.getObject(2);
                        Object[] attrs = row.getAttributes();
                        Map<String, Object> saleData = new HashMap<>();
                        saleData.put("hourLabel", attrs[0]);
                        saleData.put("amtPercentage", attrs[1]);
                        saleData.put("cntPercentage", attrs[2]);
                        hourlySales.add(saleData);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return hourlySales;
        });
    }

    @Async
    public CompletableFuture<String> getMaxLiftConsequentAsync(String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            DataSource dataSource = jdbcTemplate.getDataSource();
            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_get_max_lift_consequent(?, ?, ?)}")) {

                cstmt.setString(1, primaryBusiness);
                cstmt.setString(2, secondaryBusiness);
                cstmt.registerOutParameter(3, Types.VARCHAR);

                cstmt.execute();

                return cstmt.getString(3);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Async
    public CompletableFuture<Map<String, String>> getIndicatorAsync(String city_nm, String dong_nm) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, String> results = new HashMap<>();
            DataSource dataSource = jdbcTemplate.getDataSource();
            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_indicator(?, ?, ?, ?)}")) {

                cstmt.setString(1, city_nm);
                cstmt.setString(2, dong_nm);
                cstmt.registerOutParameter(3, Types.VARCHAR);
                cstmt.registerOutParameter(4, Types.VARCHAR);

                cstmt.execute();

                results.put("v_code", cstmt.getString(3));
                results.put("v_desc", cstmt.getString(4));
                return results;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Async
    public CompletableFuture<Map<String, Integer>> getMerchantAsync(String city, String dong, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, Integer> result = new HashMap<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_merchant(?, ?, ?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.INTEGER);
                cstmt.registerOutParameter(6, Types.INTEGER);
                cstmt.registerOutParameter(7, Types.INTEGER);

                cstmt.execute();

                result.put("상가수비율", cstmt.getInt(5));
                result.put("휴업수비율", cstmt.getInt(6));
                result.put("폐업수비율", cstmt.getInt(7));

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getMerchantCntAsync(String city, String dong, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_merchant_cnt(?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.REF_CURSOR);

                cstmt.execute();

                try (ResultSet rs = (ResultSet) cstmt.getObject(5)) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("v_ta_ym", rs.getString(1));
                        row.put("v_mer_cnt", rs.getInt(2));
                        result.add(row);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getYearAmtAsync(String city, String dong, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_year_amt(?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.REF_CURSOR);

                cstmt.execute();

                try (ResultSet rs = (ResultSet) cstmt.getObject(5)) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("v_ta_ymd", rs.getString(1));
                        row.put("v_amt", rs.getLong(2));
                        result.add(row);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getUnitPriceCntAsync(String city, String dong, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_unit_price_cnt(?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.REF_CURSOR);

                cstmt.execute();

                try (ResultSet rs = (ResultSet) cstmt.getObject(5)) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("v_ta_ymd", rs.getString(1));
                        row.put("v_cnt", rs.getInt(2));
                        row.put("v_unit_price", rs.getDouble(3));
                        result.add(row);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getRecentMonthlySalesAsync(String city, String dong, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call get_recent_monthly_sales(?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.REF_CURSOR);

                cstmt.execute();

                try (ResultSet rs = (ResultSet) cstmt.getObject(5)) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("salesMonth", rs.getString("sales_month"));
                        row.put("totalSales", rs.getInt("total_sales"));
                        result.add(row);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getAverageFlowpopAsync(String city, String dong) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call get_average_flowpop(?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, dong);
                cstmt.registerOutParameter(3, Types.REF_CURSOR);

                cstmt.execute();

                try (ResultSet rs = (ResultSet) cstmt.getObject(3)) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("ageGroup", rs.getString("age_group"));
                        row.put("mCntAvg", rs.getInt("M_CNT_AVG"));
                        row.put("fCntAvg", rs.getInt("F_CNT_AVG"));
                        result.add(row);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getCustomerPercentageChangeAsync(String cityNm, String admiNm, String tpbuzNm1, String tpbuzNm2) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call get_customer_percentage_change(?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, cityNm);
                cstmt.setString(2, admiNm);
                cstmt.setString(3, tpbuzNm1);
                cstmt.setString(4, tpbuzNm2);
                cstmt.registerOutParameter(5, Types.REF_CURSOR);

                cstmt.execute();

                try (ResultSet rs = (ResultSet) cstmt.getObject(5)) {
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        row.put("sex", rs.getString("sex").trim()); // 공백 제거
                        row.put("age", rs.getString("age").trim()); // 공백 제거
                        row.put("percentageChange", rs.getDouble("change_in_percentage"));
                        result.add(row);
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<Map<String, Object>> getFullBusinessAnalysisAsync(String cityNm, String admiNm, String buzMajorNm, String buzMinorNm) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, Object> result = new HashMap<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call full_business_analysis(?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, cityNm);
                cstmt.setString(2, admiNm);
                cstmt.setString(3, buzMajorNm);
                cstmt.setString(4, buzMinorNm);
                cstmt.registerOutParameter(5, Types.VARCHAR);
                cstmt.registerOutParameter(6, Types.NUMERIC);
                cstmt.registerOutParameter(7, Types.VARCHAR);
                cstmt.registerOutParameter(8, Types.NUMERIC);
                cstmt.registerOutParameter(9, Types.VARCHAR);

                cstmt.execute();

                result.put("consequent", cstmt.getString(5));
                result.put("selectedDistrictCount", cstmt.getInt(6));
                result.put("maxDistrict", cstmt.getString(7));
                result.put("maxMerCount", cstmt.getInt(8));
                result.put("maxDistrictName", cstmt.getString(9));

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }
    @Async
    public CompletableFuture<String> getGPTResponseAsync(String inputContent) {
        return CompletableFuture.supplyAsync(() -> {
            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(30, TimeUnit.SECONDS)
                    .writeTimeout(30, TimeUnit.SECONDS)
                    .build();
//            System.out.println("API_KEY : " + API_KEY);
            String json = "{ \"model\": \"gpt-4\", \"messages\": [{\"role\": \"user\", \"content\": \"" + inputContent.replace("\"", "\\\"") + "\"}]}";
            RequestBody body = RequestBody.create(json, MediaType.parse("application/json; charset=utf-8"));
            Request request = new Request.Builder()
                    .url(API_URL)
                    .header("Authorization", "Bearer " + API_KEY)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                String responseBody = response.body().string();
                logToFile(inputContent, responseBody);

                if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
                return responseBody;
            } catch (Exception e) {
                e.printStackTrace();
                logToFile(inputContent, e.getMessage());
                return null;
            }
        });
    }

    private void logToFile(String inputContent, String response) {
        try {
            // Create log directory if it does not exist
            Path logDirPath = Paths.get(LOG_DIR);
            if (!Files.exists(logDirPath)) {
                Files.createDirectories(logDirPath);
            }

            // Create or append to the log file
            String logFileName = "gpt_response.log";
            Path logFilePath = logDirPath.resolve(logFileName);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath.toString(), true))) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                String currentTime = LocalDateTime.now().format(formatter);
                writer.write("Timestamp: " + currentTime);
                writer.newLine();
                writer.write("Input Content: " + inputContent);
                writer.newLine();
                writer.write("Response: " + response);
                writer.newLine();
                writer.write("---------------------------------------------------------");
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getEstimatedAmtAsync(String city, String adminiDistrict, String primaryBusiness, String secondaryBusiness) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            DataSource dataSource = jdbcTemplate.getDataSource();

            try (Connection conn = dataSource.getConnection();
                 CallableStatement cstmt = conn.prepareCall("{call proc_estimated_amt(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {

                cstmt.setString(1, city);
                cstmt.setString(2, adminiDistrict);
                cstmt.setString(3, primaryBusiness);
                cstmt.setString(4, secondaryBusiness);
                cstmt.registerOutParameter(5, Types.NUMERIC);
                cstmt.registerOutParameter(6, Types.VARCHAR);
                cstmt.registerOutParameter(7, Types.NUMERIC);
                cstmt.registerOutParameter(8, Types.NUMERIC);
                cstmt.registerOutParameter(9, Types.NUMERIC);
                cstmt.registerOutParameter(10, Types.NUMERIC);
                cstmt.registerOutParameter(11, Types.VARCHAR);

                cstmt.execute();

                Long admiNum = cstmt.getLong(5);
                String tpbuzNum = cstmt.getString(6);
                Double cnt = cstmt.getDouble(7);
                Double totalPop = cstmt.getDouble(8);
                Double operPer = cstmt.getDouble(9);
                Double closPer = cstmt.getDouble(10);
                String indc = cstmt.getString(11);

                Map<String, Object> resData = new HashMap<>();
                resData.put("admiNum", admiNum);
                resData.put("tpbuzNum", tpbuzNum);
                resData.put("amt", 1); // Default value, replace if needed
                resData.put("cnt", cnt);
                resData.put("totalPop", totalPop);
                resData.put("operPer", operPer);
                resData.put("closPer", closPer);
                resData.put("indc", indc);

                result.add(resData);

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<Map<String, Object>> getPredictionAsync(Map<String, Object> input) {
        return CompletableFuture.supplyAsync(() -> {
            String pythonScriptPath = "ml/predict_amt.py";
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> result = new HashMap<>();

            try {
                // JSON 데이터를 생성
                String jsonData = objectMapper.writeValueAsString(input);

                // JSON 데이터를 적절히 이스케이프하여 파이썬 스크립트에 전달
                String[] command = new String[]{"python", pythonScriptPath, jsonData};
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.redirectErrorStream(true);
                Process process = processBuilder.start();

                // 표준 출력 읽기
                BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                StringBuilder output = new StringBuilder();
                String line;
                while ((line = stdInput.readLine()) != null) {
                    output.append(line);
                }

                // 에러 출력 읽기
                BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));
                StringBuilder errorOutput = new StringBuilder();
                while ((line = stdError.readLine()) != null) {
                    errorOutput.append(line);
                }

                int exitCode = process.waitFor();
//                System.out.println("Exited with code: " + exitCode);

                if (exitCode == 0) {
                    result = objectMapper.readValue(output.toString(), Map.class);
                } else {
                    result.put("status", "error");
                    result.put("message", "Python script execution failed with exit code " + exitCode);
                    result.put("details", errorOutput.toString()); // Add detailed error message
                }
            } catch (Exception e) {
                e.printStackTrace();
                result.put("status", "error");
                result.put("message", e.getMessage());
            }

            return result;
        });
    }

    @Async
    public CompletableFuture<List<Map<String, Object>>> getTimeSeriesPredictionAsync(String city, String code) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, Object>> result = new ArrayList<>();
            String pythonScriptPath = "ml/models/timeSeries.py";
            String cityName = city.split(" ")[0];

            String[] command = new String[]{"python", pythonScriptPath, cityName, code};
            String currentDir = System.getProperty("user.dir");
            try {
                // 명령어 출력

                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.redirectErrorStream(true);
                Process process = processBuilder.start();

                // 표준 출력 및 오류 출력 읽기
                BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));
                StringBuilder output = new StringBuilder();
                StringBuilder errorOutput = new StringBuilder();
                String line;

                while ((line = stdInput.readLine()) != null) {
                    output.append(line);
                }
                while ((line = stdError.readLine()) != null) {
                    errorOutput.append(line);
                }

                int exitCode = process.waitFor();
//                System.out.println("Python script output: " + output.toString());
//                System.out.println("Python script error output: " + errorOutput.toString());
//                System.out.println("Exited with code: " + exitCode);

                if (exitCode == 0) {
                    ObjectMapper mapper = new ObjectMapper();
                    result = mapper.readValue(output.toString(), new TypeReference<List<Map<String, Object>>>(){});
                } else {
                    throw new RuntimeException("Python script execution failed with exit code " + exitCode + " and error: " + errorOutput.toString());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return result;
        });
    }


    @Async
    public CompletableFuture<String> getCodeAsync(String mainNm, String sumNm) {
        return CompletableFuture.supplyAsync(() -> {
            DataSource dataSource = jdbcTemplate.getDataSource();
            String code = null;
            try (Connection conn = dataSource.getConnection();
                 CallableStatement callableStatement = conn.prepareCall("{ call proc_get_code(?, ?, ?) }")) {
                callableStatement.setString(1, mainNm);
                callableStatement.setString(2, sumNm);
                callableStatement.registerOutParameter(3, Types.VARCHAR);
                callableStatement.execute();
                code = callableStatement.getString(3);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return code;
        });
    }
}
