package Analysis.Team2.service;

import org.hibernate.dialect.OracleTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class AnalysisService {

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


}
