package Analysis.Team2.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

@Service
public class AnalysisService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Long getMonthlySalesAmount(String city, String adminiDistrict, String primaryBusiness, String secondaryBusiness) {
        DataSource dataSource = jdbcTemplate.getDataSource();
        Long amount = null;

        try (Connection conn = dataSource.getConnection();
             CallableStatement callableStatement = conn.prepareCall("{ call proc_month_amt(?, ?, ?, ?, ?) }")) {

            // 입력 파라미터 설정
            callableStatement.setString(1, city);
            callableStatement.setString(2, adminiDistrict);
            callableStatement.setString(3, primaryBusiness);
            callableStatement.setString(4, secondaryBusiness);

            // 출력 파라미터 설정
            callableStatement.registerOutParameter(5, Types.NUMERIC);

            // 프로시저 실행
            callableStatement.execute();

            // 출력 파라미터에서 결과 값 읽기
            amount = callableStatement.getLong(5);

            // 디버그 또는 로깅
            System.out.println("매출액: " + amount);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return amount;
    }
}
