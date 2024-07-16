package Analysis.Team2.repository;

import Analysis.Team2.model.BankBranch;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface BankBranchRepository extends JpaRepository<BankBranch, String> {
    List<BankBranch> findByCity(String city);
    List<BankBranch> findBySubDistrict(String subDistrict);

}
