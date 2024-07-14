package Analysis.Team2.repository;

import Analysis.Team2.model.TimeSlots;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Date;
import java.util.List;
import java.util.Optional;


public interface TimeSlotsRepository extends JpaRepository<TimeSlots, String> {
    Optional<TimeSlots> findByBranchIdAndEntranceDateAndEntranceTime(String branchId, String entranceDate, String entranceTime);
    List<TimeSlots> findByBranchId(String branchId);

}
