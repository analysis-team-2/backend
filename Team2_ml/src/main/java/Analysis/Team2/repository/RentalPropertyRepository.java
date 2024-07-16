package Analysis.Team2.repository;

import Analysis.Team2.model.RentalProperty;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;


public interface RentalPropertyRepository extends JpaRepository<RentalProperty, String> {
    List<RentalProperty> findByDong(String dong);
}
