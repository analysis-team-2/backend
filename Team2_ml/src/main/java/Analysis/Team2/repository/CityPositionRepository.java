package Analysis.Team2.repository;

import Analysis.Team2.model.CItyPosition;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CityPositionRepository extends JpaRepository<CItyPosition, String> {
    List<CItyPosition> findByCityName(String cityName);

}
