package Analysis.Team2.service;

import Analysis.Team2.model.CItyPosition;
import Analysis.Team2.repository.CityPositionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RegionService {
    @Autowired
    private CityPositionRepository cityPositionRepository;

    public List<CItyPosition> getCityPositions(String cityName) {
        return cityPositionRepository.findByCityName(cityName);
    }
}
