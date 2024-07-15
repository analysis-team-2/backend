package Analysis.Team2.service;

import Analysis.Team2.model.RentalProperty;
import Analysis.Team2.repository.RentalPropertyRepository;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class RentalPropertyService {
    @Autowired
    private RentalPropertyRepository rentalPropertyRepository;

    @Async
    public CompletableFuture<List<RentalProperty>> getRentalPropertiesByDong(String dongName) {
        return CompletableFuture.completedFuture(rentalPropertyRepository.findByDong(dongName));
    }
    public void addRentalPropertiesToResponse(String dongName, JSONObject responseJSON) {
        try {
            CompletableFuture<List<RentalProperty>> futureProperties = getRentalPropertiesByDong(dongName);
            List<RentalProperty> properties = futureProperties.get(); // 비동기 작업 완료 대기
            if (properties != null && !properties.isEmpty()) {
                for (RentalProperty property : properties) {
                    JSONObject propertyJSON = new JSONObject();
                    propertyJSON.put("type", property.getType());
                    propertyJSON.put("tradeType", property.getTradeType());
                    propertyJSON.put("bojeung", property.getBojeung());
                    propertyJSON.put("rentPrice", property.getRentPrice());
                    propertyJSON.put("contractSpace", property.getContractSpace());
                    propertyJSON.put("availSpace", property.getAvailSpace());
                    propertyJSON.put("address", property.getAddress());
                    responseJSON.put("legalDongName", dongName);
                    responseJSON.append("properties", propertyJSON);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
