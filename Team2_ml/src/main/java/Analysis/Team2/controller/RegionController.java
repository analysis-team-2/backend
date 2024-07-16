package Analysis.Team2.controller;

import Analysis.Team2.model.CItyPosition;
import Analysis.Team2.repository.CityPositionRepository;
import Analysis.Team2.service.*;
import Analysis.Team2.utils.DongConverter;
import jdk.swing.interop.SwingInterOpUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@CrossOrigin(origins = {"http://changdoc.s3-website-ap-southeast-1.amazonaws.com", "http://localhost:3000"}, allowCredentials = "true")
@RequestMapping("/analysis")
public class RegionController {

    @Autowired
    private BankBranchService bankBranchService;
    @Autowired
    private RentalPropertyService rentalPropertyService;
    @Autowired
    private TimeSlotsService timeslotService;
    @Autowired
    private DongConverter dongConverter;
    @Autowired
    private AnalysisService analysisService;
    @Autowired
    private RegionService regionService;

    @PostMapping("/region/{cityname}")
    public ResponseEntity<String> requestRegion(@PathVariable String cityname, @RequestBody String requestBody) {
        JSONObject jsonRequest = new JSONObject(requestBody);
        String cityName = jsonRequest.getString("region_city");
        String dongName = jsonRequest.getString("region_dong");
        String category1 = jsonRequest.getString("category1");
        String category2 = jsonRequest.getString("category2");

//        dongName = "철산동";
        JSONObject responseJSON = new JSONObject();
        CompletableFuture<Void> bankBranchFuture = CompletableFuture.runAsync(() -> bankBranchService.getCityBankBranch(cityName, responseJSON));
        String finalDongName = dongName;
        CompletableFuture<Void> rentalPropertyFuture = CompletableFuture.runAsync(() -> {
            String legalDong = dongConverter.convertToLegalDong(finalDongName);
            rentalPropertyService.addRentalPropertiesToResponse(legalDong, responseJSON);
        });
        CompletableFuture<Void> timeslotFuture = CompletableFuture.runAsync(() -> timeslotService.addTimeslotsToResponse(responseJSON));
        CompletableFuture<Void> cityPositionsFuture = CompletableFuture.runAsync(() -> {
            List<CItyPosition> cityPositions = regionService.getCityPositions(cityName);
            JSONArray positionsArray = new JSONArray();
            List<CompletableFuture<Void>> salesFutures = new ArrayList<>();

            for (CItyPosition position : cityPositions) {
                CompletableFuture<Void> salesFuture = analysisService.getMonthlySalesAmountAsync(cityName, position.getDongName(), category1, category2)
                        .thenAccept(salesAmount -> {
                            JSONObject positionJSON = new JSONObject();
                            positionJSON.put("lat", position.getLat());
                            positionJSON.put("lng", position.getLng());
                            positionJSON.put("dongName", position.getDongName());
                            positionJSON.put("salesAmount", salesAmount);
                            positionsArray.put(positionJSON);
                        });
                salesFutures.add(salesFuture);
            }
            CompletableFuture.allOf(salesFutures.toArray(new CompletableFuture[0])).join();
            responseJSON.put("dongPositions", positionsArray);
        });

        CompletableFuture.allOf(bankBranchFuture, rentalPropertyFuture, timeslotFuture, cityPositionsFuture).join(); // 모든 비동기 작업 완료 대기

        if (responseJSON.has("branches") && responseJSON.getJSONArray("branches").length() > 0) {
            return ResponseEntity.ok(responseJSON.toString());
        } else {
            responseJSON.put("error", "No branches found for the given region.");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(responseJSON.toString());
        }
    }
}
