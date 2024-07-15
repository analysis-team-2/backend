package Analysis.Team2.controller;

import Analysis.Team2.service.AnalysisService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RestController
@CrossOrigin(origins = "http://localhost:3000", allowCredentials = "true")
@RequestMapping("/analysis")
public class MainController {

    @Autowired
    AnalysisService analysisService;
    @PostMapping("/main")
    public CompletableFuture<ResponseEntity<String>> mainRequest(@RequestBody String requestBody) {
        JSONObject jsonRequest = new JSONObject(requestBody);
        String city = jsonRequest.getString("region_city");
        String dong = jsonRequest.getString("region_dong");
        String category1 = jsonRequest.getString("category1");
        String category2 = jsonRequest.getString("category2");

        // 비동기 작업 시작
        CompletableFuture<List<Map<String, Object>>> daySalesFuture = analysisService.getDaySalesAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> genderAgeDataFuture = analysisService.getGenderAgeDistributionAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> hourlySalesFuture = analysisService.getHourlySalesAsync(city, dong, category1, category2);
        CompletableFuture<String> maxLiftConsequentFuture = analysisService.getMaxLiftConsequentAsync(category1, category2);
        CompletableFuture<Map<String, String>> indicatorFuture = analysisService.getIndicatorAsync(city, dong);

        // 모든 비동기 작업이 완료될 때까지 기다림
        return CompletableFuture.allOf(daySalesFuture, genderAgeDataFuture, hourlySalesFuture, indicatorFuture)
                .thenApply(v -> {
                    // 데이터 처리
                    List<Map<String, Object>> daySalesData = daySalesFuture.join();
                    List<Map<String, Object>> genderAgeData = genderAgeDataFuture.join();
                    List<Map<String, Object>> hourlySalesData = hourlySalesFuture.join();
                    String maxLiftConsequent = maxLiftConsequentFuture.join();
                    Map<String, String> indicator = indicatorFuture.join();


                    JSONArray daySalesArray = new JSONArray();
                    for (Map<String, Object> daySale : daySalesData) {
                        JSONObject daySaleJSON = new JSONObject();
                        daySaleJSON.put("day", daySale.get("day"));
                        daySaleJSON.put("totalAmt", daySale.get("totalAmt"));
                        daySalesArray.put(daySaleJSON);
                    }

                    JSONArray genderAgeArray = new JSONArray();
                    for (Map<String, Object> data : genderAgeData) {
                        JSONObject dataJSON = new JSONObject();
                        dataJSON.put("sex", data.get("sex"));
                        dataJSON.put("ageLabel", data.get("ageLabel"));
                        dataJSON.put("percentage", data.get("percentage"));
                        genderAgeArray.put(dataJSON);
                    }

                    JSONArray hourlySalesArray = new JSONArray();
                    for (Map<String, Object> hourSale : hourlySalesData) {
                        JSONObject hourSaleJSON = new JSONObject();
                        hourSaleJSON.put("hourLabel", hourSale.get("hourLabel"));
                        hourSaleJSON.put("amtPercentage", hourSale.get("amtPercentage"));
                        hourSaleJSON.put("cntPercentage", hourSale.get("cntPercentage"));
                        hourlySalesArray.put(hourSaleJSON);
                    }

                    JSONObject responseJSON = new JSONObject();
                    if (!genderAgeArray.isEmpty()) {
                        responseJSON.put("genderAgeDistribution", genderAgeArray);
                        responseJSON.put("daySales", daySalesArray);
                        responseJSON.put("hourlySales", hourlySalesArray);
                        responseJSON.put("associationCategory", maxLiftConsequent);
                        responseJSON.put("indicator", new JSONObject(indicator)); // indicator 정보 추가
                        responseJSON.put("status", "success");
                        responseJSON.put("message", "데이터 전달 성공");
                        return ResponseEntity.ok(responseJSON.toString());
                    } else {
                        responseJSON.put("error", "No data found for the given parameters.");
                        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(responseJSON.toString());
                    }
                });
    }


}