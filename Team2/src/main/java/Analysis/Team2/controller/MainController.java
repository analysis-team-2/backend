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
        CompletableFuture<Map<String, Integer>> merchantFuture = analysisService.getMerchantAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> merchantCntFuture = analysisService.getMerchantCntAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> yearAmtFuture = analysisService.getYearAmtAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> unitPriceCntFuture = analysisService.getUnitPriceCntAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> recentMonthlySalesFuture = analysisService.getRecentMonthlySalesAsync(city, dong, category1, category2);
        CompletableFuture<List<Map<String, Object>>> averageFlowpopFuture = analysisService.getAverageFlowpopAsync(city, dong);
        CompletableFuture<List<Map<String, Object>>> customerPercentageChangeFuture = analysisService.getCustomerPercentageChangeAsync(city, dong, category1, category2);
        CompletableFuture<Map<String, Object>> fullBusinessAnalysisFuture = analysisService.getFullBusinessAnalysisAsync(city, dong, category1, category2);

        // 모든 비동기 작업이 완료될 때까지 기다림
        return CompletableFuture.allOf(daySalesFuture, genderAgeDataFuture, hourlySalesFuture, maxLiftConsequentFuture, indicatorFuture, merchantFuture, merchantCntFuture, yearAmtFuture, unitPriceCntFuture, recentMonthlySalesFuture, averageFlowpopFuture, customerPercentageChangeFuture, fullBusinessAnalysisFuture)
                .thenApply(v -> {
                    // 데이터 처리
                    List<Map<String, Object>> daySalesData = daySalesFuture.join();
                    List<Map<String, Object>> genderAgeData = genderAgeDataFuture.join();
                    List<Map<String, Object>> hourlySalesData = hourlySalesFuture.join();
                    String maxLiftConsequent = maxLiftConsequentFuture.join();
                    Map<String, String> indicator = indicatorFuture.join();
                    Map<String, Integer> merchantData = merchantFuture.join();
                    List<Map<String, Object>> merchantCntData = merchantCntFuture.join();
                    List<Map<String, Object>> yearAmtData = yearAmtFuture.join();
                    List<Map<String, Object>> unitPriceCntData = unitPriceCntFuture.join();
                    List<Map<String, Object>> recentMonthlySalesData = recentMonthlySalesFuture.join();
                    List<Map<String, Object>> averageFlowpopData = averageFlowpopFuture.join();
                    List<Map<String, Object>> customerPercentageChangeData = customerPercentageChangeFuture.join();
                    Map<String, Object> fullBusinessAnalysisData = fullBusinessAnalysisFuture.join();


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
                    JSONArray merchantCntArray = new JSONArray();
                    for (Map<String, Object> merchantCnt : merchantCntData) {
                        JSONObject merchantCntJSON = new JSONObject();
                        merchantCntJSON.put("년월", merchantCnt.get("v_ta_ym"));
                        merchantCntJSON.put("상가수", merchantCnt.get("v_mer_cnt"));
                        merchantCntArray.put(merchantCntJSON);
                    }
                    JSONArray yearAmtArray = new JSONArray();
                    for (Map<String, Object> yearAmt : yearAmtData) {
                        JSONObject yearAmtJSON = new JSONObject();
                        yearAmtJSON.put("년월", yearAmt.get("v_ta_ymd"));
                        yearAmtJSON.put("월매출액합계", yearAmt.get("v_amt"));
                        yearAmtArray.put(yearAmtJSON);
                    }

                    JSONArray unitPriceCntArray = new JSONArray();
                    for (Map<String, Object> unitPriceCnt : unitPriceCntData) {
                        JSONObject unitPriceCntJSON = new JSONObject();
                        unitPriceCntJSON.put("년월", unitPriceCnt.get("v_ta_ymd"));
                        unitPriceCntJSON.put("매출건수", unitPriceCnt.get("v_cnt"));
                        unitPriceCntJSON.put("상가당매출액(평균)", unitPriceCnt.get("v_unit_price"));
                        unitPriceCntArray.put(unitPriceCntJSON);
                    }

                    JSONArray recentMonthlySalesArray = new JSONArray();
                    for (Map<String, Object> monthlySale : recentMonthlySalesData) {
                        JSONObject monthlySaleJSON = new JSONObject();
                        monthlySaleJSON.put("salesMonth", monthlySale.get("salesMonth"));
                        monthlySaleJSON.put("totalSales", monthlySale.get("totalSales"));
                        recentMonthlySalesArray.put(monthlySaleJSON);
                    }

                    JSONArray averageFlowpopArray = new JSONArray();
                    for (Map<String, Object> flowpop : averageFlowpopData) {
                        JSONObject flowpopJSON = new JSONObject();
                        flowpopJSON.put("ageGroup", flowpop.get("ageGroup"));
                        flowpopJSON.put("mCntAvg", flowpop.get("mCntAvg"));
                        flowpopJSON.put("fCntAvg", flowpop.get("fCntAvg"));
                        averageFlowpopArray.put(flowpopJSON);
                    }

                    JSONArray customerPercentageChangeArray = new JSONArray();
                    for (Map<String, Object> customerChange : customerPercentageChangeData) {
                        JSONObject customerChangeJSON = new JSONObject();
                        customerChangeJSON.put("sex", customerChange.get("sex"));
                        customerChangeJSON.put("age", customerChange.get("age"));
                        customerChangeJSON.put("totalCntPrevious", customerChange.get("totalCntPrevious"));
                        customerChangeJSON.put("totalCntCurrent", customerChange.get("totalCntCurrent"));
                        customerChangeJSON.put("percentageChange", customerChange.get("percentageChange"));
                        customerPercentageChangeArray.put(customerChangeJSON);
                    }



                    JSONObject responseJSON = new JSONObject();
                    if (!genderAgeArray.isEmpty()) {
                        responseJSON.put("genderAgeDistribution", genderAgeArray);
                        responseJSON.put("daySales", daySalesArray);
                        responseJSON.put("hourlySales", hourlySalesArray);
//                        responseJSON.put("associationCategory", maxLiftConsequent);
                        responseJSON.put("indicator", new JSONObject(indicator)); // indicator 정보 추가
                        responseJSON.put("merchantData", new JSONObject(merchantData));
                        responseJSON.put("merchantCntData", merchantCntArray);
                        responseJSON.put("yearAmtData", yearAmtArray);
                        responseJSON.put("unitPriceCntData", unitPriceCntArray);
                        responseJSON.put("recentMonthlySales", recentMonthlySalesArray);
                        responseJSON.put("averageFlowpop", averageFlowpopArray);
                        responseJSON.put("customerPercentageChange", customerPercentageChangeArray);
                        responseJSON.put("fullBusinessAnalysis", new JSONObject(fullBusinessAnalysisData));
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