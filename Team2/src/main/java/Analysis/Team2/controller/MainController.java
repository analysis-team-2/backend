package Analysis.Team2.controller;

import Analysis.Team2.service.AnalysisService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
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
    public String mainRequest(@RequestBody String requestBody) throws ExecutionException, InterruptedException {
        JSONObject jsonRequest = new JSONObject(requestBody);
        String city = jsonRequest.getString("region_city");
        String dong = jsonRequest.getString("region_dong");
        String category1 = jsonRequest.getString("category1");
        String category2 = jsonRequest.getString("category2");
        JSONArray customerAgeArray = jsonRequest.getJSONArray("customerAge");


        CompletableFuture<List<Map<String, Object>>> daySalesFuture = analysisService.getDaySalesAsync(city, dong, category1, category2);
        List<Map<String, Object>> daySalesData = daySalesFuture.get();  // 비동기 결과 대기

        // 요일별 매출 데이터를 JSON 배열로 변환
        JSONArray daySalesArray = new JSONArray();
        for (Map<String, Object> daySale : daySalesData) {
            JSONObject daySaleJSON = new JSONObject();
            daySaleJSON.put("day", daySale.get("day"));
            daySaleJSON.put("totalAmt", daySale.get("totalAmt"));
            daySalesArray.put(daySaleJSON);
        }

        CompletableFuture<List<Map<String, Object>>> genderAgeDataFuture = analysisService.getGenderAgeDistributionAsync(city, dong, category1, category2);
        List<Map<String, Object>> genderAgeData = genderAgeDataFuture.get(); // Wait for async result

        JSONArray genderAgeArray = new JSONArray();
        for (Map<String, Object> data : genderAgeData) {
            JSONObject dataJSON = new JSONObject();
            dataJSON.put("sex", data.get("sex"));
            dataJSON.put("ageLabel", data.get("ageLabel"));
            dataJSON.put("percentage", data.get("percentage"));
            genderAgeArray.put(dataJSON);
        }


        // 최종 응답 객체 구성
        JSONObject responseJSON = new JSONObject();
        responseJSON.put("status", "success");
        responseJSON.put("message", "데이터 전달 성공");
        responseJSON.put("daySales", daySalesArray);  // 추가된 부분
        responseJSON.put("genderAgeDistribution", genderAgeArray);

        return responseJSON.toString();
    }

}