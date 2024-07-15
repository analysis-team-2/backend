package Analysis.Team2.controller;

import Analysis.Team2.service.AnalysisService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "http://localhost:3000", allowCredentials = "true")
@RequestMapping("/analysis")
public class MainController {

    @Autowired
    AnalysisService analysisService;
    @PostMapping("/main")
    public String mainRequest(@RequestBody String requestBody) {
        JSONObject jsonRequest = new JSONObject(requestBody);
        String city = jsonRequest.getString("region_city");
        String dong = jsonRequest.getString("region_dong");
        String category1 = jsonRequest.getString("category1");
        String category2 = jsonRequest.getString("category2");
        JSONArray customerAgeArray = jsonRequest.getJSONArray("customerAge");
//        System.out.println(address);
//        System.out.println(category);



        System.out.println(analysisService.getDaySales(city, dong, category1, category2));



        JSONObject responseJSON = new JSONObject();
        responseJSON.put("status", "success");
        responseJSON.put("message", "데이터 전달 성공");
        responseJSON.put("data", new JSONObject()); // 예시 데이터

        return responseJSON.toString();
    }

}