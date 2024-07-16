package Analysis.Team2.controller;

import Analysis.Team2.model.TimeSlots;
import Analysis.Team2.service.TimeSlotsService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;

@RestController
@CrossOrigin(origins = {"http://changdoc.s3-website-ap-southeast-1.amazonaws.com", "http://localhost:3000"}, allowCredentials = "true")
@RequestMapping("/analysis")
public class TimeSlotsController {

    @Autowired
    private TimeSlotsService timeslotService;

    @PostMapping("/reservation/{bank_id}")
    @Async
    public CompletableFuture<ResponseEntity<String>> reserveTimeSlot(@PathVariable("bank_id") String branchId, @RequestBody String requestBody) {
        JSONObject jsonRequest = new JSONObject(requestBody);
        String userId = jsonRequest.getString("user_id");
        String reserveTime = jsonRequest.getString("reserve_time");

        return timeslotService.reserveTimeSlot(branchId, userId, reserveTime)
                .thenApply(reserved -> {
                    JSONObject responseJSON = new JSONObject();
                    if (reserved) {
                        responseJSON.put("reserve_status", "Reservation success");
                        return ResponseEntity.ok(responseJSON.toString());
                    } else {
                        responseJSON.put("reserve_status", "Reservation fail");
                        return ResponseEntity.status(HttpStatus.CONFLICT).body(responseJSON.toString());
                    }
                });
    }

//    @GetMapping("/reservation/{bank_id}")
//    @Async
//    public CompletableFuture<ResponseEntity<String>> getTimeslotsByBranchId(@PathVariable("bank_id") String branchId) {
//        return timeslotService.getTimeslotsByBranchId(branchId)
//                .thenApply(timeslots -> {
//                    JSONObject responseJSON = new JSONObject();
//                    for (TimeSlots timeslot : timeslots) {
//                        JSONObject timeslotJSON = new JSONObject();
//                        timeslotJSON.put("entranceDate", timeslot.getEntranceDate());
//                        timeslotJSON.put("entranceTime", timeslot.getEntranceTime());
//                        timeslotJSON.put("status", timeslot.getStatus());
//                        responseJSON.append("timeslots", timeslotJSON);
//                    }
//                    return ResponseEntity.ok(responseJSON.toString());
//                });
//    }
}
