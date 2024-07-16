package Analysis.Team2.service;

import Analysis.Team2.model.TimeSlots;
import Analysis.Team2.repository.TimeSlotsRepository;
import jakarta.transaction.Transactional;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.util.List;
import java.util.Optional;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

@Service
public class TimeSlotsService {
    @Autowired
    private TimeSlotsRepository timeSlotsRepository;

    @Transactional
    @Async
    public CompletableFuture<Boolean> reserveTimeSlot(String branchId, String userId, String reserveTime) {
        try {
            String entranceDate = reserveTime.substring(0,10);
            String entranceTime = reserveTime.substring(11,16);

            Optional<TimeSlots> slotOpt = timeSlotsRepository.findByBranchIdAndEntranceDateAndEntranceTime(branchId, entranceDate, entranceTime);
            if (slotOpt.isPresent()) {
                TimeSlots slot = slotOpt.get();
                if ("예약가능".equals(slot.getStatus())) {
                    slot.setStatus("예약불가");
                    slot.setOccupier(userId);
                    timeSlotsRepository.save(slot);
                    return CompletableFuture.completedFuture(true);
                }
                else {
                    return CompletableFuture.completedFuture(false);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(false);
    }
    @Async
    public CompletableFuture<List<TimeSlots>> getTimeslotsByBranchId(String branchId) {
        return CompletableFuture.completedFuture(timeSlotsRepository.findByBranchId(branchId));
    }

    public void addTimeslotsToResponse(JSONObject responseJSON) {
        if (responseJSON.has("branches")) {
            for (int i = 0; i < responseJSON.getJSONArray("branches").length(); i++) {
                JSONObject branch = responseJSON.getJSONArray("branches").getJSONObject(i);
                String branchId = branch.getString("branchId");
                try {
                    CompletableFuture<List<TimeSlots>> futureTimeslots = getTimeslotsByBranchId(branchId);
                    List<TimeSlots> timeslots = futureTimeslots.get(); // 비동기 작업 완료 대기
                    for (TimeSlots timeslot : timeslots) {
                        JSONObject timeslotJSON = new JSONObject();
                        timeslotJSON.put("entranceDate", timeslot.getEntranceDate());
                        timeslotJSON.put("entranceTime", timeslot.getEntranceTime());
                        timeslotJSON.put("status", timeslot.getStatus());
                        branch.append("timeslots", timeslotJSON);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
