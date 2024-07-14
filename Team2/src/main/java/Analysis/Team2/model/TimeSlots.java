package Analysis.Team2.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

@Entity
@Table(name = "TIMESLOTS")
public class TimeSlots {
    @Id
    @Column(name = "SLOT_ID")
    private String slotId;
    @Column(name = "BRANCH_ID")
    private String branchId;
    @Column(name = "ENTRANCE_DATE")
    private String entranceDate;
    @Column(name = "ENTRANCE_TIME")
    private String entranceTime;
    @Column(name = "STATUS")
    private String status;
    @Column(name = "OCCUPIER")
    private String occupier;

    public String getOccupier() {
        return occupier;
    }

    public void setOccupier(String occupier) {
        this.occupier = occupier;
    }

    public String getSlotId() {
        return slotId;
    }

    public void setSlotId(String slotId) {
        this.slotId = slotId;
    }

    public String getBranchId() {
        return branchId;
    }

    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    public String getEntranceDate() {
        return entranceDate;
    }

    public void setEntranceDate(String entranceDate) {
        this.entranceDate = entranceDate;
    }

    public String getEntranceTime() {
        return entranceTime;
    }

    public void setEntranceTime(String entranceTime) {
        this.entranceTime = entranceTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
