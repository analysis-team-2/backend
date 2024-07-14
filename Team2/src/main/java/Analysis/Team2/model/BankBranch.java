package Analysis.Team2.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "BANKBRANCH")
public class BankBranch {
    @Id
    private String branchId;
    private Double branchLatitude;
    private Double branchLongitude;
    private String branchName;
    private String phoneNumber;
    private String address;
    private String city;
    private String district;
    @Column(name = "SUB_DISTRICT")
    private String subDistrict;
    private String detailAddress;
    public String getBranchId() {
        return branchId;
    }

    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    public Double getBranchLatitude() {
        return branchLatitude;
    }

    public void setBranchLatitude(Double branchLatitude) {
        this.branchLatitude = branchLatitude;
    }

    public Double getBranchLongitude() {
        return branchLongitude;
    }

    public void setBranchLongitude(Double branchLongitude) {
        this.branchLongitude = branchLongitude;
    }

    public String getBranchName() {
        return branchName;
    }

    public void setBranchName(String branchName) {
        this.branchName = branchName;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getSubDistrict() {
        return subDistrict;
    }

    public void setSubDistrict(String subDistrict) {
        this.subDistrict = subDistrict;
    }

    public String getDetailAddress() {
        return detailAddress;
    }

    public void setDetailAddress(String detailAddress) {
        this.detailAddress = detailAddress;
    }
}
