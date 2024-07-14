package Analysis.Team2.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "RENTALPROPERTY")
public class RentalProperty {

    @Id
    @Column(name = "PROPERTY_ID")
    private String propertyId;

    @Column(name = "REGION_NAME")
    private String regionName;

    @Column(name = "DONG")
    private String dong;

    @Column(name = "TYPE")
    private String type;

    @Column(name = "TRADE_TYPE")
    private String tradeType;

    @Column(name = "BOJEUNG")
    private Integer bojeung;

    @Column(name = "RENT_PRICE")
    private Integer rentPrice;

    @Column(name = "CONTRACT_SPACE")
    private Integer contractSpace;

    @Column(name = "AVAIL_SPACE")
    private Double availSpace;

    @Column(name = "ADDRESS")
    private String address;

    // Getters and Setters
    public String getPropertyId() {
        return propertyId;
    }

    public void setPropertyId(String propertyId) {
        this.propertyId = propertyId;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getDong() {
        return dong;
    }

    public void setDong(String dong) {
        this.dong = dong;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTradeType() {
        return tradeType;
    }

    public void setTradeType(String tradeType) {
        this.tradeType = tradeType;
    }

    public Integer getBojeung() {
        return bojeung;
    }

    public void setBojeung(Integer bojeung) {
        this.bojeung = bojeung;
    }

    public Integer getRentPrice() {
        return rentPrice;
    }

    public void setRentPrice(Integer rentPrice) {
        this.rentPrice = rentPrice;
    }

    public Integer getContractSpace() {
        return contractSpace;
    }

    public void setContractSpace(Integer contractSpace) {
        this.contractSpace = contractSpace;
    }

    public Double getAvailSpace() {
        return availSpace;
    }

    public void setAvailSpace(Double availSpace) {
        this.availSpace = availSpace;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}