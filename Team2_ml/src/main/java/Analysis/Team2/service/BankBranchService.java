package Analysis.Team2.service;

import Analysis.Team2.model.BankBranch;
import Analysis.Team2.repository.BankBranchRepository;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class BankBranchService {

    @Autowired
    private BankBranchRepository bankBranchRepository;

    @Async
    public List<BankBranch> getBranchesByCity(String city) {
        return bankBranchRepository.findByCity(city);
    }

    public List<BankBranch> getBranchesBySubDistrict(String subDistrict) {
        return bankBranchRepository.findBySubDistrict(subDistrict);
    }

    public void getCityBankBranch(String regionName, JSONObject responseJSON) {
        System.out.println(regionName);
        List<BankBranch> branches = getBranchesBySubDistrict(regionName);
        if (branches != null) {
            for (BankBranch branch : branches) {
                JSONObject branchInfoJSON = new JSONObject();
                branchInfoJSON.put("branchId", branch.getBranchId());
                branchInfoJSON.put("branchName", branch.getBranchName());
                branchInfoJSON.put("phoneNumber", branch.getPhoneNumber());
                branchInfoJSON.put("branchAddress", branch.getAddress());
                JSONObject centerJson = new JSONObject();
                centerJson.put("lat", branch.getBranchLatitude());
                centerJson.put("lng", branch.getBranchLongitude());
                branchInfoJSON.append("center", centerJson);
                responseJSON.append("branches", branchInfoJSON);
            }
        }
    }
}
