package Analysis.Team2.controller;

import Analysis.Team2.service.UserService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/analysis")
public class UsersController {
    @Autowired
    private UserService userService;

    @CrossOrigin(origins = "http://localhost:3000", allowCredentials = "true")
    @PostMapping("/login")
    public ResponseEntity<String> loginRequest(@RequestBody String requestBody) {
        JSONObject jsonRequest = new JSONObject(requestBody);
        String loginId = jsonRequest.getString("login_id");
        String loginPw = jsonRequest.getString("login_pw");

        JSONObject responseJSON = new JSONObject();
        if (userService.validateUser(loginId, loginPw)) {
            responseJSON.put("status", "success");
            responseJSON.put("message", "Login successful.");
            responseJSON.put("username", userService.getUserNameById(loginId));
            return ResponseEntity.ok(responseJSON.toString());
        } else {
            responseJSON.put("status", "failure");
            responseJSON.put("message", "Invalid login ID or password.");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(responseJSON.toString());
        }
    }

}
