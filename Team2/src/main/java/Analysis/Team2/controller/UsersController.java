package Analysis.Team2.controller;

import Analysis.Team2.model.Users;
import Analysis.Team2.service.UserService;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = {"http://changdoc.s3-website-ap-southeast-1.amazonaws.com", "http://localhost:3000"}, allowCredentials = "true")
@RequestMapping("/analysis")
public class UsersController {
    @Autowired
    private UserService userService;

    @PostMapping("/signup")
    public ResponseEntity<String> signup(@RequestBody String requestBody) {
        try {
            JSONObject jsonRequest = new JSONObject(requestBody);
//            JSONObject signupData = jsonRequest.getJSONObject("signupData");

            String registerId = jsonRequest.getString("signup_id");
            String registerPw = jsonRequest.getString("signup_pw");
            String registerName = jsonRequest.getString("signup_nm");

            JSONObject responseJSON = new JSONObject();
            if (userService.checkSignUp(registerId)) {
                Users user = new Users();
                user.setUserId(registerId);
                user.setUserPw(registerPw);
                user.setUserName(registerName);
                userService.saveUser(user);

                responseJSON.put("status", "signup success");
                return new ResponseEntity<>(responseJSON.toString(), HttpStatus.OK);
            } else {
                responseJSON.put("status", "signup failed");
                return new ResponseEntity<>(responseJSON.toString(), HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            JSONObject responseJSON = new JSONObject();
            responseJSON.put("status", "error");
            responseJSON.put("message", e.getMessage());
            return new ResponseEntity<>(responseJSON.toString(), HttpStatus.BAD_REQUEST);
        }
    }

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
