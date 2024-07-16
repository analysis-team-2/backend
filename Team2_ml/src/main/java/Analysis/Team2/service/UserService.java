package Analysis.Team2.service;

import Analysis.Team2.model.Users;
import Analysis.Team2.repository.UsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UserService {
    @Autowired
    private UsersRepository usersRepository;
    public Users getUserById(String userId) {
        Optional<Users> user = usersRepository.findById(userId);
        return user.orElse(null);
    }

    public boolean validateUser(String userId, String password) {
        Users user = getUserById(userId);
        if (user != null && user.getUserPw().equals(password)) {
            return true;
        }
        return false;
    }

    public String getUserNameById(String userId) {
        Users user = getUserById(userId);
        return user.getUserName();
    }

    public boolean checkSignUp(String registerId) {
        // 회원가입 가능 여부를 체크하는 로직을 구현합니다.
        // 예를 들어, ID가 이미 존재하는지 체크할 수 있습니다.
        return !usersRepository.existsByUserId(registerId);
    }
    public Users saveUser(Users user) {
        return usersRepository.save(user);
    }
}
