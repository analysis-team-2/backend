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

    public String getUserNameById(String userId){
        Users user = getUserById(userId);
        return user.getUserName();
    }
}
