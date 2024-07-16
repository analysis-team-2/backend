package Analysis.Team2.repository;

import Analysis.Team2.model.Users;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UsersRepository extends JpaRepository<Users, String> {
    boolean existsByUserId(String userId);

}
