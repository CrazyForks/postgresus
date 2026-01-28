package users_repositories

var userRepository = &UserRepository{}
var usersSettingsRepository = &UsersSettingsRepository{}
var passwordResetRepository = &PasswordResetRepository{}

func GetUserRepository() *UserRepository {
	return userRepository
}

func GetUsersSettingsRepository() *UsersSettingsRepository {
	return usersSettingsRepository
}

func GetPasswordResetRepository() *PasswordResetRepository {
	return passwordResetRepository
}
