from openbb_core.app.service.user_service import UserService

def test_get_user_settings(logger):
    user_setting = UserService.read_from_file()
    credentials = user_setting.credentials
    api_key = credentials.akshare_api_key.get_secret_value()

