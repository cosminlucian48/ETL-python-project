import yaml
import lead.utils.constants as constants


def get_postgres_creds():
    if not hasattr(constants, 'CREDENTIALS_YAML_PATH'):
        raise AttributeError('Could not retrieve credentials.')
    with open(constants.CREDENTIALS_YAML_PATH, 'r') as file:
        config = yaml.load(file, yaml.Loader)
        return config['postgres']


def get_mongo_creds():
    if not hasattr(constants, 'CREDENTIALS_YAML_PATH'):
        raise AttributeError('Could not retrieve credentials.')
    with open(constants.CREDENTIALS_YAML_PATH, 'r') as file:
        config = yaml.load(file, yaml.Loader)
        return config['mongo']


def get_mongo_string_creds():
    config = get_mongo_creds()
    return f"mongodb+srv://{config['user']}:{config['password']}@{config['endpoint']}/{config['database']}.{config['collection']}"
