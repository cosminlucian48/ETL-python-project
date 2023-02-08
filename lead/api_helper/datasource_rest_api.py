import json
import requests
import yaml
import lead.utils.constants as constants


def login_rest_api():
    if not hasattr(constants, 'CREDENTIALS_YAML_PATH'):
        raise Exception('Could not retrieve API credentials.')

    with open(constants.CREDENTIALS_YAML_PATH, 'r') as file:
        credentials = yaml.load(file, yaml.Loader)
        credentials = credentials['rest_api']

    if credentials is None:
        raise Exception('Could not retrieve API credentials.')

    credentials_json = json.dumps(credentials, indent=4)

    try:
        res = requests.post(constants.DS_REST_API_TOKEN, data=credentials_json)

        # it will raise exceptions when the response status is !=200
        res.raise_for_status()

        # TODO should add JWT validation
        if 'token' in res.json().keys() and res.json()['token']:
            return res.json()['token']
    except requests.exceptions.RequestException as e:
        print('Requests error: ' + str(e))
    except Exception as e:
        print('Global: ' + str(e))
    raise Exception("Could not login.")


def get_api_data(auth_header):
    if not hasattr(constants, 'DS_REST_API_CONTENT'):
        raise Exception('Could not retrieve API credentials.')
    try:
        res = requests.post(constants.DS_REST_API_CONTENT, headers=auth_header)
        res.raise_for_status()  # it will raise exceptions when the response status is !=200
        if res.json():
            return res.json()
    except requests.exceptions.RequestException as e:
        print('Requests error: ' + str(e))
    except Exception as e:
        print('Global: ' + str(e))
