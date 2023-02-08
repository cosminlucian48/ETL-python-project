import yaml
import jwt
import datetime
from fastapi import HTTPException
import lead.utils.constants as constants

def read_spark_credentials():
    try:
        with open(constants.CREDENTIALS_YAML_PATH, 'r') as f:
            creds = yaml.load(f, yaml.Loader)
            return creds['spark_rest_api']
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except KeyError:
        raise HTTPException(status_code=400, detail="Key not found in file")
    except ValueError:
        raise HTTPException(status_code=500, detail="Unexpected server error.")
    except AttributeError:
        raise HTTPException(status_code=500, detail="Unexpected server error.")
    except NameError:
        raise HTTPException(status_code=500, detail="Unexpected server error.")


def encode_jwt_token(req_body):
    spark_credentials = read_spark_credentials()

    if spark_credentials is None \
            or ('secret_token' not in spark_credentials.keys()
                or 'username' not in spark_credentials.keys()
                or 'password' not in spark_credentials.keys()):
        raise HTTPException(status_code=400, detail="Key not found in file")

    if type(req_body.username) != str or req_body.username == '' or req_body.username == None \
            or type(req_body.password) != str or req_body.password == '' or req_body.password == None:
        raise HTTPException(status_code=400, detail="Bad login parameters.")

    if (spark_credentials['username'] == req_body.username) and (spark_credentials['password'] == req_body.password):
        now = datetime.datetime.now()
        expiration = now + datetime.timedelta(hours=6)
        try:

            encoded_jwt = jwt.encode({
                'username': req_body.username,
                'password': req_body.password,
                'exp': expiration.timestamp()
            }, spark_credentials['secret_token'], algorithm="HS256")
            return {
                'token': encoded_jwt
            }
        except TypeError as e:
            raise HTTPException(status_code=400, detail=f"Failed to encode token: {e}")

    else:
        raise HTTPException(status_code=401, detail="Invalid credentials.")


def decode_jwt_token(token):
    try:
        secret_token = read_spark_credentials()['secret_token']
        decoded = jwt.decode(token, secret_token, algorithms=['HS256'])
        now = datetime.datetime.now().timestamp()
        if 'exp' not in decoded.keys() or decoded['exp'] < now:
            raise HTTPException(status_code=401, detail="Token has expired.")
        return decoded
    except jwt.exceptions.InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid token.")
    except jwt.exceptions.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired.")
    except KeyError:
        raise HTTPException(status_code=400, detail="Key not found in file")
