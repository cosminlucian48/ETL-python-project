from fastapi import APIRouter
from lead.api_helper.TokenModel import Token
from lead.api_helper.authentification_helper import encode_jwt_token

security_route = APIRouter()


@security_route.post('/token', status_code=200)
async def generate_token(req_body: Token):
    gen_token = encode_jwt_token(req_body)
    return gen_token
