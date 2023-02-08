from fastapi import HTTPException, Depends, APIRouter
from fastapi.security import OAuth2PasswordBearer
from lead.api_helper.authentification_helper import decode_jwt_token
from lead.db_helper.postgres_operations import query_internet_price_speed_per_country, query_country_nr_internet_users, \
    query_region_internet_prices, query_subregion_internet_speed


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token/")

query_route = APIRouter()


@query_route.get("/data/internet-prices-speed-country", status_code=200)
async def internet_price_speed_per_country(token: str = Depends(oauth2_scheme),
                                           country: str = ''):
    decoded = decode_jwt_token(token)
    if decoded is None or decoded == '':
        raise HTTPException(status_code=401, detail="User not logged in.")
    prepared_data = query_internet_price_speed_per_country(country)
    return prepared_data


@query_route.get("/data/count-internet-users", status_code=200)
async def country_nr_internet_users(token: str = Depends(oauth2_scheme),
                                    query_type: str = 'lowest'):
    decoded = decode_jwt_token(token)
    if decoded is None or decoded == '':
        raise HTTPException(status_code=401, detail="User not logged in.")
    prepared_data = query_country_nr_internet_users(query_type)
    return prepared_data


@query_route.get("/data/region-internet-prices", status_code=200)
async def region_internet_prices(token: str = Depends(oauth2_scheme),
                                 query_type: str = 'lowest'):
    decoded = decode_jwt_token(token)
    if decoded is None or decoded == '':
        raise HTTPException(status_code=401, detail="User not logged in.")
    prepared_data = query_region_internet_prices(query_type)
    return prepared_data


@query_route.get("/data/subregion-internet-speed", status_code=200)
async def subregion_internet_speed(token: str = Depends(oauth2_scheme),
                                   query_type: str = 'lowest'):
    decoded = decode_jwt_token(token)
    if decoded is None or decoded == '':
        raise HTTPException(status_code=401, detail="User not logged in.")
    prepared_data = query_subregion_internet_speed(query_type)
    return prepared_data
