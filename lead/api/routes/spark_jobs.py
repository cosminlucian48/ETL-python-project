import subprocess
from fastapi import Depends, HTTPException, APIRouter
from fastapi.security import OAuth2PasswordBearer
from lead.api_helper.authentification_helper import decode_jwt_token
from lead.api_helper.InsertMode import InsertMode

spark_route = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token/")


@spark_route.post("/spark/api-data-migration", status_code=200)
async def api_data_migration(token: str = Depends(oauth2_scheme), insert_mode: str = 'overwrite'):
    decoded = decode_jwt_token(token)
    if insert_mode != InsertMode.UPDATE.value and insert_mode != InsertMode.OVERWRITE.value:
        raise HTTPException(status_code=400,
                            detail="Bad value for insert_mode argument.")
    result = subprocess.run(["C:\\Spark\\bin\\spark-submit.cmd",
                             'spark_script_rest_api.py',
                             insert_mode],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                            )
    if result.returncode == 0:
        return {
            "status": "success",
            "output": result.stdout.decode(),
            'inset_mode': insert_mode
        }
    else:
        raise HTTPException(status_code=500, detail=f"Server Error - Spark job failed : {result.stdout.decode()}")


@spark_route.post("/spark/mongo-data-migration", status_code=200)
async def mongo_data_migration(token: str = Depends(oauth2_scheme), insert_mode: str = 'overwrite'):
    decoded = decode_jwt_token(token)
    if insert_mode != InsertMode.UPDATE.value and insert_mode != InsertMode.OVERWRITE.value:
        raise HTTPException(status_code=400,
                            detail="Bad value for insert_mode argument.")
    result = subprocess.run(["C:\\Spark\\bin\\spark-submit.cmd",
                             '--packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
                             'spark_script_mongo.py',
                             insert_mode],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                            )
    if result.returncode == 0:
        return {
            "status": "success",
            "output": result.stdout.decode(),
            'inset_mode': insert_mode
        }
    else:
        raise HTTPException(status_code=500, detail=f"Server Error - Spark job failed : {result.stdout.decode()}")


@spark_route.post("/spark/file-data-migration", status_code=200)
async def file_data_migration(token: str = Depends(oauth2_scheme), insert_mode: str = 'overwrite'):
    decoded = decode_jwt_token(token)

    if insert_mode != InsertMode.UPDATE.value and insert_mode != InsertMode.OVERWRITE.value:
        raise HTTPException(status_code=400,
                            detail="Bad value for insert_mode argument.")
    result = subprocess.run(["C:\\Spark\\bin\\spark-submit.cmd",
                             'spark_script_local_file.py',
                             insert_mode],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                            )

    if result.returncode == 0:
        return {
            "status": "success",
            "output": result.stdout.decode(),
            'inset_mode': insert_mode
        }
    else:
        raise HTTPException(status_code=500,
                            detail=f"Server Error - Spark job failed : {result.stdout.decode()}")
