from pydantic import BaseModel, Field
from typing import Union


class Token(BaseModel):
    username: Union[str, None] = Field(
        default=None, title="The user name needs to be specified", min_length=1
    )
    password: Union[str, None] = Field(
        default=None, title="The password needs to be specified", min_length=1
    )
