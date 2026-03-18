# development config
import os


local_mongodb_host = os.getenv("MONGODB_HOST", "127.0.0.1")
local_mongodb_port = int(os.getenv("MONGODB_PORT", "27037"))
local_mongodb_user = os.getenv("MONGODB_USER", "sca")
local_mongodb_password = os.getenv("MONGODB_PASSWORD", "sca123")
local_mongodb_db = os.getenv("MONGODB_DB", "sca")
local_mongodb_url = os.getenv(
    "MONGODB_URL",
    "mongodb://{}:{}@{}:{}/{}".format(
        local_mongodb_user,
        local_mongodb_password,
        local_mongodb_host,
        local_mongodb_port,
        local_mongodb_db,
    ),
)
